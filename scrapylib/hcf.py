"""
HCF Middleware

This SpiderMiddleware uses the HCF backend from hubstorage to retrieve the new
urls to crawl and store back the links extracted.

To activate this middleware it needs to be added to the SPIDER_MIDDLEWARES
list, i.e:

SPIDER_MIDDLEWARES = {
    'scrapylib.hcf.HcfMiddleware': 543,
}

And the next settings need to be defined:

    HS_AUTH     - API key
    HS_PROJECTID - Project ID in the panel.

The next optional settings can be defined:

    HS_ENDPOINT - URL to the API endpoint, i.e: http://localhost:8003.
                  The default value is provided by the python-hubstorage
                  package.
    HCF_MAX_CONCURRENT_BATCHES - maximum number of concurrently processed
                                batches. The defaut is 10.

By default, middleware will use spider name as HCF frontier and '0' as slot
both for getting new requests from HCF and putting requests to HCF.
Default values can be overriden from inside a spider using the
spider attributes: "hcf_frontier" and "hcf_slot" respectively. It is also
possible to override target frontier and slot using Request meta
('hcf_slot' and 'hcf_frontier' keys).

The next keys can be defined in a Request meta in order to control the behavior
of the HCF middleware:

    use_hcf - If set to True the request will be stored in the HCF.
    hcf_params - Dictionary of parameters to be stored in the HCF with the request
                 fingerprint

        qdata    data to be stored along with the fingerprint in the request queue
        fdata    data to be stored along with the fingerprint in the fingerprint set
        p    Priority - lower priority numbers are returned first. The default is 0

    hcf_slot - If present, this slot is used for storing request in the HCF.
    hcf_frontier - If present, this frontier is used for storing request
                   in the HCF.

The value of 'qdata' parameter could be retrieved later using
``response.meta['hcf_params']['qdata']``.

The spider can override how requests are serialized and deserialized
for HCF by providing ``hcf_make_request`` and/or ``hcf_serialize_request``
methods with the following signatures::

    def hcf_deserialize_request(self, hcf_params, batch_id):
        # ...
        # url = hcf_params['fp']
        # return Request(url)

    def hcf_serialize_request(self, request):
        # if not request.meta.get('use_hcf', False):
        #     return request
        # ...
        # return {'fp': request.url, 'qdata': {...} }

This may be useful if your fingerprints are not URLs or you want to
customize the process for other reasons (e.g. to make "use_hcf" flag unnecessary).
If your ``hcf_serialize_request`` decided not to serialize a request
(or can't serialize it) then return request unchanged - it will be scheduled
without HCF.

"""
import logging
import collections
import itertools
from scrapy import signals, log
from scrapy.exceptions import NotConfigured, DontCloseSpider
from scrapy.http import Request
from hubstorage import HubstorageClient


class HcfMiddleware(object):

    PRIVATE_INFO_KEY = '__hcf_info__'

    def __init__(self, crawler):

        self.crawler = crawler
        self.hs_endpoint = crawler.settings.get("HS_ENDPOINT")
        self.hs_auth = self._get_config(crawler, "HS_AUTH")
        self.hs_projectid = self._get_config(crawler, "HS_PROJECTID")
        self.hcf_max_concurrent_batches = int(crawler.settings.get('HCF_MAX_CONCURRENT_BATCHES', 10))

        self.hsclient = HubstorageClient(auth=self.hs_auth, endpoint=self.hs_endpoint)
        self.project = self.hsclient.get_project(self.hs_projectid)
        self.fclient = self.project.frontier

        # For better introspection keep track of both done and scheduled requests.
        self.batches = {}

        # It is not possible to limit a number of batches received from HCF;
        # to limit a number of concurrently processed batches there is a buffer.
        # Limiting a number of batches processed concurrently is important
        # because in case of error we reprocess batches that are partially
        # complete, and all concurrently processed batches could be partially
        # complete because we don't enforce sequential processing order.
        # For example, if we have 1000 batches and each of them
        # have 99 requests done, 1 unfinished then in case of error we'll
        # need to reprocess all 99000 finished requests.
        # Another way to solve this is to reschedule unfinished requests to
        # new batches and mark all existing as processed.
        self.batches_buffer = collections.deque()
        self.seen_batch_ids = set()  # XXX: are batch ids globally unique?

        crawler.signals.connect(self.close_spider, signals.spider_closed)
        crawler.signals.connect(self.idle_spider, signals.spider_idle)

        # Make sure the logger for hubstorage.batchuploader is configured
        logging.basicConfig()

    def _get_config(self, crawler, key):
        value = crawler.settings.get(key)
        if not value:
            raise NotConfigured('%s not found' % key)
        return value

    def _msg(self, msg, level=log.INFO):
        log.msg('(HCF) %s' % msg, level)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):
        # XXX: Running this middleware for several spiders concurrently
        # is not supported; multiple input slots/frontiers are also unsupported
        # (they complicate e.g. batch removing)
        self.consume_from_frontier = getattr(spider, 'hcf_frontier', spider.name)
        self.consume_from_slot = getattr(spider, 'hcf_slot', '0')
        self._msg('Input frontier: %s' % self.consume_from_frontier)
        self._msg('Input slot: %s' % self.consume_from_slot)

        self.has_new_requests = False
        for req in self._get_new_requests(spider):
            self.has_new_requests = True
            yield req

        # if there are no links in the hcf, use the start_requests
        if not self.has_new_requests:
            self._msg('Using start_requests')
            for non_hcf_item in self._hcf_process_spider_result(start_requests, spider):
                yield non_hcf_item

    def process_spider_output(self, response, result, spider):
        self._mark_as_done(response.meta)
        for non_hcf_item in self._hcf_process_spider_result(result, spider):
            yield non_hcf_item

    def _mark_as_done(self, meta):
        if self.PRIVATE_INFO_KEY in meta:
            batch_id, fp = meta[self.PRIVATE_INFO_KEY]
            done, todo = self.batches[batch_id]
            done.add(fp)
            todo.remove(fp)
            self._msg('%s is removed from batch(%s)' % (fp, batch_id))

    def _hcf_process_spider_result(self, result, spider):
        """
        Put all applicable Requests from ``result`` iterator to a HCF queue,
        yield other objects.
        """
        serialize = getattr(spider, 'hcf_serialize_request', self._serialize_request)
        num_enqueued = 0
        for request in result:
            if not isinstance(request, Request):  # item or None
                yield request
                continue

            data = serialize(request)
            if isinstance(data, Request):
                # this is a standard non-HCF request or serialization failed
                yield data
                continue

            frontier, slot = self._get_output_hcf_path(request)
            self.fclient.add(frontier, slot, [data])
            num_enqueued += 1

        if num_enqueued:
            self._msg("%d requests are put to queue" % num_enqueued)

    def _get_output_hcf_path(self, request):
        """ Determine to which frontier and slot should be saved the request. """
        frontier = request.meta.get('hcf_frontier', self.consume_from_frontier)
        slot = request.meta.get('hcf_slot', self.consume_from_slot)
        return frontier, slot

    def _serialize_request(self, request):
        if not request.meta.get('use_hcf', False):
            # standard request
            return request

        if request.method != 'GET':
            self._msg("'use_hcf' meta key is not supported "
                      "for non GET requests (%s)" % request.url, log.ERROR)
            return request
        # TODO: more validation rules?
        # e.g. for non-default callbacks and extra meta values
        # which are not supported by this default serialization function

        data = {'fp': request.url}
        data.update(request.meta.get('hcf_params', {}))
        return data

    def _deserialize_request(self, hcf_params, batch_id):
        return Request(
            url=hcf_params['fp'],
            meta={'hcf_params': hcf_params},
        )

    def close_spider(self, spider, reason):
        # When spider is closed, some of the scheduled requests
        # might be not processed yet; _delete_processed_batches
        # doesn't remove such requests.
        self._delete_processed_batches()

        # Close the frontier client in order to make sure that
        # all the new links are stored.
        self.fclient.close()
        self.hsclient.close()

    def idle_spider(self, spider):
        self.fclient.flush()

        # If spider entered idle state then all scheduled requests
        # were somehow processed, so we may remove all requests from
        # scheduled batches in HCF. It is hard to track all requests
        # otherwise because of download errors, DNS errors, redirects, etc.
        self._delete_started_batches()

        has_new_requests = False
        for request in self._get_new_requests(spider):
            self.crawler.engine.schedule(request, spider)
            has_new_requests = True

        if has_new_requests:
            raise DontCloseSpider()

    def _get_new_requests(self, spider):
        """ Get a new batch of links from the HCF."""
        num_links = 0
        num_batches = 0
        deserialize_request = getattr(spider, 'hcf_deserialize_request', self._deserialize_request)

        new_batches = self._get_new_batches(self.hcf_max_concurrent_batches)
        for num_batches, batch in enumerate(new_batches, 1):
            self._msg("incoming batch: len=%d, id=%s" % (len(batch['requests']), batch['id']))

            assert batch['id'] not in self.batches
            done, todo = set(), set(fp for fp, data in batch['requests'])
            self.batches[batch['id']] = done, todo

            for fp, qdata in batch['requests']:
                request = deserialize_request({'fp': fp, 'qdata': qdata}, batch['id'])
                request.meta.setdefault(self.PRIVATE_INFO_KEY, (batch['id'], fp))
                yield request
                num_links += 1

        self._msg('Read %d new links from %d batches, slot(%s)' % (num_links, num_batches, self.consume_from_slot))
        self._msg('Current batches: %s' % self._get_batch_sizes())

    def _get_new_batches(self, max_batches):
        """
        Return at most ``max_batches``, fetching them from HCF if necessary.
        """
        buffer_size = len(self.batches_buffer)
        self._msg("Buffered batches: %d" % buffer_size)

        if len(self.batches_buffer) >= max_batches:
            self._msg("Buffer has enough batches, no need to go to HCF")
            for i in range(max_batches):
                yield self.batches_buffer.popleft()
        else:
            # TODO: hook =====================
            # e.g. it should be possible to use roundrobin itertools recipe
            # for fetching batches from several slots
            new_batches = self.fclient.read(self.consume_from_frontier, self.consume_from_slot)
            # ================================

            # HCF could return already buffered batches; remove them
            new_batches_iter = (b for b in new_batches if b['id'] not in self.seen_batch_ids)

            # yield buffered batches first
            combined_batches = itertools.islice(
                itertools.chain(
                    (self.batches_buffer.popleft() for i in range(buffer_size)),
                    new_batches_iter
                ),
                max_batches
            )

            num_consumed = 0
            for num_consumed, batch in enumerate(combined_batches, 1):
                self.seen_batch_ids.add(batch['id'])
                yield batch

            # XXX: new_batches_iter must be generator for this to work properly
            self.batches_buffer.extend(new_batches_iter)

            self.seen_batch_ids.update(b['id'] for b in self.batches_buffer)
            num_read = len(self.batches_buffer) + num_consumed - buffer_size
            self._msg('Read %d new batches from slot(%s)' % (num_read, self.consume_from_slot))

    def _delete_started_batches(self):
        """ Delete all started batches from HCF """
        self._msg("Deleting started batches: %r" % self._get_batch_sizes())
        ids = self.batches.keys()
        self.fclient.delete(self.consume_from_frontier, self.consume_from_slot, ids)
        for batch_id in ids:
            del self.batches[batch_id]

    def _delete_processed_batches(self):
        """ Delete in the HCF the ids of the processed batches."""
        self._msg("Deleting processed batches: %r" % self._get_batch_sizes())
        ids = self._get_processed_batch_ids()
        self.fclient.delete(self.consume_from_frontier, self.consume_from_slot, ids)
        for batch_id in ids:
            del self.batches[batch_id]

        self._msg('Deleted %d processed batches in slot(%s)' % (
                         len(ids), self.consume_from_slot))
        self._msg('%d remaining batches with %d remaining requests (and %d processed requests)' % (
            len(self.batches),
            sum(len(todo) for _, (done, todo) in self.batches.iteritems()),
            sum(len(done) for _, (done, todo) in self.batches.iteritems()),
        ))
        # self._msg("remaining: %r" % [todo for _, (done, todo) in self.batches.iteritems()])

    def _get_processed_batch_ids(self):
        return [batch_id for batch_id, (done, todo) in self.batches.iteritems() if not todo]

    def _get_batch_sizes(self):
        return [(len(done), len(todo)) for _, (done, todo) in self.batches.iteritems()]
