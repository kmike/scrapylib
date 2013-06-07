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
    HS_FRONTIER  - Frontier name.
    HS_CONSUME_FROM_SLOT - Slot from where the spider will read new URLs.

Note that HS_FRONTIER and HS_SLOT can be overriden from inside a spider using
the spider attributes: "hs_frontier" and "hs_consume_from_slot" respectively.

The next optional settings can be defined:

    HS_ENDPOINT - URL to the API endpoint, i.e: http://localhost:8003.
                  The default value is provided by the python-hubstorage
                  package.
    HS_MAX_CONCURRENT_BATCHES - maximum number of concurrently processed
                                batches. The defaut is 10.

The next keys can be defined in a Request meta in order to control the behavior
of the HCF middleware:

    use_hcf - If set to True the request will be stored in the HCF.
    hcf_params - Dictionary of parameters to be stored in the HCF with the request
                 fingerprint

        qdata    data to be stored along with the fingerprint in the request queue
        fdata    data to be stored along with the fingerprint in the fingerprint set
        p    Priority - lower priority numbers are returned first. The default is 0

The value of 'qdata' parameter could be retrieved later using
``response.meta['hcf_params']['qdata']``.

The spider can override the default slot assignation function by setting the
spider slot_callback method to a function with the following signature::

   def slot_callback(request):
       ...
       return slot

"""
import logging
import collections
import itertools
from scrapy import signals, log
from scrapy.exceptions import NotConfigured, DontCloseSpider
from scrapy.http import Request
from hubstorage import HubstorageClient


# class HcfStoreStrategy(object):
#     def __init__(self, hubstorage_client):
#         self.client = hubstorage_client
#

class HcfMiddleware(object):

    PRIVATE_INFO_KEY = '__hcf_info__'

    def __init__(self, crawler):

        self.crawler = crawler
        self.hs_endpoint = crawler.settings.get("HS_ENDPOINT")
        self.hs_auth = self._get_config(crawler, "HS_AUTH")
        self.hs_projectid = self._get_config(crawler, "HS_PROJECTID")
        self.hs_frontier = self._get_config(crawler, "HS_FRONTIER")
        self.hs_consume_from_slot = self._get_config(crawler, "HS_CONSUME_FROM_SLOT")
        self.hs_max_concurrent_batches = int(crawler.settings.get('HS_MAX_CONCURRENT_BATCHES', 10))

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
        self.seen_batch_ids = set()

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

    def process_spider_output(self, response, result, spider):
        # XXX: or maybe use process_spider_input?
        # or process_spider_exception?
        if self.PRIVATE_INFO_KEY in response.meta:
            batch_id, fp = response.meta[self.PRIVATE_INFO_KEY]
            done, todo = self.batches[batch_id]
            done.add(fp)
            todo.remove(fp)

        for non_hcf_item in self._hcf_process_spider_result(result, spider):
            yield non_hcf_item

    def _hcf_process_spider_result(self, result, spider):
        """
        Put all applicable Requests from ``result`` iterator to a HCF queue,
        yield other objects.
        """
        num_enqueued = 0
        for item in result:
            if not self._is_hcf_request(item) or not self._valid_hcf_request(item):
                yield item
                continue
            self._enqueue_request(item, spider)
            num_enqueued += 1

        if num_enqueued:
            self._msg("%d requests are put to queue" % num_enqueued)

    def _is_hcf_request(self, item):
        """ Return if an item is a request intended to be stored in HCF queue """
        return isinstance(item, Request) and item.meta.get('use_hcf', False)

    def _valid_hcf_request(self, request):
        """
        Return True if a request could be enqueued;
        return False and log an error otherwise.
        """
        if request.method != 'GET':
            self._msg("'use_hcf' meta key is not supported "
                      "for non GET requests (%s)" % request.url, log.ERROR)
            return False

        # TODO: more validation rules,
        # e.g. for non-default callbacks and extra meta values.

        return True

    def _enqueue_request(self, request, spider):
        """ Put request to HCF queue. """

        slot_callback = getattr(spider, 'slot_callback', self._get_slot)
        slot = slot_callback(request)

        hcf_params = request.meta.get('hcf_params')
        fp = {'fp': request.url}
        if hcf_params:
            fp.update(hcf_params)

        # This is not necessarily a request to HCF because there is a batch
        # uploader in python-hubstorage, so it is fine to send a single item
        self.fclient.add(self.hs_frontier, slot, [fp])
        # self._msg("request enqueued to slot(%s)" % slot, log.DEBUG)

    def process_start_requests(self, start_requests, spider):

        self.hs_frontier = getattr(spider, 'hs_frontier', self.hs_frontier)
        self._msg('Using HS_FRONTIER=%s' % self.hs_frontier)

        self.hs_consume_from_slot = getattr(spider, 'hs_consume_from_slot', self.hs_consume_from_slot)
        self._msg('Using HS_CONSUME_FROM_SLOT=%s' % self.hs_consume_from_slot)

        self.has_new_requests = False
        for req in self._get_new_requests():
            self.has_new_requests = True
            yield req

        # if there are no links in the hcf, use the start_requests
        if not self.has_new_requests:
            self._msg('Using start_requests')
            for non_hcf_item in self._hcf_process_spider_result(start_requests, spider):
                yield non_hcf_item

    def close_spider(self, spider, reason):
        # Close the frontier client in order to make sure that
        # all the new links are stored.
        self._delete_processed_batches()
        self.fclient.close()
        self.hsclient.close()

    def idle_spider(self, spider):
        self.fclient.flush()
        self._delete_processed_batches()

        has_new_requests = False
        for request in self._get_new_requests():
            self.crawler.engine.schedule(request, spider)
            has_new_requests = True

        if has_new_requests:
            raise DontCloseSpider()

    def _get_new_requests(self):
        """ Get a new batch of links from the HCF."""
        num_links = 0
        num_batches = 0
        new_batches = self._get_new_batches(self.hs_max_concurrent_batches)

        for num_batches, batch in enumerate(new_batches, 1):
            self._msg("incoming batch: len=%d, id=%s" % (len(batch['requests']), batch['id']))

            assert batch['id'] not in self.batches
            self.batches[batch['id']] = set(), set(fp for fp, data in batch['requests'])

            for fingerprint, data in batch['requests']:
                # TODO: hook for custom Request instantiation =========
                meta = {
                    self.PRIVATE_INFO_KEY: (batch['id'], fingerprint),
                    'hcf_params': {'qdata': data},
                }
                yield Request(url=fingerprint, meta=meta)
                # ======================================================
                num_links += 1

        self._msg('Read %d new links from %d batches, slot(%s)' % (num_links, num_batches, self.hs_consume_from_slot))
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
            new_batches = self.fclient.read(self.hs_frontier, self.hs_consume_from_slot)
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
            self._msg('Read %d new batches from slot(%s)' % (num_read, self.hs_consume_from_slot))

    def _get_processed_batch_ids(self):
        return [batch_id for batch_id, (done, todo) in self.batches.iteritems() if not todo]

    def _get_batch_sizes(self):
        return [(len(done), len(todo)) for _, (done, todo) in self.batches.iteritems()]

    def _delete_processed_batches(self):
        """ Delete in the HCF the ids of the processed batches."""
        self._msg("Deleting batches: %r" % self._get_batch_sizes())

        ids = self._get_processed_batch_ids()
        self.fclient.delete(self.hs_frontier, self.hs_consume_from_slot, ids)
        for batch_id in ids:
            del self.batches[batch_id]

        self._msg('Deleted %d processed batches in slot(%s)' % (
                         len(ids), self.hs_consume_from_slot))
        self._msg('%d remaining batches with %d remaining requests (and %d processed requests)' % (
            len(self.batches),
            sum(len(todo) for _, (done, todo) in self.batches.iteritems()),
            sum(len(done) for _, (done, todo) in self.batches.iteritems()),
        ))

    def _get_slot(self, request):
        """ Determine to which slot should be saved the request."""
        return self.hs_consume_from_slot
