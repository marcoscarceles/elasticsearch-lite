/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package grails.plugins.elasticsearch.listener

import grails.plugins.elasticsearch.exception.IndexException
import grails.plugins.elasticsearch.lite.ElasticSearchLiteContext
import grails.plugins.elasticsearch.lite.ElasticSearchService
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Holds objects to be indexed.
 * <br/>
 * It looks like we need to keep object references in memory until they indexed properly.
 * If indexing fails, all failed objects are retried. Still no support for max number of retries (todo)
 * NOTE: if cluster state is RED, everything will probably fail and keep retrying forever.
 * NOTE: This is shared class, so need to be thread-safe.
 */
class IndexRequestQueue {

    private static final Logger LOG = LoggerFactory.getLogger(this)

    private ElasticSearchLiteContext elasticSearchLiteContext
    private ElasticSearchService elasticSearchService

    /**
     * A set containing the pending index requests.
     */
    private Set indexRequests = []

    /**
     * A set containing the pending delete requests.
     */
    private Set deleteRequests = []

    private ConcurrentLinkedDeque<OperationBatch> operationBatch = new ConcurrentLinkedDeque<OperationBatch>()

    void setElasticSearchLiteContext(ElasticSearchLiteContext elasticSearchLiteContext) {
        this.elasticSearchLiteContext = elasticSearchLiteContext
    }

    void setElasticSearchService(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService
    }

    void addIndexRequest(instance) {
        indexRequests << instance
    }

    void addDeleteRequest(instance) {
        deleteRequests << instance
    }

    /**
     * Execute pending requests and clear both index & delete pending queues.
     *
     * @return Returns an OperationBatch instance which is a listener to the last executed bulk operation. Returns NULL
     *         if there were no operations done on the method call.
     */
    void executeRequests() {
        Set toIndex = []
        Set toDelete = []

        cleanOperationBatchList()

        // Copy existing queue to ensure we are interfering with incoming requests.
        synchronized (this) {
            toIndex.addAll(indexRequests)
            toDelete.addAll(deleteRequests)
            indexRequests.clear()
            deleteRequests.clear()
        }

        // If there are domain instances that are both in the index requests & delete requests list,
        // they are directly deleted.
        toIndex.removeAll(toDelete)

        // If there is nothing in the queues, just stop here
        if (toIndex.isEmpty() && toDelete.empty) {
            return
        }

        BulkRequestBuilder bulkRequest = elasticSearchService.buildBulkIndex(toIndex)
        elasticSearchService.buildBulkUnindex(bulkRequest, toDelete)

        if (bulkRequest.numberOfActions() > 0) {
            OperationBatch completeListener = new OperationBatch(0, toIndex, toDelete)
            operationBatch.add(completeListener)
            try {
                bulkRequest.execute().addListener(completeListener)
            } catch (Exception e) {
                throw new IndexException("Failed to index/delete ${bulkRequest.numberOfActions()}", e)
            }
        }
    }

    void waitComplete() {
        LOG.debug('IndexRequestQueue.waitComplete() called')
        List<OperationBatch> clone = []
        synchronized (this) {
            clone.addAll(operationBatch)
            operationBatch.clear()
        }
        clone.each { it.waitComplete() }
    }

    private void cleanOperationBatchList() {
        synchronized (this) {
            for (Iterator<OperationBatch> it = operationBatch.iterator(); it.hasNext();) {
                OperationBatch current = it.next()
                if (current.complete) {
                    it.remove()
                }
            }
        }
        LOG.debug('OperationBatchList cleaned')
    }

    class OperationBatch implements ActionListener<BulkResponse> {

        private AtomicInteger attempts
        private Set toIndex
        private Set toDelete
        private CountDownLatch synchronizedCompletion = new CountDownLatch(1)

        OperationBatch(int attempts, Set toIndex, Set toDelete) {
            this.attempts = new AtomicInteger(attempts)
            this.toIndex = toIndex
            this.toDelete = toDelete
        }

        boolean isComplete() {
            synchronizedCompletion.count == 0
        }

        void waitComplete() {
            waitComplete(null)
        }

        /**
         * Wait for the operation to complete. Use this method to synchronize the application with the last ES operation.
         *
         * @param msTimeout A maximum timeout (in milliseconds) before the wait method returns, whether the operation has been completed or not.
         *                  Default value is 5000 milliseconds
         */
        void waitComplete(Integer msTimeout) {
            msTimeout = msTimeout == null ? 5000 : msTimeout

            try {
                if (!synchronizedCompletion.await(msTimeout, TimeUnit.MILLISECONDS)) {
                    LOG.warn("OperationBatchList.waitComplete() timed out after ${msTimeout.toString()} ms")
                }
            } catch (InterruptedException ie) {
                LOG.warn('OperationBatchList.waitComplete() interrupted')
            }
        }

        void fireComplete() {
            synchronizedCompletion.countDown()
        }

        void onResponse(BulkResponse bulkResponse) {
            bulkResponse.getItems().each { BulkItemResponse item ->
                boolean removeFromQueue = !item.failed || item.failureMessage.contains('UnavailableShardsException')
                // On shard failure, do not re-push.
                if (removeFromQueue) {
                    // remove successful OR fatal ones.
                    toIndex.removeAll {
                        item.id == it.id as String
                    }
                    toDelete.removeAll {
                        item.id == it.id as String
                    }
                }
                if (it.failed) {
                    LOG.error("Failed bulk item: $item.failureMessage")
                }
            }
            if (!toIndex.isEmpty() || !toDelete.isEmpty()) {
                push()
            } else {
                fireComplete()
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Batch complete: ${bulkResponse.getItems().length} actions.")
                }
            }
        }

        void onFailure(Throwable e) {
            // Everything failed. Re-push all.
            LOG.error('Bulk request failure', e)
            def remainingAttempts = attempts.getAndDecrement()
            if (remainingAttempts > 0) {
                LOG.info("Retrying failed bulk request ($remainingAttempts attempts remaining)")
                push()
            } else {
                LOG.info("Aborting bulk request - no attempts remain)")
            }
        }

        /**
         * Push specified entities to be retried.
         */
        void push() {
            LOG.debug("Pushing retry: ${toIndex.size()} indexing, ${toDelete.size()} deletes.")
            toIndex.each {
                synchronized (this) {
                    if (!indexRequests.contains(it)) {
                        // Do not overwrite existing stuff in the queue.
                        indexRequests << it
                    }
                }
            }
            toDelete.each {
                synchronized (this) {
                    if (!deleteRequests.contains(it)) {
                        deleteRequests << it
                    }
                }
            }
            executeRequests()
        }
    }
}
