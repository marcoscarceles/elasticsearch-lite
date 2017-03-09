package grails.plugins.elasticsearch.lite.bulk

import groovy.util.logging.Slf4j
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse

/**
 * Created by marcoscarceles on 09/03/2017.
 */
@Slf4j
class DefaultBulkProcessorListener implements BulkProcessor.Listener {
    @Override
    void beforeBulk(long executionId, BulkRequest request) {
        if(log.isTraceEnabled()) {
            log.trace("Bulk Request #{} REQUESTING {} ...", executionId, request.subRequests()?.size() ?: 0)
        }
    }
    @Override
    void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if(log.isTraceEnabled()) {
            log.trace("Bulk Request #{} COMPLETED in {} milliseconds (failures = {})", executionId, response.tookInMillis, response.hasFailures())
        }
    }
    @Override
    void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        if(log.isTraceEnabled()) {
            log.trace("Bulk Request #${executionId} FAILED with exception", failure)
        }
    }
}
