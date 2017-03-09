package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.gorm.DetachedCriteria
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import groovy.util.logging.Slf4j
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.grails.datastore.mapping.engine.event.AbstractPersistenceEvent
import org.grails.datastore.mapping.engine.event.PostDeleteEvent
import org.grails.datastore.mapping.engine.event.PostInsertEvent
import org.grails.datastore.mapping.engine.event.PostUpdateEvent
import org.grails.datastore.mapping.query.api.Criteria
import org.springframework.beans.factory.InitializingBean
import reactor.spring.context.annotation.Consumer

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
@Consumer
class ElasticSearchService implements ElasticSearchConfigAware, InitializingBean {

    GrailsApplication grailsApplication
    ElasticSearchLiteContext elasticSearchLiteContext

    @Override
    void afterPropertiesSet() throws Exception {
        if(esConfig.autoIndex == 'async') {
            on('gorm:postInsert') { PostInsertEvent event ->
                onUpsert(event)
            }
            on('gorm:postUpdate') { PostUpdateEvent event ->
                onUpsert(event)
            }
            on('gorm:postDelete') { PostDeleteEvent event ->
                onDelete(event)
            }
        }
    }

    Client getClient() {
        elasticSearchLiteContext.client
    }

    private BulkProcessor getBulkProcessor() {
        elasticSearchLiteContext.bulkProcessor
    }

    void onUpsert(AbstractPersistenceEvent event) {
        if(elasticSearchLiteContext.autoIndexingEnabled) {
            withReadAccess(event.entityObject.class) {
                index(event.entityObject)
            }
        }
    }

    void onDelete(AbstractPersistenceEvent event) {
        if(elasticSearchLiteContext.autoIndexingEnabled) {
            withReadAccess(event.entityObject.class) {
                unindex(event.entityObject)
            }
        }
    }

    IndexRequestBuilder buildIndex(Object domainObject) {
        IndexRequestBuilder request
        try {
            Class domainClass = domainObject.getClass()
            if(elasticSearchLiteContext.isSearchable(domainClass)) {
                log.debug("Building ElasticSearch index request for instance of ${domainObject.getClass()} with id ${domainObject.id}")
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                request = marshaller.buildIndex(client, esType, domainObject)
            } else {
                log.debug("Attempted to build index request for non @Searchable class ${domainClass}.")
            }
        } catch (Exception e) {
            log.error("Unable to build index request for instance of ${domainObject?.getClass()} due to Exception", e)
        }
        request
    }

    DeleteRequestBuilder buildDelete(Object domainObject) {
        DeleteRequestBuilder request
        try {
            Class domainClass = domainObject.getClass()
            if (elasticSearchLiteContext.isSearchable(domainClass)) {
                log.debug("Building ElasticSearch delete request for instance of ${domainObject.getClass()} with id ${domainObject.id}")
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                request = marshaller.buildDelete(client, esType, domainObject)
            } else {
                log.debug("Attempted to build delete request for non @Searchable class ${domainClass}.")
            }
        } catch (Exception e) {
            log.error("Unable to build delete request for instance of ${domainObject?.getClass()} due to Exception", e)
        }
        request
    }

    /**
     * Indexes a single domain object
     * @param backgroundTask - Whether to delegate on bulkProcessor to execute the index in the background on execute the request immediately
     * @param domainObject - The domain object to index
     * @return the IndexResponse if the object was @Searchabled and the request was sent synchronously or null otherwise
     */
    IndexResponse index(boolean backgroundTask = bulkProcessor != null, Object domainObject) {
        IndexResponse response
        try {
            IndexRequestBuilder request = buildIndex(domainObject)
            if(request) {
                    log.debug("Indexing instance of ${domainObject.getClass()} with id ${domainObject.id} into ElasticSearch")
                    if(backgroundTask && bulkProcessor) {
                        bulkProcessor.add(request.request())
                    } else {
                        response = request.get()
                    }
            }
        } catch (Exception e) {
            log.error("Unable to index instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
        }
        response
    }

    void indexAll(DetachedCriteria detachedCriteria = null, Class ... domainClasses) {
        indexAll(bulkProcessor != null, detachedCriteria, domainClasses as List)
    }

    void indexAll(boolean backgroundTask, DetachedCriteria detachedCriteria = null, Class ... domainClasses) {
        indexAll(backgroundTask, detachedCriteria, domainClasses as List)
    }

    void indexAll(DetachedCriteria detachedCriteria = null, List<Class> domainClasses) {
        indexAll(bulkProcessor != null, detachedCriteria, domainClasses)
    }

    void indexAll(boolean backgroundTask, DetachedCriteria detachedCriteria = null, List<Class> domainClasses) {
        domainClasses?.findAll { elasticSearchLiteContext.isSearchable(it) }.each { Class domainClass ->
            withReadAccess(domainClass) { // Do not cache domain objects in memory
                int total = domainClass.count()
                Criteria criteria = detachedCriteria ?: domainClass.createCriteria()

                log.debug("Begin bulk index of domain class ${domainClass} with ${total} instances")

                0.step(total, bulkBatchSize) { offset ->
                    log.info("Indexing ${offset} to ${offset+bulkBatchSize}")
                    List instances = criteria.list(offset:offset, max: bulkBatchSize) {
                        order("id", "desc")
                    }
                    index(instances)

                    if(!backgroundTask || !bulkProcessor) { //Otherwise let the BulkProcessor take care of the batching
                        sleep(bulkInterval.millis)
                    }
                }
                log.debug("Bulk index of domain class ${domainClass} with ${total} completed")
            }
        }
    }

    BulkResponse index(boolean backgroundTask = bulkProcessor != null, Object ... domainObjects) {
        index(backgroundTask, domainObjects as List)
    }

    BulkRequestBuilder buildBulkIndex(BulkRequestBuilder bulkRequest = client.prepareBulk(), Collection domainObjects) {

        domainObjects.each { domainObject ->
            try {
                IndexRequestBuilder request = buildIndex(domainObject)
                if(request) {
                    bulkRequest.add(request)
                }
            } catch (Exception e) {
                log.error("Unable to index instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
            }
        }

        bulkRequest
    }

    /**
     * Index a set of domain objects
     * @param backgroundTask - Whether to use bulkProcessor to perform the index in the background.
     * @param domainObjects
     * @return a BulkResponse if the request was executed synchronously (ie. no BulkProcessor is used) and at least one
     * instance was indexed, or null if BulkProcessor is used or none of the instances provided were @Searchable.
     */
    BulkResponse index(boolean backgroundTask = bulkProcessor != null, Collection domainObjects) {
        BulkResponse response
        if(backgroundTask && bulkProcessor) {
            domainObjects.each { domainObject ->
                try{
                    IndexRequest request = buildIndex(domainObject)?.request()
                    if(request) {
                        bulkProcessor.add(request)
                    }
                } catch (Exception e) {
                    log.error("Unable to index instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
                }
            }
        } else {
            BulkRequestBuilder request = buildBulkIndex(domainObjects)
            if(request?.numberOfActions()) {
                response = request.get()
            }
        }
        response
    }

    SearchResponse search(Class domainClass, QueryBuilder query) {
        SearchResponse response
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            try {
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                response = marshaller.buildSearch(client, esType, query).get()
            } catch (Exception e) {
                log.error("Unable to search instance of ${domainClass} with query ${query} due to Exception", e)
            }
        } else {
            log.warn("Attempted to serch an instance of ${domainClass}, which is not @Searchable")
        }
        response
    }

    /**
     * Unindexes a single domain object
     * @param backgroundTask - Whether to delegate on bulkProcessor to execute the index in the background on execute the request immediately
     * @param domainObject - The domain object to unindex
     * @return the DeleteResponse if the object was @Searchabled and the request was sent synchronously or null otherwise
     */
    DeleteResponse unindex(boolean backgroundTask = bulkProcessor != null, Object domainObject) {
        DeleteResponse response
        try {
            DeleteRequestBuilder request = buildDelete(domainObject)
            if(request) {
                log.debug("Unindexing instance of ${domainObject.getClass()} with id ${domainObject.id} into ElasticSearch")
                if(backgroundTask && bulkProcessor) {
                    bulkProcessor.add(request.request())
                } else {
                    response = request.get()
                }
            }
        } catch (Exception e) {
            log.error("Unable to unindex instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
        }
        response
    }

    void unindexAll(DetachedCriteria detachedCriteria = null, Class ... domainClasses) {
        unindexAll(bulkProcessor != null, detachedCriteria, domainClasses as List)
    }

    void unindexAll(boolean backgroundTask, DetachedCriteria detachedCriteria = null, Class ... domainClasses) {
        unindexAll(backgroundTask, detachedCriteria, domainClasses as List)
    }

    void unindexAll(DetachedCriteria detachedCriteria = null, List<Class> domainClasses) {
        unindexAll(bulkProcessor != null, detachedCriteria, domainClasses as List)
    }

    void unindexAll(boolean backgroundTask, DetachedCriteria detachedCriteria = null, List<Class> domainClasses) {
        domainClasses?.findAll { elasticSearchLiteContext.isSearchable(it) }.each { Class domainClass ->
            int total = domainClass.count()
            Criteria criteria = detachedCriteria ?: domainClass.createCriteria()

            log.debug("Begin bulk index of domain class ${domainClass} with ${total} instances")

            0.step(total, batchSize) { offset ->
                log.info("Indexing ${offset} to ${offset+bulkBatchSize}")
                List instances = criteria.list(offset:offset, max:bulkBatchSize) {
                    order("id", "desc")
                }
                unindex(instances)

                if(!backgroundTask || !bulkProcessor) { //Otherwise let the BulkProcessor take care of the batching
                    sleep(bulkInterval)
                }
            }
            log.debug("Bulk index of domain class ${domainClass} with ${total} completed")
        }
    }

    BulkResponse unindex(boolean backgroundTask = bulkProcessor != null, Object ... domainObjects) {
        unindex(backgroundTask, domainObjects as List)
    }

    BulkRequestBuilder buildBulkUnindex(bulkRequest = client.prepareBulk(), Collection domainObjects) {
        domainObjects.each { domainObject ->
            try {
                DeleteRequestBuilder request = buildDelete(domainObject)
                if(request) {
                    bulkRequest.add(request)
                }
            } catch(Exception e) {
                log.error("Unable to delete instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
            }
        }
        bulkRequest
    }

    /**
     * Unindex a set of domain objects
     * @param domainObjects
     * @return a BulkResponse if the request was executed synchronously (ie. no BulkProcessor is used) and at least one
     * instance was indexed, or null if BulkProcessor is used or none of the instances provided were @Searchable
     */
    BulkResponse unindex(boolean backgroundTask = bulkProcessor != null, Collection domainObjects) {
        BulkResponse response
        if(backgroundTask && bulkProcessor) {
            domainObjects.each { domainObject ->
                try {
                    DeleteRequest request = buildDelete(domainObject)?.request()
                    if(request) {
                        bulkProcessor.add(request)
                    }
                } catch(Exception e) {
                    log.error("Unable to delete instance of ${domainObject.getClass()} with id ${domainObject.id} due to Exception", e)
                }
            }
        } else {
            BulkRequestBuilder request = buildBulkUnindex(domainObjects)
            if(request?.numberOfActions()) {
                response = request.get()
            }
        }
        response
    }

    IndexRequestBuilder prepareIndex(Class domain) {
        ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domain)
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        marshaller.prepareIndex(client, esType)
    }

    SearchRequestBuilder prepareSearch(Class domain) {
        ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domain)
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        marshaller.prepareSearch(client, esType)
    }

    DeleteRequestBuilder prepareDelete(Class domain) {
        ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domain)
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        marshaller.prepareDelete(client, esType)
    }

    MoreLikeThisQueryBuilder.Item moreLike(Object domainObject) {
        MoreLikeThisQueryBuilder.Item item
        if(elasticSearchLiteContext.isSearchable(domainObject?.class)) {
            ElasticSearchType esType = elasticSearchLiteContext.getType(domainObject.getClass())
            item = new MoreLikeThisQueryBuilder.Item(esType.queryingIndex, esType.type, domainObject.id as String)
        }
        return item
    }

    private <T> T withReadAccess(Class domainClass, Closure<T> closure) {
        try {
            domainClass.withStatelessSession(closure)
        } catch(UnsupportedOperationException e) {
            domainClass.withSession(closure)
        }
    }

    /**
     * If bulkProcessor is being used, flush all requests
     * @return true if bulkProcessor flush was performed
     */
    boolean flush() {
        boolean didFlush = false
        if(bulkProcessor) {
            bulkProcessor.flush()
            didFlush = true
        }
        return didFlush
    }
}
