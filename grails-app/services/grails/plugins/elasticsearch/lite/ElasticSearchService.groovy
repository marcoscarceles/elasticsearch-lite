package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import groovy.util.logging.Slf4j
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.delete.DeleteResponse
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

    void onUpsert(AbstractPersistenceEvent event) {
        if(elasticSearchLiteContext.autoIndexingEnabled) {
            index(event.entityObject)
        }
    }

    void onDelete(AbstractPersistenceEvent event) {
        if(elasticSearchLiteContext.autoIndexingEnabled) {
            unindex(event.entityObject)
        }
    }

    IndexRequestBuilder buildIndex(Object domainObject) {
        Class domainClass = domainObject.class
        IndexRequestBuilder request
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Building ElasticSearch request for instance of ${domainObject.class} with id ${domainObject.id}")
            ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
            ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
            request = marshaller.buildIndex(client, esType, domainObject)
        } else {
            log.warn("Attempted to build index request for non @Searchable class ${domainClass}.")
        }
        request
    }

    IndexResponse index(Object domainObject) {
        IndexRequestBuilder request = buildIndex(domainObject)
        IndexResponse response
        if(request) {
            try {
                log.debug("Indexing instance of ${domainObject.class} with id ${domainObject.id} into ElasticSearch")
                response = request.get()
            } catch (Exception e) {
                log.error("Unable to index instance of ${domainObject.class} with id ${domainObject.id} due to Exception", e)
            }
        }
        response
    }

    void indexAll(Class ... domainClasses) {
        indexAll(domainClasses as List)
    }

    void indexAll(List<Class> domainClasses) {
        domainClasses?.findAll { elasticSearchLiteContext.isSearchable(it) }.each { Class domainClass ->
            withReadAccess(domainClass) { // Do not cache domain objects in memory
                int total = domainClass.count()
                int batchSize = esConfig.bulkBatchSize ?: 500
                int interval = esConfig.bulkInterval ?: 100
                Criteria criteria = domainClass.createCriteria()

                log.debug("Begin bulk index of domain class ${domainClass} with ${total} instances")

                0.step(total, batchSize) { offset ->
                    log.info("Indexing ${offset} to ${offset+batchSize}")
                    List instances = criteria.list(offset:offset, max:batchSize) {
                        order("id", "desc")
                    }
                    index(instances)
                    sleep(interval)
                }
                log.debug("Bulk index of domain class ${domainClass} with ${total} completed")
            }
        }
    }

    BulkResponse index(Object ... domainObjects) {
        index(domainObjects as List)
    }

    BulkRequestBuilder buildBulkIndex(BulkRequestBuilder bulkRequest = client.prepareBulk(), Collection domainObjects) {

        domainObjects.each { domainObject ->
            IndexRequestBuilder request = buildIndex(domainObject)
            if(request) {
                try {
                    bulkRequest.add(marshaller.buildIndex(client, esType, domainObject))
                } catch(Exception e) {
                    log.error("Unable to index instance of ${domainObject.class} with id ${domainObject.id} due to Exception", e)
                }
            }
        }
    }

    BulkResponse index(Collection domainObjects) {
        BulkResponse response
        BulkRequestBuilder request = buildBulkIndex(domainObjects)
        if(request?.numberOfActions()) {
            response = request.get()
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

    DeleteResponse unindex(Object domainObject) {
        Class domainClass = domainObject.class
        DeleteResponse response
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Deleting instance of ${domainClass} with id ${domainObject.id} from ElasticSearch")
            try {
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                response = marshaller.buildDelete(client, esType, domainObject).get()
            } catch (Exception e) {
                log.info("Unable to delete instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
            }
        }
        response
    }

    void unindexAll(Class ... domainClasses) {
        unindexAll(domainClasses as List)
    }

    void unindexAll(List<Class> domainClasses) {
        domainClasses?.findAll { elasticSearchLiteContext.isSearchable(it) }.each { Class domainClass ->
            int total = domainClass.count()
            int batchSize = esConfig.bulkBatchSize ?: 500
            int interval = esConfig.bulkInterval ?: 100
            Criteria criteria = domainClass.createCriteria()

            log.debug("Begin bulk index of domain class ${domainClass} with ${total} instances")

            0.step(total, batchSize) { offset ->
                log.info("Indexing ${offset} to ${offset+batchSize}")
                List instances = criteria.list(offset:offset, max:batchSize) {
                    order("id", "desc")
                }
                unindex(instances)
                sleep(interval)
            }
            log.debug("Bulk index of domain class ${domainClass} with ${total} completed")
        }
    }

    BulkResponse unindex(Object ... domainObjects) {
        unindex(domainObjects as List)
    }

    BulkResponse buildBulkUnindex(bulkRequest = client.prepareBulk(), Collection domainObjects) {

        domainObjects.each { domainObject ->
            Class domainClass = domainObject.class
            if(elasticSearchLiteContext.isSearchable(domainClass)) {
                log.debug("Deleting instance of ${domainClass} with id ${domainObject.id} from ElasticSearch")
                try {
                    ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)
                    ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                    bulkRequest.add(marshaller.buildDelete(client, esType, domainObject))
                } catch (Exception e) {
                    log.error("Unable to delete instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
                }
            }
        }
        bulkRequest
    }

    BulkResponse unindex(Collection domainObjects) {
        BulkResponse response
        BulkRequestBuilder request = buildBulkUnindex(domainObjects)
        if(request?.numberOfActions()) {
            response = request.get()
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
            ElasticSearchType esType = elasticSearchLiteContext.getType(domainObject.class)
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
}
