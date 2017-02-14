package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.grails.datastore.mapping.engine.event.AbstractPersistenceEvent
import org.grails.datastore.mapping.engine.event.PostInsertEvent
import org.grails.datastore.mapping.engine.event.PostUpdateEvent
import org.grails.io.support.PathMatchingResourcePatternResolver
import org.grails.io.support.Resource
import reactor.spring.context.annotation.Consumer
import reactor.spring.context.annotation.Selector

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Consumer
class ElasticSearchService implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchLiteContext elasticSearchLiteContext

    Client getClient() {
        elasticSearchLiteContext.client
    }

    @Selector('gorm:postInsert')
    void onInsert(PostInsertEvent event) {
        onUpsert(event);
    }

    @Selector('gorm:postUpdate')
    void onUpdate(PostUpdateEvent event) {
        onUpsert(event);
    }

    void onUpsert(AbstractPersistenceEvent event) {
        index(event.entityObject)
    }

    @Selector('gorm:postDelete')
    void onDelete(AbstractPersistenceEvent event) {
        unindex(event.entityObject)
    }

    IndexResponse index(Object domainObject) {
        Class domainClass = domainObject.class
        IndexResponse response
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Indexing instance of ${domainClass} with id ${domainObject.id} into ElasticSearch")
            try {
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)

                response = prepareIndex(domainClass).setId(domainObject.id as String)
                        .setSource(marshaller.toSource(domainObject)).get()
            } catch (Exception e) {
                log.error("Unable to index instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
            }
        }
        response
    }

    SearchResponse search(Class domainClass, QueryBuilder query) {
        SearchResponse response
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            try {
                response = prepareSearch(domain).setQuery(query)
            } catch (Exception e) {
                log.error("Unable to search instance of ${domainClass} with query ${query} due to Exception", e)
            }
        }
        response
    }

    DeleteResponse unindex(Object domainObject) {
        Class domainClass = domainObject.class
        DeleteResponse response
        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Deleting instance of ${domainClass} with id ${domainObject.id} from ElasticSearch")
            try {
                response = prepareDelete(domainClass).setId(domainObject.id as String).get()
            } catch (Exception e) {
                log.info("Unable to delete instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
            }
        }
        response
    }

    IndexRequestBuilder prepareIndex(Class domain) {
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        prepareIndex(esType)
    }

    IndexRequestBuilder prepareIndex(ElasticSearchType esType) {
        client.prepareIndex(esType.indexingIndex).setType(esType.indexingIndex)
    }

    SearchRequestBuilder prepareSearch(Class domain) {
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        prepareSearch(esType)
    }

    SearchRequestBuilder prepareSearch(ElasticSearchType esType) {
        client.prepareSearch(esType.queryingIndex).setTypes(esType.indexingIndex)
    }

    DeleteRequestBuilder prepareDelete(Class domain) {
        ElasticSearchType esType = elasticSearchLiteContext.getType(domain)
        prepareDelete(esType)
    }

    DeleteRequestBuilder prepareDelete(ElasticSearchType esType) {
        client.prepareDelete().setIndex(esType.indexingIndex).setType(esType.type)
    }

    MoreLikeThisQueryBuilder.Item moreLike(Object domain) {
        MoreLikeThisQueryBuilder.Item item
        if(elasticSearchLiteContext.isSearchable(domain?.class)) {
            ElasticSearchType esType = elasticSearchLiteContext.getType(domain.class)
            item = new MoreLikeThisQueryBuilder.Item(esType.queryingIndex, esType.type, domain.id as String)
        }
        return item
    }
}
