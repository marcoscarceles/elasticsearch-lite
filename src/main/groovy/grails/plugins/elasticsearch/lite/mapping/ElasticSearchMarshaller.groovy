package grails.plugins.elasticsearch.lite.mapping

import grails.plugins.elasticsearch.lite.ElasticSearchType
import groovy.json.JsonBuilder
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.QueryBuilder

/**
 * Created by marcoscarceles on 08/02/2017.
 */
trait ElasticSearchMarshaller<T> {

    abstract JsonBuilder getMapping()

    abstract def toSource(T instance)

    IndexRequestBuilder prepareIndex(Client client, ElasticSearchType esType) {
        client.prepareIndex(esType.indexingIndex, esType.type)
    }

    IndexRequestBuilder buildIndex(Client client, ElasticSearchType esType, T instance) {
        prepareIndex(client, esType).setId(instance.id as String)
                .setSource(toSource(instance))
    }

    SearchRequestBuilder prepareSearch(Client client, ElasticSearchType esType) {
        client.prepareSearch(esType.queryingIndex).setTypes(esType.type)
    }

    SearchRequestBuilder buildSearch(Client client, ElasticSearchType esType, QueryBuilder query) {
        prepareSearch(client, esType).setQuery(query)
    }

    DeleteRequestBuilder prepareDelete(Client client, ElasticSearchType esType) {
        client.prepareDelete().setIndex(esType.indexingIndex).setType(esType.type)
    }

    DeleteRequestBuilder buildDelete(Client client, ElasticSearchType esType, T instance) {
        prepareDelete(client, esType).setId(instance.id as String)
    }

}
