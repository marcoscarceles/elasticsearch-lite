package grails.plugins.elasticsearch.lite

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.IndexNamingUtils
import groovy.transform.CompileStatic

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@CompileStatic
class ElasticSearchType {

    String index
    String type
    Class parent
    ElasticSearchMarshaller marshaller

    String getQueryingIndex() {
        IndexNamingUtils.queryingIndexFor(index)
    }

    String getIndexingIndex() {
        IndexNamingUtils.indexingIndexFor(index)
    }

    List<String> knownIndices() {
        [index, queryingIndex, indexingIndex]
    }
}
