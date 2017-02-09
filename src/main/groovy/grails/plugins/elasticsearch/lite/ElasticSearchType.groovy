package grails.plugins.elasticsearch.lite

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.IndexNamingUtils

/**
 * Created by marcoscarceles on 08/02/2017.
 */
class ElasticSearchType {

    String index
    String type
    ElasticSearchMarshaller marshaller

    public static final READ_SUFFIX = "_read"
    public static final WRITE_SUFFIX = "_write"

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
