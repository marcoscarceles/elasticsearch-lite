package grails.plugins.elasticsearch.lite.mapping

import grails.plugins.elasticsearch.util.IndexNamingUtils

import javax.persistence.Index

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
