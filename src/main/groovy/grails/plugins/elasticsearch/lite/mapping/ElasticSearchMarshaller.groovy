package grails.plugins.elasticsearch.lite.mapping

import groovy.json.JsonBuilder

/**
 * Created by marcoscarceles on 08/02/2017.
 */
trait ElasticSearchMarshaller<T> {

    abstract JsonBuilder getMapping()

    abstract def toSource(T instance)

}
