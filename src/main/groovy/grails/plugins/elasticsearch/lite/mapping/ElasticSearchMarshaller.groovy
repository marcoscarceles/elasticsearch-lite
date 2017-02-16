package grails.plugins.elasticsearch.lite.mapping

import groovy.json.JsonBuilder
import groovy.transform.CompileStatic

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@CompileStatic
trait ElasticSearchMarshaller<T> {

    abstract JsonBuilder getMapping()

    abstract def toSource(T instance)

}
