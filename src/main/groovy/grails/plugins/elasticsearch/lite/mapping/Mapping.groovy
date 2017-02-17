package grails.plugins.elasticsearch.lite.mapping

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE) //on class level
@interface Mapping {
    Class<ElasticSearchMarshaller<?>> value()
}
