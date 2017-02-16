package grails.plugins.elasticsearch.lite.mapping

import groovy.transform.CompileStatic

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Created by marcoscarceles on 08/02/2017.
 */

@CompileStatic
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@interface Searchable {
    String index()
    String type()
}
