package test;

import grails.plugins.elasticsearch.lite.ElasticSearchLiteContext;
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller;
import grails.util.Holders;

/**
 * Created by marcoscarceles on 16/02/2017.
 */
public class TestMarshallers {

    private static ElasticSearchLiteContext ctx

    static ElasticSearchMarshaller getMarshaller(def object) {
        ElasticSearchMarshaller marshaller
        if(!ctx) {
            ctx = Holders.getGrailsApplication().getMainContext().getBean(ElasticSearchLiteContext)
        }
        if(object) {
            marshaller = ctx.getMarshaller(object instanceof Class ? object : object.class)
        }
        marshaller
    }
}
