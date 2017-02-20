package test.mapping.migration

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

/**
 * Created by @marcos-carceles on 08/01/15.
 */
class Supplier {

    String name

}

class SupplierMarshaller implements ElasticSearchMarshaller<Supplier> {

    @Override
    JsonBuilder getMapping() {
        throw new UnsupportedOperationException()
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Supplier instance) {
        source.startObject()
                .field('name', instance.name)
        source.endObject()
    }
}
