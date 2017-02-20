package test.mapping.migration

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import test.TestMarshallers

@Searchable(index = 'test.mapping.migration', type = 'item')
@Mapping(ItemMarshaller)
class Item {

    String name
    Supplier supplier
    static constraints = {
        supplier nullable: true
    }
}

class ItemMarshaller implements ElasticSearchMarshaller<Item> {

    static boolean USE_NESTED = false

    @Override
    JsonBuilder getMapping() {
        JsonBuilder catalog = new JsonBuilder()
        catalog {
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "supplier" {
                    "type"(USE_NESTED ? 'nested' : 'object')
                }
            }
        }
        return catalog
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Item instance) {
        source.startObject()
                .field('name', instance.name)
        if(instance.supplier) {
            SupplierMarshaller marshaller = TestMarshallers.getMarshaller(Supplier)
            marshaller.toSource(source.field('supplier'), instance.supplier)
        }
        source.endObject()
    }
}
