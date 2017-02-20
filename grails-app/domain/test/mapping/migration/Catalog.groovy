package test.mapping.migration

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import test.TestMarshallers

import javax.naming.OperationNotSupportedException

@Searchable(index = 'test.mapping.migration', type = 'catalog')
@Mapping(CatalogMarshaller)
class Catalog {

    String company
    String issue
    List pages

    static hasMany = [pages:Page]

    static constraints = {
    }
}

class CatalogMarshaller implements ElasticSearchMarshaller<Catalog> {

    static boolean USE_NESTED = false

    @Override
    JsonBuilder getMapping() {
        JsonBuilder catalog = new JsonBuilder()
        catalog {
            "properties" {
                "company"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "issue"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "pages" {
                    "type"(USE_NESTED ? 'nested' : 'object')
                }
            }
        }
        return catalog
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Catalog instance) {
        source.startObject()
                .field('company', instance.company)
                .field('issue', instance.issue)
        source.field('pages').startArray()
        instance.pages.each { Page page ->
            source.startObject()
                    .field('number', page.number)
                    .field('products').startArray()
            page.products.each { Item product ->
                TestMarshallers.getMarshaller(Item).toSource(product)
            }
            source.endArray().endObject()
            source.endObject()
        }
        source.endArray()

        source.endObject()
    }
}
