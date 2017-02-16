package test

import com.fasterxml.jackson.databind.ObjectMapper
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.grails.web.json.JSONObject

@Searchable(index = "test", type = "product")
@Mapping(ProductMarshaller)
class Product {
    String name
    String description = "A description of a product"
    Float price = 1.00
    Date date
    JSONObject json

    static constraints = {
        name blank: false
        description nullable: true
        price nullable: true
        date nullable: true
        json nullable: true
    }

    static mapping = {
        autoImport(false)
        json type: JsonUserType
    }
}


class ProductMarshaller implements ElasticSearchMarshaller<Product> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder product = new JsonBuilder()
        product {
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "description"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "product" {
                    "type" "double"
                    "include_in_all" true
                }
                "date" {
                    "type" "date"
                    "include_in_all" true
                }
                "json" {
                    "type" "object"
                }
            }
        }
        return product
    }

    @Override
    def toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Product product) {
        source.startObject()
                .field('name', product.name)
                .field('description', product.description)
                .field('price', product.price)
                .field('date', product.date)
                .field('json', product.json as Map)
        source.endObject()
    }
}
