package test.custom.id

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

class Toy {
    UUID id
    String name
    String color

    static mapping = {
        id(generator: "uuid2", type: "uuid-char", length: 36)
    }

    static constraints = {
        name(nullable: true)
    }
}

class ToyMarshaller implements ElasticSearchMarshaller<Toy> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder toy = new JsonBuilder()
        toy {
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "color"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }

        toy
    }

    @Override
    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Toy instance) {
        source.startObject()
                .field('name', instance.name)
                .field('color', instance.color)
        source.endObject()
    }
}
