package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

@Searchable(index = 'test', type = 'store')
@Mapping(StoreMarshaller)
class Store {

    String name
    String description = "A description of a store"
    String owner = "Owner of the store"

    static searchable = true

    static constraints = {
        name blank: false
        description nullable: true
        owner nullable: false
    }

    static mapping = {
        autoImport(false)
    }

    public String toString() {
        name
    }
}

class StoreMarshaller implements ElasticSearchMarshaller<Store> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder store = new JsonBuilder()
        store {
            "properties" {
                "name"{
                    "type" "text"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "description"{
                    "type" "text"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "owner"{
                    "type" "text"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }
        return store
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Store instance) {
        source.startObject()
                .field('name', instance.name)
                .field('description', instance.description)
                .field('owner', instance.owner)
        source.endObject()
    }
}
