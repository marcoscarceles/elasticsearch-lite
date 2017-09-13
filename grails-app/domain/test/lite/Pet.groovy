package test.lite

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentFactory

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Searchable(index="test.lite", type="pet")
@Mapping(PetMarshaller)
class Pet {
    String name
    String photo

    static constraints = {
        name nullable:true
        photo nullable:true
    }
}

class PetMarshaller implements ElasticSearchMarshaller<Pet> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder pet = new JsonBuilder()
        pet {
            "properties" {
                "name"{
                    "type" "text"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "photo"{
                    "type" "text"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }
        return pet
    }

    @Override
    def toSource(Pet instance) {
        XContentFactory.jsonBuilder().startObject()
                .field('name', instance.name)
                .field('photo', instance.photo)
        .endObject()
    }
}
