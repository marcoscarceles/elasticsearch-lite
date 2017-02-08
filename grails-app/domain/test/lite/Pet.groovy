package test.lite

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Searchable(index="test.lite", type="pet")
@Mapping(PetMarshaller)
class Pet {
    String name
}

class PetMarshaller implements ElasticSearchMarshaller<Pet> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder pet = new JsonBuilder()
        pet {
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }
        return pet
    }

    @Override
    def toSource(Pet instance) {
        return null
    }
}
