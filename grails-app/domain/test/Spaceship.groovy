package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

/**
 * Created by marcoscarceles on 17/02/2017.
 */
@Searchable(index = 'test', type = 'spaceship')
@Mapping(SpaceshipMarshaller)
class Spaceship {

    String name
    Person captain
    String shipData

    static searchable = {
        captain component: 'inner'
        shipData dynamic: true
    }

    static mapping = {
        shipData type: 'text', column: 'data'
    }

    static constraints = {
        shipData nullable: true
    }
}

class SpaceshipMarshaller implements ElasticSearchMarshaller<Spaceship> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder spaceship = new JsonBuilder()
        JsonBuilder captainMapping = TestMarshallers.getMarshaller(Person).mapping

        spaceship {
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "captain"(captainMapping.content)
                "shipData"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }
        return spaceship
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Spaceship instance) {
        source.startObject()
                .field('name', instance.name)
                .field('shipData',instance.shipData)
        if(instance.captain) {
            PersonMarshaller personMarshaller = TestMarshallers.getMarshaller(Person)
            personMarshaller.toSource(source.field('captain'), instance.captain)
        }
        source.endObject()
    }
}
