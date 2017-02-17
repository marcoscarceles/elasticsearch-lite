package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

/**
 * @author Noam Y. Tenne
 */
@Mapping(PersonMarshaller)
class Person {

    String firstName
    String lastName
    String password

    List<String> nickNames

    String getFullName() {
        return firstName + " " + lastName
    }

    static transients = ['fullName']
    static hasMany = [nickNames:String]
    
    static mapping = {
        autoImport(false)
    }

    static constraints = {
        password nullable: true
    }
}

class PersonMarshaller implements ElasticSearchMarshaller<Person> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder person = new JsonBuilder()

        person {
            "properties" {
                "firstName"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "lastName"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "fullName"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "password"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "nickNames"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
            }
        }
        return person
    }

    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Person instance) {
        source.startObject()
                .field('firstName', instance.firstName)
                .field('lastName', instance.lastName)
                .field('fullName', instance.fullName)
        if(instance.nickNames) {
            source.field('nickNames', instance.nickNames)
        }
        source.endObject()
    }
}
