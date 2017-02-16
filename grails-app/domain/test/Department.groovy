package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder

@Searchable(index = 'test', type = 'department')
@Mapping(DepartmentMarhsaller)
class Department {

    String name
    Long numberOfProducts
    Store store

    String getFullDepartmentName() {
        "${store}'s ${name} department"
    }

    static constraints = {
        name blank: false
        numberOfProducts nullable: true
    }

    static mapping = {
        autoImport(false)
    }
}

class DepartmentMarhsaller implements ElasticSearchMarshaller<Department> {

    @Override
    JsonBuilder getMapping() {
        JsonBuilder department = new JsonBuilder()

        department {
            "_parent" {
                "type" Store.getAnnotation(Searchable).type()
            }
            "properties" {
                "name"{
                    "type" "string"
                    "term_vector" "with_positions_offsets"
                    "include_in_all"true
                }
                "numberOfProducts"{
                    "type" "long"
                    "include_in_all"true
                }
            }
        }

        return department
    }

    @Override
    def toSource(Department instance) {
        return null
    }
}
