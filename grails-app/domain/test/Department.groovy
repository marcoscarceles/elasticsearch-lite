package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

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


    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), Department instance) {
        source.startObject()
                .field('name', instance.name)
                .field('numberOfProducts', instance.numberOfProducts)
        .endObject()
    }
}
