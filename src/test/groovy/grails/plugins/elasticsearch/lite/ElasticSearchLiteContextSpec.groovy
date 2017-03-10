package grails.plugins.elasticsearch.lite

import grails.plugins.elasticsearch.lite.mapping.Searchable
import grails.test.mixin.Mock
import grails.util.Holders
import spock.lang.Specification
import spock.lang.Unroll
import test.Building
import test.Department
import test.GeoPoint
import test.Person
import test.Product
import test.Spaceship
import test.Store
import test.lite.Pet

import javax.annotation.PostConstruct
import java.lang.reflect.Method

/**
 * Created by marcoscarceles on 10/03/2017.
 */
@Mock([GeoPoint, Building, Person, Spaceship])
class ElasticSearchLiteContextSpec extends Specification {

    ElasticSearchLiteContext elasticSearchLiteContext

    def setup() {
        elasticSearchLiteContext = new ElasticSearchLiteContext()
        elasticSearchLiteContext.grailsApplication = Holders.grailsApplication
    }

    @Unroll
    def "Stores the ElasticSearch details for #domainClasses"() {
        expect:
        getSearchable(domainClasses) == searchableClasses

        when:
        elasticSearchLiteContext.init(domainClasses)

        then:
        elasticSearchLiteContext.elasticSearchTypes.keySet() == searchableClasses as Set
        elasticSearchLiteContext.elasticSearchTypes.values().count { it instanceof ElasticSearchType } == searchableClasses.size()
        domainClasses.collect { elasticSearchLiteContext.getMarshaller(it) }.findAll().size() == domainClasses.size()

        where:
        domainClasses                                                            || searchableClasses
        [Pet]                                                                    || [Pet]
        [GeoPoint, Building, Person, Spaceship]                                  || [Building, Spaceship]
        [Pet, Building, GeoPoint, Department, Person, Product, Spaceship, Store] || [Pet, Building, Department, Product, Spaceship, Store]
    }

    def "initializes based on the Grails domain classes available"() {
        expect:
        getSearchable([GeoPoint, Building, Person, Spaceship]).size() == 2

        when:
        Method init = ElasticSearchLiteContext.declaredMethods.find { it.isAnnotationPresent(PostConstruct) }
        init.invoke(elasticSearchLiteContext)

        then:
        elasticSearchLiteContext.elasticSearchTypes.size() == 2
    }

    List<Class> getSearchable(List<Class> classes) {
        classes.findAll { it.isAnnotationPresent(Searchable) }
    }
}
