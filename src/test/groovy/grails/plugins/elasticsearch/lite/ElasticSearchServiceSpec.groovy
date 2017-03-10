package grails.plugins.elasticsearch.lite

import grails.test.mixin.Mock
import grails.test.mixin.TestFor
import org.elasticsearch.client.Client
import spock.lang.Specification
import test.Person

/**
 * Created by marcoscarceles on 10/03/2017.
 */
@Mock([Person])
@TestFor(ElasticSearchService)
class ElasticSearchServiceSpec extends Specification {

    ElasticSearchLiteContextSpec elasticSearchLiteContext

    def setup() {
        elasticSearchLiteContext = new ElasticSearchLiteContextSpec()
        elasticSearchLiteContext.client = Mock(Client)
        service.elasticSearchLiteContext = elasticSearchLiteContext
    }

    def "indexes a domain object"() {
        given:
        elasticSearchLiteContext.init(Person)
    }
}
