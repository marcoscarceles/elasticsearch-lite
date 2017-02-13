package grails.plugins.elasticsearch

import grails.plugins.elasticsearch.lite.ElasticSearchAdminService
import grails.plugins.elasticsearch.lite.ElasticSearchService
import grails.test.mixin.integration.Integration
import grails.transaction.Rollback
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.QueryBuilders
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Specification
import test.lite.Pet

@Integration
@Rollback
class DynamicMethodsIntegrationSpec extends Specification {

    @Autowired
    ElasticSearchAdminService elasticSearchAdminService
    @Autowired
    ElasticSearchService elasticSearchService

    void setup() {
        new Pet(name: "Captain Kirk", photo: "http://www.nicenicejpg.com/100").save(failOnError: true)
        new Pet(name: "Captain Picard", photo: "http://www.nicenicejpg.com/200").save(failOnError: true)
        new Pet(name: "Captain Sisko", photo: "http://www.nicenicejpg.com/300").save(failOnError: true)
        new Pet(name: "Captain Janeway", photo: "http://www.nicenicejpg.com/400").save(failOnError: true)
        new Pet(name: "Captain Archer", photo: "http://www.nicenicejpg.com/500").save(failOnError: true)
    }

    void cleanup() {
        Pet.all.each { Pet captain ->
            captain.delete()
        }
    }

    void "can search using Dynamic Methods"() {
        expect:
        elasticSearchService.prepareSearch(Pet).setQuery(QueryBuilders.matchAllQuery()).setFetchSource(false)
                .get().hits.totalHits == 5

        when:
        SearchResponse response = Pet.search QueryBuilders.matchQuery('name', 'Captain')

        then:
        response.hits.total == 5
    }

    void "can search and filter using Dynamic Methods"() {
        expect:
        elasticSearchService.prepareSearch(Pet).setQuery(QueryBuilders.matchAllQuery()).setFetchSource(false)
                .get().hits.totalHits == 5

        when:
        SearchResponse response = Pet.search QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery('name', 'Captain'))
                .filter(QueryBuilders.termQuery('photo', "http://www.nicenicejpg.com/100"))

        then:
        response.hits.totalHits == 1
        response.hits.hits.first().fields['name'].value == "Captain Kirk"
    }
}
