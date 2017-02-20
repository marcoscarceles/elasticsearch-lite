package grails.plugins.elasticsearch.lite

import grails.plugins.elasticsearch.lite.ElasticSearchAdminService
import grails.plugins.elasticsearch.lite.ElasticSearchService
import grails.test.mixin.integration.Integration
import grails.transaction.Rollback
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.QueryBuilder
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

        int attempts = 5
        boolean keepWaiting = true
        while(keepWaiting) {
            keepWaiting = keepWaiting && elasticSearchService.prepareSearch(Pet).setQuery(QueryBuilders.matchAllQuery()).setFetchSource(false)
                    .get().hits.totalHits < 5
            keepWaiting = keepWaiting && --attempts
            if(keepWaiting) {
                Thread.sleep(500)
            }
        }
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
        response.hits.totalHits == 5
    }

    void "can search and filter using Dynamic Methods"() {
        expect:
        elasticSearchService.prepareSearch(Pet).setQuery(QueryBuilders.matchAllQuery()).setFetchSource(false)
                .get().hits.totalHits == 5

        when:
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery('name', 'Captain'))
                .filter(QueryBuilders.termQuery('photo', "100"))

        and:
        SearchResponse response = Pet.search query

        then:
        response.hits.totalHits == 1

        when:
        SearchRequestBuilder request = Pet.prepareSearch()
        response = request.setQuery(query).setFetchSource(true).get()

        then:
        response.hits.hits.first().sourceAsMap()['name'] == "Captain Kirk"
    }
}
