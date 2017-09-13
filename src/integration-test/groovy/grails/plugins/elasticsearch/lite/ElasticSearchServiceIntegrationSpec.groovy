package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.test.mixin.integration.Integration
import grails.transaction.Rollback
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.grails.web.json.JSONObject
import org.joda.time.format.ISODateTimeFormat
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Specification
import test.*
import test.custom.id.Toy

import static org.elasticsearch.common.unit.DistanceUnit.KILOMETERS
import static org.elasticsearch.index.query.QueryBuilders.boolQuery
import static org.elasticsearch.index.query.QueryBuilders.existsQuery
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery
import static org.elasticsearch.index.query.QueryBuilders.idsQuery
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery
import static org.elasticsearch.index.query.QueryBuilders.matchQuery
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery
import static org.elasticsearch.search.aggregations.AggregationBuilders.max
import static org.elasticsearch.search.sort.SortBuilders.fieldSort

@Integration
@Rollback
class ElasticSearchServiceIntegrationSpec extends Specification {

    @Autowired
    ElasticSearchLiteContext elasticSearchLiteContext
    @Autowired
    ElasticSearchService elasticSearchService
    @Autowired
    ElasticSearchAdminService elasticSearchAdminService
    @Autowired
    GrailsApplication grailsApplication

    static Boolean isSetup = false

    /*
     * This test class doesn't delete any ElasticSearch indices, because that would also delete the mapping.
     * Be aware of this when indexing new objects.
     */

    void setup() {
        /*
        *  This is workaround due to issue with Grails3 and springbboot, otherwise we could have added in setupSpec
        * */
        if (!isSetup) {
            isSetup = true
            setupData()
        }
    }

    void setupData() {
        Product product01 = new Product(name: 'horst', price: 3.95)
        product01.save(failOnError: true)

        Product product02 = new Product(name: 'hobbit', price: 5.99)
        product02.save(failOnError: true)

        Product product03 = new Product(name: 'best', price: 10.99)
        product03.save(failOnError: true)

        Product product04 = new Product(name: 'high and supreme', price: 45.50)
        product04.save(failOnError: true)
    }

    void 'Index and un-index a domain object'() {
        when:
        def product = new Product(name: 'myTestProduct')
        product.save(failOnError: true)

        and:
        elasticSearchService.index(product) //Not necessary, but just in case GORM events hadn't triggered yet
        elasticSearchAdminService.refresh(Product)

        then:
        elasticSearchService.search(Product, queryStringQuery('myTestProduct')).hits.totalHits == 1

        when:
        elasticSearchService.unindex(product)
        elasticSearchAdminService.refresh(Product)

        then:
        elasticSearchService.search(Product, queryStringQuery('myTestProduct')).hits.totalHits == 0
    }

    void 'Indexing the same object multiple times updates the corresponding ES entry'() {
        given:
        def product = new Product(name: 'myTestProduct')
        product.save(failOnError: true)

        when:
        elasticSearchService.index(product)
        elasticSearchAdminService.refresh(Product)

        then:
        elasticSearchService.search(Product, queryStringQuery('myTestProduct')).hits.totalHits == 1

        when:
        product.name = 'newProductName'
        product.save(failOnError: true)
        elasticSearchService.index(product)
        elasticSearchAdminService.refresh(Product)

        then:
        elasticSearchService.search(Product, queryStringQuery('myTestProduct')).hits.totalHits == 0

        and:
        SearchResponse response = elasticSearchService.search(Product, queryStringQuery(product.name))
        response.hits.totalHits == 1
        response.hits.hits.first().sourceAsMap()['name'] == product.name
    }

    void 'a json object value should be marshalled and de-marshalled correctly'() {
        given:
        def product = new Product(name: 'product with json value')
        product.json = new JSONObject("""
{
    "test": {
        "details": "blah"
    }
}
"""
        )
        product.save(failOnError: true)
        elasticSearchService.index(product)
        elasticSearchAdminService.refresh(Product)

        when:
        SearchResponse response = elasticSearchService.search(Product, matchQuery('name', product.name))

        then:
        response.hits.totalHits == 1
        Map esProduct = response.hits.hits.first().sourceAsMap()
        esProduct['name'] == product.name
        esProduct['json'] == product.json as Map
    }

    void 'a date value should be marshalled and de-marshalled correctly'() {
        Date date = new Date()
        given:
        Product product = new Product(
                name: 'product with date value',
                date: date
        ).save(failOnError: true)
        elasticSearchService.index(product)
        elasticSearchAdminService.refresh(Product)

        when:
        SearchResponse response = elasticSearchService.search(Product, idsQuery().addIds(product.id as String))

        then:
        response.hits.totalHits == 1
        Map esProduct = response.hits.hits.first().sourceAsMap()
        esProduct.name == product.name
        Date esDate = ISODateTimeFormat.dateTimeParser().parseDateTime(esProduct.date).toDate()
        esDate == product.date
    }

    void 'a geo point location is marshalled and de-marshalled correctly'() {
        given:
        GeoPoint location = new GeoPoint(
                lat: 53.00,
                lon: 10.00
        ).save(failOnError: true)

        Building building = new Building(
                name: 'EvileagueHQ',
                location: location
        ).save(failOnError: true)

        elasticSearchService.index(building)
        elasticSearchAdminService.refresh(Building)

        when:
        SearchResponse response = elasticSearchService.search(Building,  matchQuery('name', 'EvileagueHQ'))

        then:
        response.hits.totalHits == 1
        Map esBuilding = response.hits.hits.first().sourceAsMap()
        def resultLocation = esBuilding.location
        resultLocation.lat == location.lat
        resultLocation.lon == location.lon
    }

    void 'search with geo distance filter'() {
        given: 'a building with a geo point location'
        GeoPoint geoPoint = new GeoPoint(
                lat: 50.1,
                lon: 13.3
        ).save(failOnError: true)

        def building = new Building(
                name: 'Test Product',
                location: geoPoint
        ).save(failOnError: true)

        elasticSearchService.index(building)
        elasticSearchAdminService.refresh(Building)

        when: 'a geo distance filter search is performed'

        QueryBuilder distanceQuery = geoDistanceQuery('location').distance(50, KILOMETERS).point(50, 13)
        SearchResponse response = elasticSearchService.search(Building, distanceQuery)

        then: 'the building should be found'
        response.hits.totalHits == 1
        SearchHit esBuilding = response.hits.hits.first()
        esBuilding.id == building.id as String
    }

    void 'searching with filtered query'() {
        given: 'some products'
        def wurmProduct = new Product(name: 'wurm', price: 2.00)
        wurmProduct.save(failOnError: true)

        def hansProduct = new Product(name: 'hans', price: 0.5)
        hansProduct.save(failOnError: true)

        def fooProduct = new Product(name: 'foo', price: 5.0)
        fooProduct.save(failOnError: true)

        elasticSearchService.index(wurmProduct, hansProduct, fooProduct)
        elasticSearchAdminService.refresh(Product)

        when: 'searching for a price'
        SearchResponse response = elasticSearchService.search(Product, boolQuery().filter(rangeQuery("price").gte(1.99).lte(2.3)))

        then: "the result should be product 'wurm'"
        response.hits.totalHits == 1
        Map esProduct = response.hits.hits.first().sourceAsMap()
        esProduct.name == wurmProduct.name
    }

    void 'searching for special characters in data pool'() {

        given: 'some products'
        def product01 = new Product(name: 'ästhätik', price: 3.95)
        product01.save(failOnError: true)

        elasticSearchService.index(product01)
        elasticSearchAdminService.refresh(Product)

        when: "search for 'a umlaut' "

        SearchResponse response = elasticSearchService.search(Product, matchQuery('name', 'ästhätik'))

        then: 'the result should contain 1 product'
        response.hits.totalHits == 1
        Map esProduct = response.hits.hits.first().sourceAsMap()
        esProduct.name == product01.name
    }

    void 'searching for features of the parent element from the actual element'() {

        given: 'parent and child elements'

        def parentParentElement = new Store(name: 'Eltern-Elternelement', owner: 'Horst')
        parentParentElement.save(failOnError: true)

        def parentElement = new Department(name: 'Elternelement', numberOfProducts: 4, store: parentParentElement)
        parentElement.save(failOnError: true)

        def childElement = new Product(name: 'Kindelement', price: 5.00)
        childElement.save(failOnError: true)

        elasticSearchService.index(parentParentElement, parentElement, childElement)
        elasticSearchAdminService.refresh(Store, Department, Product)

        when:
        SearchResponse response = elasticSearchService.search(Department, hasParentQuery('store', matchQuery('owner', 'Horst'), false))

        then:
        response.hits.totalHits
    }

    void 'Paging and sorting through search results'() {
        given: 'a bunch of products'
        def product
        10.times {
            product = new Product(name: "Produkt${it}", price: it).save(failOnError: true, flush: true)
            elasticSearchService.index(product)
        }
        elasticSearchAdminService.refresh(Product)

        when: 'a search is performed'
        SearchResponse response = elasticSearchService.prepareSearch(Product)
                .setFrom(3)
                .setSize(2)
                .addSort(fieldSort('name.keyword'))
                .setQuery(wildcardQuery('name', 'produkt*'))
        .get()

        then: 'the correct result-part is returned'
        response.hits.totalHits == 10
        response.hits.size() == 2
        List<Map> esProducts = response.hits.hits*.sourceAsMap()
        esProducts*.name == ['Produkt3', 'Produkt4']
    }

    void 'Multiple sorting through search results'() {
        given: 'a bunch of products'
        def product
        2.times { int i ->
            2.times { int k ->
                product = new Product(name: "Yogurt$i", price: k).save(failOnError: true, flush: true)
                elasticSearchService.index(product)
            }
        }
        elasticSearchAdminService.refresh(Product)

        when: 'a search is performed'
        def sort1 = fieldSort('name.keyword').order(SortOrder.ASC)
        def sort2 = fieldSort('price').order(SortOrder.DESC)
        SearchResponse response = elasticSearchService.prepareSearch(Product)
                .setQuery(wildcardQuery('name', 'yogurt*'))
                .addSort(sort1)
                .addSort(sort2)
        .get()

        then: 'the correct result-part is returned'
        response.hits.totalHits == 4
        response.hits.size() == 4
        List<Map> esProducts = response.hits.hits*.sourceAsMap()
        esProducts*.name == ['Yogurt0', 'Yogurt0', 'Yogurt1', 'Yogurt1']
        esProducts*.price == [1, 0, 1, 0]

        when: 'another search is performed'
        sort1 = new FieldSortBuilder('name.keyword').order(SortOrder.DESC)
        sort2 = new FieldSortBuilder('price').order(SortOrder.ASC)

        response = elasticSearchService.prepareSearch(Product)
                .setQuery(wildcardQuery('name', 'yogurt*'))
                .addSort(sort1)
                .addSort(sort2)
        .get()
        esProducts = response.hits.hits*.sourceAsMap()

        then: 'the correct result-part is returned'
        response.hits.totalHits == 4
        response.hits.size() == 4
        esProducts*.name == ['Yogurt1', 'Yogurt1', 'Yogurt0', 'Yogurt0']
        esProducts*.price == [0, 1, 0, 1]
    }

    void 'a geo distance search finds geo points at varying distances'() {
        setup:
        List<Building> buildings = []
        [
                [lat: 48.13, lon: 11.60, name: '81667'],
                [lat: 48.19, lon: 11.65, name: '85774'],
                [lat: 47.98, lon: 10.18, name: '87700']
        ].each {
            GeoPoint geoPoint = new GeoPoint(lat: it.lat, lon: it.lon).save(failOnError: true)
            buildings << new Building(name: "Postcode ${it.name}", location: geoPoint).save(failOnError: true)
        }
        elasticSearchService.index(buildings)
        elasticSearchAdminService.refresh(Building)

        when: 'a geo distance search is performed'
        SearchResponse response = elasticSearchService.search(Building, boolQuery()
                .must(matchQuery('name', 'Postcode'))
                .filter(geoDistanceQuery('location').distance(distance).point(48.141, 11.57))
        )

        then: 'all geo points in the search radius are found'
        response.hits.totalHits == postalCodesFound.size()
        List<Map> esBuildings = response.hits.hits*.sourceAsMap()
        esBuildings*.name as Set == postalCodesFound as Set

        cleanup:
        buildings.each {
            elasticSearchService.unindex(it)
            it.delete()
        }
        elasticSearchAdminService.refresh(Building)

        where:
        distance || postalCodesFound
        '1km'     | []
        '5km'     | ['Postcode 81667']
        '20km'    | ['Postcode 81667', 'Postcode 85774']
        '1000km'  | ['Postcode 81667', 'Postcode 85774', 'Postcode 87700']
    }

    void 'Index a domain object with UUID-based id'() {
        given:
        def car = new Toy(name: 'Car', color: "Red")
        car.save(failOnError: true)

        def plane = new Toy(name: 'Plane', color: "Yellow")
        plane.save(failOnError: true)
        elasticSearchService.index([car, plane])
        elasticSearchAdminService.refresh(Toy)

        when:
        SearchResponse response = elasticSearchService.search(Toy, queryStringQuery('Yellow'))

        then:
        response.hits.totalHits == 1
        response.hits.hits.first().id == plane.id as String
    }

    void 'bulk test'() {
        given:
        elasticSearchLiteContext.autoIndexingEnabled = false

        List<Spaceship> spaceships = (1..321).collect {
            def person = new Person(firstName: 'Person', lastName: 'McNumbery' + it).save(flush: true)
            def spaceship = new Spaceship(name: 'Ship-' + it, captain: person).save(flush: true)
            spaceship
        }

        expect:
        elasticSearchService.search(Spaceship, matchAllQuery()).hits.totalHits == 0

        when:
        BulkResponse bulkResponse = elasticSearchService.index(spaceships)

        then:
        !bulkResponse.hasFailures()

        when:
        elasticSearchAdminService.refresh(Spaceship)

        SearchResponse searchResponse = elasticSearchService.prepareSearch(Spaceship).setQuery(boolQuery()
                .filter(matchQuery('name', 'Ship'))
                .filter(existsQuery('captain'))
        ).setSize(0).get()

        then:
        searchResponse.hits.totalHits == 321

        cleanup:
        elasticSearchService.unindex(spaceships)
        spaceships.each {
            it.delete()
        }
        elasticSearchLiteContext.autoIndexingEnabled = true
    }

    void 'Use an aggregation'() {
        given:
        def jim = new Product(name: 'jim', price: 1.99).save(flush: true, failOnError: true)
        def xlJim = new Product(name: 'xl-jim', price: 5.99).save(flush: true, failOnError: true)
        elasticSearchService.index(jim, xlJim)
        elasticSearchAdminService.refresh(Product)

        when:
        SearchResponse response = elasticSearchService.prepareSearch(Product)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchQuery('name', 'jim'))
                .addAggregation(max('max_price').field('price'))
        .get()

        then:
        response.hits.totalHits == 2
        Max max =  response.aggregations.get('max_price')
        max.value == 5.99
    }
}
