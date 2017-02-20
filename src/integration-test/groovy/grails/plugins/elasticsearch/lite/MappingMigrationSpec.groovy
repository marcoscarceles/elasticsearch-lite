package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.exception.MappingException
import grails.test.mixin.integration.Integration
import grails.transaction.Rollback
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Ignore
import spock.lang.Specification
import test.mapping.migration.Catalog
import test.mapping.migration.CatalogMarshaller
import test.mapping.migration.Item
import test.mapping.migration.ItemMarshaller

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery

/**
 * Created by @marcos-carceles on 07/01/15.
 */
@Integration
@Rollback
class MappingMigrationSpec extends Specification {

    @Autowired GrailsApplication grailsApplication
    @Autowired ElasticSearchClientFactory elasticSearchClientFactory
    @Autowired ElasticSearchLiteContext elasticSearchLiteContext
    @Autowired ElasticSearchService elasticSearchService
    @Autowired ElasticSearchAdminService elasticSearchAdminService

    ElasticSearchAdminService getEs() {
        elasticSearchAdminService
    }

    void setup() {
        es.getIndices().each {
            es.deleteIndex(it)
        }
        // Recreate a clean environment as if the app had just booted
        grailsApplication.config.elasticSearch.migration = [strategy: "none"]
        grailsApplication.config.elasticSearch.bulkIndexOnStartup = false
        elasticSearchClientFactory.setup()
    }

    /*
     * STRATEGY : alias
     * case 1: Index does not exist
     * case 2: Alias exists
     * case 3: Index exists
     */

    void "With 'alias' strategy an index and an alias are created when none exist"() {
        given: "That an index does not exist"
        es.deleteIndex catalogType.index

        and: "Alias Configuration"
        grailsApplication.config.elasticSearch.migration = [strategy: "alias"]

        expect:
        !es.indexExists(catalogType.index)

        when:
        elasticSearchClientFactory.initializeIndices()

        then:
        es.indexExists(catalogType.index, 0)
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 0)
        es.mappingExists(catalogType.index, catalogType.type)
    }

    void "With 'alias' strategy if alias exist, the next one is created"() {
        given: "A range of previously created versions"
        es.deleteIndex catalogType.index
        (0..10).each {
            es.createIndex catalogType.index, it
        }
        es.pointAliasTo catalogType.index, catalogType.index, 10
        es.pointAliasTo catalogType.queryingIndex, catalogType.index
        es.pointAliasTo catalogType.indexingIndex, catalogType.index

        and: "Two different mapping conflicts on the same index"
        assert catalogType != itemType
        assert catalogType.index == itemType.index

        //Create conflicting Mapping
        CatalogMarshaller.USE_NESTED = true
        ItemMarshaller.USE_NESTED = true
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        //Restore initial state for next use
        CatalogMarshaller.USE_NESTED = false
        ItemMarshaller.USE_NESTED = false

        and: "Alias Configuration"
        grailsApplication.config.elasticSearch.migration = [strategy: "alias"]
        grailsApplication.config.elasticSearch.bulkIndexOnStartup = false //Content creation tested on a different test

        expect:
        es.indexExists catalogType.index, 10
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 10)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 10)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 10)

        when: 'Installing new mappings'
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        then: "A new version is created"
        es.indexExists catalogType.index, 11
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 11)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 11)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 11)

        and: "Only one version is created and not a version per conflict"
        catalogType.index == itemType.index
        catalogType.queryingIndex == itemType.queryingIndex
        catalogType.indexingIndex == itemType.indexingIndex
        !es.indexExists(itemType.index, 12)

        and: "Others mappings are created as well"
        es.mappingExists(itemType.index, itemType.type)
    }

    void "With 'alias' strategy if index exists, decide whether to replace with alias based on config"() {
        given: "Two different mapping conflicts on the same index"
        assert catalogType.type != itemType.type
        assert catalogType.index == itemType.index

        //Delete previous index
        es.deleteIndex(catalogType.index)

        //Create conflicting Mapping
        CatalogMarshaller.USE_NESTED = true
        CatalogMarshaller.USE_NESTED = true
        es.createIndex(catalogType.index)
        es.pointAliasTo(catalogType.queryingIndex, catalogType.index)
        es.pointAliasTo(catalogType.indexingIndex, catalogType.index)
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        //Restore initial state for next use
        CatalogMarshaller.USE_NESTED = false
        CatalogMarshaller.USE_NESTED = false

        and: "Existing content"
        List toIndex = []
        toIndex << new Catalog(company:"ACME", issue: 1).save(flush:true,failOnError: true)
        toIndex << new Catalog(company:"ACME", issue: 2).save(flush:true,failOnError: true)
        toIndex << new Item(name:"Road Runner Ultrafast Glue").save(flush:true, failOnError: true)
        elasticSearchService.index(toIndex)
        elasticSearchAdminService.refresh()

        expect:
        es.indexExists catalogType.index
        !es.aliasExists(catalogType.index)
        Catalog.count() == 2
        Catalog.search(queryStringQuery('ACME')).hits.totalHits == 2
        Item.count() == 1
        Item.search(queryStringQuery("Glue")).hits.totalHits == 1

        when:
        grailsApplication.config.elasticSearch.migration = [strategy: "alias", "aliasReplacesIndex" : false]
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        then: "an exception is thrown, due to the existing index"
        thrown MappingException

        and: "no content or mappings are affected"
        es.indexExists(catalogType.index)
        !es.aliasExists(catalogType.index)
        Catalog.count() == 2
        Catalog.search(queryStringQuery("ACME")).hits.totalHits == 2
        Item.count() == 1
        Item.search(queryStringQuery("Glue")).hits.totalHits == 1
        es.mappingExists(catalogType.index, catalogType.type)
        es.mappingExists(itemType.index, catalogType.type)

        when:
        grailsApplication.config.elasticSearch.migration = [strategy: "alias", "aliasReplacesIndex" : true]
        grailsApplication.config.elasticSearch.bulkIndexOnStartup = false //On the other cases content is recreated
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        then: "Alias replaces the index"
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 0)
        es.mappingExists(catalogType.index, catalogType.type)

        and: "Content is lost, as the index is regenerated"
        Catalog.count() == 2
        Catalog.search(queryStringQuery("ACME")).hits.totalHits == 0
        Item.count() == 1
        Item.search(queryStringQuery("Glue")).hits.totalHits == 0

        and: "All mappings are recreated"
        es.mappingExists(catalogType.index, catalogType.type)
        es.mappingExists(itemType.index, catalogType.type)

        cleanup:
        Catalog.findAll().each {
            it.unindex()
            it.delete()
        }
        Item.findAll().each {
            it.unindex()
            it.delete()
        }
    }

    /*
     * Tests for bulkIndexOnStartup = "deleted"
     * Zero Downtime for Alias to Alias
     * Minimise Downtime for Index to Alias
     */
    @Ignore("WIP : Zero Downtime implementation pending")
    void "Alias -> Alias : If configuration says to recreate the content, there is zero downtime"() {

        given: "An existing Alias"
        es.deleteIndex catalogType.index
        es.createIndex catalogType.index, 0
        es.pointAliasTo catalogType.index, catalogType.index, 0
        es.pointAliasTo catalogType.queryingIndex, catalogType.index
        es.pointAliasTo catalogType.indexingIndex, catalogType.index

        and: "A mapping conflict"
        CatalogMarshaller.USE_NESTED = true
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        //Restore initial state for next use
        CatalogMarshaller.USE_NESTED = false

        and: "Existing content"
        List toIndex = []
        toIndex << new Catalog(company:"ACME", issue: 1).save(flush:true,failOnError: true)
        toIndex << new Catalog(company:"ACME", issue: 2).save(flush:true,failOnError: true)
        toIndex << new Item(name:"Road Runner Ultrafast Glue").save(flush:true, failOnError: true)
        elasticSearchService.index(toIndex)
        elasticSearchAdminService.refresh()

        expect:
        es.indexExists(catalogType.index, 0)
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 0)

        and:
        Catalog.count() == 2
        Catalog.search(queryStringQuery("ACME")).hits.totalHits == 2
        Item.count() == 1
        Item.search(simpleQueryStringQuery("Glue")).hits.totalHits == 1

        when: "The mapping is installed and migrations happens"
        grailsApplication.config.elasticSearch.migration = [strategy: "alias"]
        grailsApplication.config.elasticSearch.bulkIndexOnStartup = "deleted"
        elasticSearchClientFactory.initializeIndices(Catalog, Item)

        then: "Temporarily, while indexing occurs, indexing happens on the new index, while querying on the old one"
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 0)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 1)

        then: "All aliases, indexes and mappings exist"
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 1)
        es.mappingExists(catalogType.queryingIndex, catalogType.type)
        es.mappingExists(itemType.queryingIndex, itemType.type)
        es.mappingExists(catalogType.indexingIndex, catalogType.type)
        es.mappingExists(itemType.indexingIndex, itemType.type)

        and: "Content isn't lost as it keeps pointing to the old index"
        Catalog.search(queryStringQuery("ACME")).hits.totalHits == 2
        Item.search(queryString("Glue")).hits.totalHits == 1

        when: "Bootstrap runs"
        elasticSearchBootStrapHelper.bulkIndexOnStartup()
        and:
        elasticSearchAdminService.refresh()

        then: "All aliases now point to the new index"
        es.indexPointedBy(catalogType.index) == es.versionIndex(catalogType.index, 1)
        es.indexPointedBy(catalogType.queryingIndex) == es.versionIndex(catalogType.index, 1)
        es.indexPointedBy(catalogType.indexingIndex) == es.versionIndex(catalogType.index, 1)

        and: "Content is still found"
        Catalog.search(queryStringQuery("ACME")).hits.totalHits == 2
        Item.search(queryStringQuery("Glue")).hits.totalHits == 1

        cleanup:
        Catalog.findAll().each {
            it.unindex()
            it.delete()
        }
        Item.findAll().each {
            it.unindex()
            it.delete()
        }

    }

    private ElasticSearchType getCatalogType() {
        elasticSearchLiteContext.getType(Catalog)
    }

    private ElasticSearchType getItemType() {
        elasticSearchLiteContext.getType(Item)
    }

}
