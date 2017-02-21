package grails.plugins.elasticsearch.lite

import grails.async.Promise
import grails.core.GrailsApplication
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import groovy.util.logging.Slf4j

/**
 * Created by marcoscarceles on 20/02/2017.
 * Created and exposed as a bean, because Bootstrap cannot be easily tested and invoked from IntegrationSpec
 */
@Slf4j
class ElasticSearchBootStrapHelper implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchService elasticSearchService
    def elasticSearchAsyncService
    ElasticSearchAdminService elasticSearchAdminService
    ElasticSearchLiteContext elasticSearchLiteContext


    void bulkIndexOnStartup() {
        elasticSearchLiteContext.autoIndexCompleted = false
        def bulkIndexOnStartup = esConfig?.bulkIndexOnStartup

        Set<Class> classesToIndex

        //Identify classes to index
        if (bulkIndexOnStartup == "migrated") { //Index lost content due to migration
            classesToIndex = elasticSearchLiteContext.getElasticSearchTypes().findAll { Class domainClass, ElasticSearchType esType ->
                esType.index in elasticSearchLiteContext.indicesRebuiltOnMigration
            }.keySet()
        } else if (bulkIndexOnStartup == "all") { //Index all
            classesToIndex = elasticSearchLiteContext.elasticSearchTypes.keySet()
        }

        elasticSearchService.indexAll(classesToIndex as List)
        elasticSearchLiteContext.elasticSearchTypes.keySet()?.each { Class domainClass ->
            ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
            int latestVersion = elasticSearchAdminService.getLatestVersion(esType.index)
            elasticSearchAdminService.pointAliasTo esType.queryingIndex, esType.index, latestVersion
            elasticSearchAdminService.pointAliasTo esType.indexingIndex, esType.index, latestVersion
        }
        elasticSearchLiteContext.autoIndexCompleted = true
    }
}
