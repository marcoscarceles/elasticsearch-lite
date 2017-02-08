package grails.plugins.elasticsearch.lite.mapping

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.exception.MappingException
import grails.plugins.elasticsearch.mapping.MappingConflict
import grails.plugins.elasticsearch.mapping.MappingMigrationStrategy
import grails.plugins.elasticsearch.mapping.SearchableClassMapping
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import grails.plugins.elasticsearch.util.IndexNamingUtils
import groovy.util.logging.Slf4j

import javax.persistence.Index

import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.alias
import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.delete
import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.deleteIndex
import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.none
import static grails.plugins.elasticsearch.util.IndexNamingUtils.indexingIndexFor
import static grails.plugins.elasticsearch.util.IndexNamingUtils.queryingIndexFor

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
class LiteMigrationManager implements ElasticSearchConfigAware {

    ElasticSearchAdminClient elasticSearchAdminClient
    ElasticSearchLiteContext elasticSearchLiteContext
    GrailsApplication grailsApplication

    def applyMigrations(MappingMigrationStrategy migrationStrategy, Map<String, Map<Class<?>, ElasticSearchType>> indices, List<ElasticSearchType> mappingConflicts, Map indexSettings) {
        elasticSearchLiteContext.indexesRebuiltOnMigration = applyAliasStrategy(indices, mappingConflicts, indexSettings)
    }

    private Set applyAliasStrategy(Map<String, Map<Class<?>, ElasticSearchType>> indices, List<ElasticSearchType> mappingConflicts, Map indexSettings) {

        Set<String> indexNames = mappingConflicts.collect { it.index } as Set<String>

        indexNames.each { String indexName ->
            log.debug("Creating new version and alias for conflicting index ${indexName}")
            boolean conflictOnAlias = elasticSearchAdminClient.aliasExists(indexName)
            if (!conflictOnAlias) {
                elasticSearchAdminClient.deleteIndex(indexName)
            }
            int nextVersion = elasticSearchAdminClient.getNextVersion(indexName)
            boolean buildQueryingAlias = (!esConfig.bulkIndexOnStartup) && (!conflictOnAlias || !migrationConfig?.disableAliasChange)
            List<ElasticSearchType> elasticSearchTypes = indices[indexName].values()
            rebuildIndexWithMappings(indexName, nextVersion, indexSettings, elasticSearchTypes, buildQueryingAlias)
        }
        indices
    }

    private void rebuildIndexWithMappings(String indexName, int nextVersion, Map indexSettings, List<ElasticSearchType> elasticSearchTypes, boolean buildQueryingAlias) {
        elasticSearchAdminClient.createIndex indexName, nextVersion, indexSettings, elasticSearchTypes
        elasticSearchAdminClient.waitForIndex indexName, nextVersion //Ensure it exists so later on mappings are created on the right version
        elasticSearchAdminClient.pointAliasTo indexName, indexName, nextVersion
        elasticSearchAdminClient.pointAliasTo IndexNamingUtils.indexingIndexFor(indexName), indexName, nextVersion
        if (buildQueryingAlias) {
            elasticSearchAdminClient.pointAliasTo IndexNamingUtils.queryingIndexFor(indexName), indexName, nextVersion
        }
    }
}
