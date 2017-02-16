package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.mapping.MappingMigrationStrategy
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import grails.plugins.elasticsearch.util.IndexNamingUtils
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
@CompileStatic
class LiteMigrationManager implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchLiteContext elasticSearchLiteContext
    ElasticSearchAdminService elasticSearchAdminService

    def applyMigrations(MappingMigrationStrategy migrationStrategy, Map<String, Map<Class<?>, ElasticSearchType>> indices, List<ElasticSearchType> mappingConflicts, Map indexSettings) {
        elasticSearchLiteContext.indexesRebuiltOnMigration = applyAliasStrategy(indices, mappingConflicts, indexSettings)
    }

    private Set applyAliasStrategy(Map<String, Map<Class<?>, ElasticSearchType>> indices, List<ElasticSearchType> mappingConflicts, Map indexSettings) {

        Set<String> indexNames = mappingConflicts.collect { it.index } as Set<String>

        indexNames.each { String indexName ->
            log.debug("Creating new version and alias for conflicting index ${indexName}")
            boolean conflictOnAlias = elasticSearchAdminService.aliasExists(indexName)
            if (!conflictOnAlias) {
                elasticSearchAdminService.deleteIndex(indexName)
            }
            int nextVersion = elasticSearchAdminService.getNextVersion(indexName)
            boolean buildQueryingAlias = (!conflictOnAlias || !migrationConfig?.disableAliasChange)
            List<ElasticSearchType> elasticSearchTypes = indices[indexName].values() as List
            rebuildIndexWithMappings(indexName, nextVersion, indexSettings, elasticSearchTypes, buildQueryingAlias)
        }
        indices.keySet()
    }

    private void rebuildIndexWithMappings(String indexName, int nextVersion, Map indexSettings, List<ElasticSearchType> elasticSearchTypes, boolean buildQueryingAlias) {
        elasticSearchAdminService.createIndex indexName, nextVersion, indexSettings, elasticSearchTypes
        elasticSearchAdminService.waitForIndex indexName, nextVersion //Ensure it exists so later on mappings are created on the right version
        elasticSearchAdminService.pointAliasTo indexName, indexName, nextVersion
        elasticSearchAdminService.pointAliasTo IndexNamingUtils.indexingIndexFor(indexName), indexName, nextVersion
        if (buildQueryingAlias) {
            elasticSearchAdminService.pointAliasTo IndexNamingUtils.queryingIndexFor(indexName), indexName, nextVersion
        }
    }
}
