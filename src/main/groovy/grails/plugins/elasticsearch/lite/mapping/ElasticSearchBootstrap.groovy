package grails.plugins.elasticsearch.lite.mapping

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.mapping.MappingMigrationStrategy
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import grails.plugins.elasticsearch.util.IndexNamingUtils
import groovy.util.logging.Slf4j
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.index.mapper.MergeMappingException
import org.elasticsearch.transport.RemoteTransportException

import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.none

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
class ElasticSearchBootstrap implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchAdminClient elasticSearchAdminClient
    ElasticSearchLiteContext elasticSearchLiteContext
    LiteMigrationManager liteMigrationManager

    void installMappings() {
        Map<Class<?>, ElasticSearchType> types = elasticSearchLiteContext.getElasticSearchTypes()
        Map<String, Map<Class<?>, ElasticSearchType>> indices = types.groupBy {k, v ->
            v.index
        }

        Map<String, Object> indexSettings = buildIndexSettings()

        log.debug("Index settings are " + indexSettings)

        log.debug("Installing mappings...")
        log.debug "indices are ${indices.keySet()}"

        MappingMigrationStrategy migrationStrategy = migrationConfig?.strategy ? MappingMigrationStrategy.valueOf(migrationConfig?.strategy as String) : none
        List<ElasticSearchType> mappingConflicts = []

        //Install the mappings for each index all together
        indices.each { indexName, indexTypes ->

            log.debug("Installing mappings for index " + indexName)

            //If the index does not exist we attempt to create all the mappings at once with it
            if(!elasticSearchAdminClient.indexExists(indexName)) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Creating index [" + indexName + "] => with new mappings:")
                        indexTypes.each { Class clazz, ElasticSearchType elasticSearchType ->
                            log.debug("\t\tMapping ["+ elasticSearchType.type +"] => " + elasticSearchType.marshaller.mapping.toPrettyString())
                        }
                    }
                    createIndexWithMappings(indexName,  indexTypes.values(), indexSettings)
                } catch (RemoteTransportException rte) {
                    log.debug(rte.getMessage())
                }
            } else { //We install the mappings one by one
                indexTypes.each { Class clazz, elasticSearchType ->
                    // Install mapping
                    if (log.isDebugEnabled()) {
                        log.debug("Installing mapping [" + elasticSearchType.type + "] => " + elasticSearchType.marshaller.mapping.toPrettyString())
                    }
                    try {
                        elasticSearchAdminClient.createMapping elasticSearchType
                    } catch (IllegalArgumentException e) {
                        log.warn("Could not install mapping ${indexName}/${elasticSearchType.type} due to ${e.getClass()}: ${e.message}, migrations needed")
                        mappingConflicts << elasticSearchType
                    } catch (MergeMappingException e) {
                        log.warn("Could not install mapping ${indexName}/${elasticSearchType.type} due to ${e.getClass()}: ${e.message}, migrations needed")
                        mappingConflicts << elasticSearchType
                    }
                }
            }
            //Create them only if they don't exist so it does not mess with other migrations
            String queryingIndex = IndexNamingUtils.queryingIndexFor(indexName)
            String indexingIndex = IndexNamingUtils.indexingIndexFor(indexName)
            if(!elasticSearchAdminClient.aliasExists(queryingIndex)) {
                elasticSearchAdminClient.pointAliasTo(queryingIndex, indexName)
                elasticSearchAdminClient.pointAliasTo(indexingIndex, indexName)
            }
        }
        if(mappingConflicts) {
            log.info("Applying migrations ...")
            liteMigrationManager.applyMigrations(migrationStrategy, indices, mappingConflicts, indexSettings)
        }

        elasticSearchAdminClient.waitForClusterStatus(ClusterHealthStatus.YELLOW)
    }

    /**
     * Creates the Elasticsearch index once unblocked and its read and write aliases
     * @param indexName
     * @throws org.elasticsearch.transport.RemoteTransportException if some other error occured
     */
    private void createIndexWithMappings(String indexName, List<ElasticSearchType> types, Map indexSettings) throws RemoteTransportException {
        // Could be blocked on cluster level, thus wait.
        elasticSearchAdminClient.waitForClusterStatus(ClusterHealthStatus.YELLOW)
        if(!elasticSearchAdminClient.indexExists(indexName)) {
            log.debug("Index ${indexName} does not exists, initiating creation...")
            def nextVersion = elasticSearchAdminClient.getNextVersion indexName
            elasticSearchAdminClient.createIndex indexName, nextVersion, indexSettings, types
            elasticSearchAdminClient.pointAliasTo indexName, indexName, nextVersion
        }
    }

    private Map<String, Object> buildIndexSettings() {
        Map<String, Object> indexSettings = new HashMap<String, Object>()
        indexSettings.put("number_of_replicas", numberOfReplicas())
        // Look for default index settings.
        if (esConfig != null) {
            Map<String, Object> indexDefaults = esConfig.get("index") as Map<String, Object>
            log.debug("Retrieved index settings")
            if (indexDefaults != null) {
                for (Map.Entry<String, Object> entry : indexDefaults.entrySet()) {
                    indexSettings.put("index." + entry.getKey(), entry.getValue())
                }
            }
        }
        indexSettings
    }

    private int numberOfReplicas() {
        def defaultNumber = (esConfig.index as ConfigObject).numberOfReplicas
        if (!defaultNumber) {
            return 0
        }
        defaultNumber as int
    }
}
