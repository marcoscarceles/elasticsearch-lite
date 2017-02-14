package grails.plugins.elasticsearch.lite

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.health.ClusterHealthStatus

import java.util.regex.Matcher

/**
 * Created by marcoscarceles on 08/02/2017.
 */
class ElasticSearchAdminService {

    static transactional = false

    private static final WAIT_FOR_INDEX_MAX_RETRIES = 10
    private static final WAIT_FOR_INDEX_SLEEP_INTERVAL = 100

    ElasticSearchLiteContext elasticSearchLiteContext
    ElasticSearchService elasticSearchService

    private AdminClient getAdminClient() {
        elasticSearchLiteContext.client.admin()
    }

    /**
     * Delete one or more index and all its data.
     * @param indices The indices to delete. If null, will delete ALL indices.
     */
    void deleteIndex(Collection<String> indices = null) {
        if (!indices) {
            adminClient.indices().delete(Requests.deleteIndexRequest("_all")).actionGet()
            log.warn 'Deleted all indices'
        } else {
            indices.each {
                adminClient.indices().delete(Requests.deleteIndexRequest(it)).actionGet()
            }
            log.info "Deleted indices ${indices}"
        }
    }

    /**
     * Delete one or more index and all its data.
     * @param indices The indices to delete. If null, will delete ALL indices.
     */
    void deleteIndex(String... indices) {
        deleteIndex(indices as Collection<String>)
    }

    /**
     * Delete one or more index and all its data.
     * @param indices The indices to delete in the form of searchable class(es).
     */
    void deleteIndex(Class... searchableClasses) {
        def toDelete = []

        // Retrieve indices to delete
        searchableClasses.each {
            ElasticSearchType esType = elasticSearchLiteContext.getType(it)
            if (esType) {
                toDelete << esType.index
            }
        }
        // We do not trigger the deleteIndex with an empty list as it would delete ALL indices.
        // If toDelete is empty, it might be because of a misuse of a Class the user thought to be a searchable class
        if (!toDelete.isEmpty()) {
            deleteIndex(toDelete.unique())
        }
    }

    /**
     * Creates mappings on a type
     * @param index The index where the mapping is being created
     * @param type The type where the mapping is created
     * @param elasticMapping The mapping definition
     */
    void createMapping(ElasticSearchType elasticSearchType) {
        log.info("Creating Elasticsearch mapping for ${elasticSearchType.index} and type ${elasticSearchType.type} ...")
        adminClient.indices().putMapping(
                new PutMappingRequest(elasticSearchType.index)
                        .type(elasticSearchType.type)
                        .source(elasticSearchType.marshaller.mapping.toString())
        ).actionGet()
    }

    /**
     * Check whether a mapping exists
     * @param index The name of the index to check on
     * @param type The type which mapping is being checked
     * @return true if the mapping exists
     */
    boolean mappingExists(String index, String type) {
        adminClient.indices().typesExists(new TypesExistsRequest([index] as String[], type)).actionGet().exists
    }

    /**
     * Deletes a version of an index
     * @param index The name of the index
     * @param version the version number, if provided <index>_v<version> will be used
     */
    void deleteIndex(String index, Integer version = null) {
        index = versionIndex index, version
        log.info("Deleting  Elasticsearch index ${index} ...")
        adminClient.indices().prepareDelete(index).execute().actionGet()
    }

    /**
     * Creates a new index
     * @param index The name of the index
     * @param settings The index settings (ie. number of shards)
     */
    void createIndex(String index, Map settings=null, List<ElasticSearchType> elasticSearchTypes) {
        log.info "Creating index ${index} ..."

        CreateIndexRequestBuilder builder = adminClient.indices().prepareCreate(index)
        if (settings) {
            builder.setSettings(settings)
        }
        elasticSearchTypes.each {
            builder.addMapping(it.type, it.marshaller.mapping.toString())
        }
        builder.execute().actionGet()
    }

    /**
     * Creates a new index
     * @param index The name of the index
     * @param version the version number, if provided <index>_v<version> will be used
     * @param settings The index settings (ie. number of shards)
     */
    void createIndex(String index, Integer version, Map settings=null, List<ElasticSearchType> elasticSearchTypes) {
        index = versionIndex(index, version)
        createIndex(index, settings, elasticSearchTypes)
    }

    /**
     * Checks whether the index exists
     * @param index The name of the index
     * @param version the version number, if provided <index>_v<version> will be used
     * @return true, if the index exists
     */
    boolean indexExists(String index, Integer version = null) {
        index = versionIndex(index, version)
        adminClient.indices().prepareExists(index).execute().actionGet().exists
    }

    /**
     * Waits for the specified version of the index to exist
     * @param index The name of the index
     * @param version the version number
     */
    void waitForIndex(String index, int version) {
        int retries = WAIT_FOR_INDEX_MAX_RETRIES
        while (getLatestVersion(index) < version && retries--) {
            log.info("Index ${versionIndex(index, version)} not found, sleeping for ${WAIT_FOR_INDEX_SLEEP_INTERVAL}...")
            Thread.sleep(WAIT_FOR_INDEX_SLEEP_INTERVAL)
        }
    }

    /**
     * Returns the name of the index pointed by an alias
     * @param alias The alias to be checked
     * @return the name of the index
     */
    String indexPointedBy(String alias) {
        def index = adminClient.indices().getAliases(new GetAliasesRequest().aliases([alias] as String[])).actionGet().getAliases()?.find {
            alias in it.value*.alias()
        }?.key
        if (!index) {
            log.debug("Alias ${alias} does not exist")
        }
        return index
    }

    /**
     * Deletes an alias pointing to an index
     * @param alias The name of the alias
     */
    void deleteAlias(String alias) {
        adminClient.indices().prepareAliases().removeAlias(indexPointedBy(alias), [alias] as String[]).execute().actionGet()
    }

    /**
     * Makes an alias point to a new index, removing the relationship with a previous index, if any
     * @param alias the alias to be created/modified
     * @param index the index to be pointed to
     * @param version the version number, if provided <index>_v<version> will be used
     */
    void pointAliasTo(String alias, String index, Integer version = null) {
        index = versionIndex(index, version)
        log.info "Creating alias ${alias}, pointing to index ${index} ..."
        String oldIndex = indexPointedBy(alias)
        //Create atomic operation
        def aliasRequest = adminClient.indices().prepareAliases()
        if (oldIndex && oldIndex != index) {
            log.warn "Index used to point to ${oldIndex}, removing ..."
            aliasRequest.removeAlias(oldIndex,alias)
        }
        aliasRequest.   addAlias(index,alias)
        aliasRequest.execute().actionGet()
    }

    /**
     * Checks whether an alias exists
     * @param alias the name of the alias
     * @return true if the alias exists
     */
    boolean aliasExists(String alias) {
        adminClient.indices().prepareAliasesExist(alias).execute().actionGet().exists
    }

    /**
     * Builds an index name based on a base index and a version number
     * @param index
     * @param version
     * @return <index>_v<version> if version is provided, <index> otherwise
     */
    String versionIndex(String index, Integer version = null) {
        version == null ? index : index + "_v${version}"
    }

    /**
     * Returns all the indices
     * @param prefix the prefix
     * @return a Set of index names
     */
    Set<String> getIndices() {
        adminClient.indices().prepareStats().execute().actionGet().indices.keySet()
    }

    /**
     * Returns all the indices starting with a prefix
     * @param prefix the prefix
     * @return a Set of index names
     */
    Set<String> getIndices(String prefix) {
        Set indices = getIndices()
        if (prefix) {
            indices = indices.findAll {
                it =~ /^${prefix}/
            }
        }
        indices
    }

    /**
     * The current version of the index
     * @param index
     * @return the current version if any exists, -1 otherwise
     */
    int getLatestVersion(String index) {
        def versions = getIndices(index).collect {
            Matcher m = (it =~ /^${index}_v(\d+)$/)
            m ? m[0][1] as Integer : -1
        }.sort()
        versions ? versions.last() : -1
    }

    /**
     * The next available version for an index
     * @param index the index name
     * @return an integer representing the next version to be used for this index (ie. 10 if the latest is <index>_v<9>)
     */
    int getNextVersion(String index) {
        getLatestVersion(index) + 1
    }

    /**
     * Waits for the cluster to be on Yellow status
     */
    void waitForClusterStatus(ClusterHealthStatus status=ClusterHealthStatus.YELLOW) {
        ClusterHealthResponse response = adminClient.cluster().health(new ClusterHealthRequest([] as String[]).waitForStatus(status)).actionGet()
        log.info("Cluster status: ${response.status}")
    }
}
