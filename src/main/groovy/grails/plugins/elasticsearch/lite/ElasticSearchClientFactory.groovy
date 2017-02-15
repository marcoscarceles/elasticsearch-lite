package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.mapping.MappingMigrationStrategy
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import grails.plugins.elasticsearch.util.IndexNamingUtils
import groovy.util.logging.Slf4j
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.mapper.MergeMappingException
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.transport.RemoteTransportException
import org.springframework.core.io.Resource
import org.springframework.core.io.support.PathMatchingResourcePatternResolver

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import static grails.plugins.elasticsearch.mapping.MappingMigrationStrategy.none

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
class ElasticSearchClientFactory implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchLiteContext elasticSearchLiteContext
    ElasticSearchAdminService elasticSearchAdminService
    LiteMigrationManager liteMigrationManager

    static final SUPPORTED_MODES = ['local', 'transport']

    void setup() {
        elasticSearchLiteContext.client = buildClient()
        initializeIndices()
    }

    void initializeIndices() {
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
            if(!elasticSearchAdminService.indexExists(indexName)) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Creating index [" + indexName + "] => with new mappings:")
                        indexTypes.each { Class clazz, ElasticSearchType elasticSearchType ->
                            log.debug("\t\tMapping ["+ elasticSearchType.type +"] => " + elasticSearchType.marshaller.mapping.toPrettyString())
                        }
                    }
                    createIndexWithMappings(indexName,  indexTypes.values() as List, indexSettings)
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
                        elasticSearchAdminService.createMapping elasticSearchType
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
            if(!elasticSearchAdminService.aliasExists(queryingIndex)) {
                elasticSearchAdminService.pointAliasTo(queryingIndex, indexName)
                elasticSearchAdminService.pointAliasTo(indexingIndex, indexName)
            }
        }
        if(mappingConflicts) {
            log.info("Applying migrations ...")
            liteMigrationManager.applyMigrations(migrationStrategy, indices, mappingConflicts, indexSettings)
        }

        elasticSearchAdminService.waitForClusterStatus(ClusterHealthStatus.YELLOW)
    }


    Client buildClient() {
        Client client
        String clientMode = esConfig.client.mode ?: 'local'

        if (!(clientMode in SUPPORTED_MODES)) {
            throw new IllegalArgumentException("Invalid client mode, expected values were ${SUPPORTED_MODES}.")
        }

        switch (clientMode) {
            case 'local':
                client = buildLocalClient()
                break;
            case 'transport':
                client = buildTransportClient()
        }
        return client
    }

    Client buildLocalClient() {

        Settings.Builder settings = getBaseSettings()

        // Determines how the data is stored (on disk, in memory, ...)
        def storeType = esConfig.index.store.type
        if (storeType) {
            settings.put('index.store.type', storeType as String)
            log.debug "Local ElasticSearch client with store type of ${storeType} configured."
        } else {
            log.debug "Local ElasticSearch client with default store type configured."
        }
        def gatewayType = esConfig.gateway.type
        if (gatewayType) {
            settings.put('gateway.type', gatewayType as String)
            log.debug "Local ElasticSearch client with gateway type of ${gatewayType} configured."
        } else {
            log.debug "Local ElasticSearch client with default gateway type configured."
        }
        def queryParsers = esConfig.index.queryparser
        if (queryParsers) {
            queryParsers.each { type, clz ->
                settings.put("index.queryparser.types.${type}".toString(), clz)
            }
        }

        def pluginsDirectory = esConfig.path.plugins
        if (pluginsDirectory) {
            settings.put('path.plugins', new File(pluginsDirectory as String).absolutePath)
        }

        // Path to the config folder of ES
        def confDirectory = esConfig.path.conf
        if (confDirectory) {
            settings.put('path.conf', confDirectory as String)
        }

        def tmpDirectory = tmpDirectory()
        log.info "Setting embedded ElasticSearch tmp dir to ${tmpDirectory}"
        settings.put("path.home", tmpDirectory)

        settings.put("node.local", true)

        //Build node and get client
        Node node = NodeBuilder.nodeBuilder().settings(settings).node()
        node.start()

        Client client = node.client()
        // Wait for the cluster to become alive.
        log.info "Waiting for ElasticSearch YELLOW status."
        client.admin().cluster().health(new ClusterHealthRequest().waitForYellowStatus()).actionGet()

        return client
    }

    Settings.Builder getBaseSettings() {
        Settings.Builder settings = Settings.settingsBuilder()

        def configFile = esConfig.bootstrap.config.file
        if (configFile) {
            log.info "Looking for bootstrap configuration file at: $configFile"
            Resource resource = new PathMatchingResourcePatternResolver().getResource(configFile)
            settings = settings.loadFromStream(configFile, resource.inputStream)
        }

        // Cluster name
        if (esConfig.cluster.name) {
            settings.put('cluster.name', esConfig.cluster.name)
        }

        // Path to the data folder of ES
        def dataPath = esConfig.path.data
        if (dataPath) {
            settings.put('path.data', dataPath as String)
            log.info "Using ElasticSearch data path: ${dataPath}"
        }

        //Inject http settings...
        if (esConfig.http) {
            flattenMap(esConfig.http).each { p ->
                settings.put("http.${p.key}", p.value as String)
            }
        }

        settings
    }

    private String tmpDirectory() {
        String baseDirectory = System.getProperty("java.io.tmpdir") ?: '/tmp'
        Path path = Files.createTempDirectory(Paths.get(baseDirectory), 'elastic-data-' + new Date().time)
        File file = path.toFile()
        file.deleteOnExit()
        return file.absolutePath
    }

    //From http://groovy.329449.n5.nabble.com/Flatten-Map-using-closure-td364360.html
    def flattenMap(map) {
        [:].putAll(map.entrySet().flatten { it.value instanceof Map ? it.value.collect { k, v -> new MapEntry(it.key + '.' + k, v) } : it })
    }


    TransportClient buildTransportClient() {

        TransportClient client

        def transportSettings = Settings.settingsBuilder()

        def transportSettingsFile = esConfig.bootstrap.transportSettings.file
        if (transportSettingsFile) {
            Resource resource = new PathMatchingResourcePatternResolver().getResource(transportSettingsFile)
            transportSettings.loadFromStream(transportSettingsFile, resource.inputStream)
        }
        // Use the "sniff" feature of transport client ?
        if (esConfig.client.transport.sniff) {
            transportSettings.put("client.transport.sniff", false)
        }
        if (esConfig.cluster.name) {
            transportSettings.put('cluster.name', esConfig.cluster.name.toString())
        }
        boolean ip4Enabled = esConfig.shield.ip4Enabled ?: true
        boolean ip6Enabled = esConfig.shield.ip6Enabled ?: false

        try {
            Class shieldPluginClass = Class.forName("org.elasticsearch.shield.ShieldPlugin")
            client = TransportClient.builder().addPlugin(shieldPluginClass).settings(transportSettings).build();
            log.info("Shield Enabled")
        } catch (ClassNotFoundException e) {
            client = TransportClient.builder().settings(transportSettings).build()
        }

        // Configure transport addresses
        if (!esConfig.client.hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress('localhost', 9300)))
        } else {
            esConfig.client.hosts.each {
                try {
                    for (InetAddress address : InetAddress.getAllByName(it.host)) {
                        if ((ip6Enabled && address instanceof Inet6Address) || (ip4Enabled && address instanceof Inet4Address)) {
                            log.info("Adding host: ${address}")
                            client.addTransportAddress(new InetSocketTransportAddress(address, it.port));
                        }
                    }
                } catch (UnknownHostException e) {
                    log.error("Unable to get the host", e.getMessage());
                }
            }
        }

        client
    }

    /**
     * Creates the Elasticsearch index once unblocked and its read and write aliases
     * @param indexName
     * @throws org.elasticsearch.transport.RemoteTransportException if some other error occured
     */
    private void createIndexWithMappings(String indexName, List<ElasticSearchType> types, Map indexSettings) throws RemoteTransportException {
        // Could be blocked on cluster level, thus wait.
        elasticSearchAdminService.waitForClusterStatus(ClusterHealthStatus.YELLOW)
        if(!elasticSearchAdminService.indexExists(indexName)) {
            log.debug("Index ${indexName} does not exists, initiating creation...")
            def nextVersion = elasticSearchAdminService.getNextVersion indexName
            elasticSearchAdminService.createIndex indexName, nextVersion, indexSettings, types
            elasticSearchAdminService.pointAliasTo indexName, indexName, nextVersion
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
