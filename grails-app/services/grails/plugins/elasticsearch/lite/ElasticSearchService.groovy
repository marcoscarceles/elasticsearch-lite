package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import grails.util.Holders
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.grails.datastore.mapping.engine.event.AbstractPersistenceEvent
import org.grails.datastore.mapping.engine.event.PostInsertEvent
import org.grails.datastore.mapping.engine.event.PostUpdateEvent
import org.grails.io.support.PathMatchingResourcePatternResolver
import org.grails.io.support.Resource
import reactor.spring.context.annotation.Consumer
import reactor.spring.context.annotation.Selector

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Consumer
final class ElasticSearchService implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
    ElasticSearchLiteContext elasticSearchLiteContext
    private TransportClient client

    synchronized Client getClient() {

        if(!this.@client) {
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
                this.@client = TransportClient.builder().addPlugin(shieldPluginClass).settings(transportSettings).build();
                log.info("Shield Enabled")
            } catch (ClassNotFoundException e) {
                this.@client = TransportClient.builder().settings(transportSettings).build()
            }

            // Configure transport addresses
            if (!esConfig.client.hosts) {
                this.@client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress('localhost', 9300)))
            } else {
                esConfig.client.hosts.each {
                    try {
                        for (InetAddress address : InetAddress.getAllByName(it.host)) {
                            if ((ip6Enabled && address instanceof Inet6Address) || (ip4Enabled && address instanceof Inet4Address)) {
                                log.info("Adding host: ${address}")
                                this.@client.addTransportAddress(new InetSocketTransportAddress(address, it.port));
                            }
                        }
                    } catch (UnknownHostException e) {
                        log.error("Unable to get the host", e.getMessage());
                    }
                }
            }
        }
        return this.@client
    }

    @Selector('gorm:postInsert')
    void onInsert(PostInsertEvent event) {
        onUpsert(event);
    }

    @Selector('gorm:postUpdate')
    void onUpdate(PostUpdateEvent event) {
        onUpsert(event);
    }

    void onUpsert(AbstractPersistenceEvent event) {
        index(event.entityObject)
    }

    @Selector('gorm:postDelete')
    void onDelete(AbstractPersistenceEvent event) {
        unindex(event.entityObject)
    }

    void index(Object domainObject) {
        Class domainClass = domainObject.class

        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Indexing instance of ${domainClass} with id ${domainObject.id} into ElasticSearch")
            try {
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)
                ElasticSearchMarshaller marshaller = elasticSearchLiteContext.getMarshaller(domainClass)

                IndexResponse response = client.prepareIndex(esType.indexingIndex, esType.type, domainObject.id as String)
                        .setSource(marshaller.toSource(domainObject)).get()
            } catch (Exception e) {
                log.debug("Unable to index instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
            }
        }
    }

    void unindex(Object domainObject) {
        Class domainClass = domainObject.class

        if(elasticSearchLiteContext.isSearchable(domainClass)) {
            log.debug("Deleting instance of ${domainClass} with id ${domainObject.id} from ElasticSearch")
            try {
                ElasticSearchType esType = elasticSearchLiteContext.getType(domainClass)

                DeleteResponse response = client.prepareDelete(esType.indexingIndex, esType.type, domainObject.id as String).get()
            } catch (Exception e) {
                log.debug("Unable to delete instance of ${domainClass} with id ${domainObject.id} due to Exception", e)
            }
        }

    }

}
