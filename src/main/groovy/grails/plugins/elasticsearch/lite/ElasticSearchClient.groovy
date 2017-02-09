package grails.plugins.elasticsearch.lite

import grails.core.GrailsApplication
import grails.plugins.elasticsearch.util.ElasticSearchConfigAware
import groovy.util.logging.Slf4j
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.grails.io.support.PathMatchingResourcePatternResolver
import org.grails.io.support.Resource

/**
 * Created by marcoscarceles on 08/02/2017.
 */
@Slf4j
final class ElasticSearchClient implements ElasticSearchConfigAware {

    GrailsApplication grailsApplication
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
}
