/*
 * Copyright 2002-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import grails.plugins.Plugin
import grails.plugins.elasticsearch.lite.ElasticSearchClientFactory
import grails.plugins.elasticsearch.lite.ElasticSearchLiteContext
import grails.plugins.elasticsearch.lite.LiteMigrationManager
import grails.plugins.elasticsearch.util.DomainDynamicMethodsUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ElasticsearchLiteGrailsPlugin extends Plugin {

    private static final Logger LOG = LoggerFactory.getLogger(this)

    def grailsVersion = '3.1.1 > *'

    def loadAfter = ['services', 'mongodb']

	def pluginExcludes = [
			"grails-app/views/error.gsp",
			"**/test/**",
			"src/docs/**"
	]

    def license = 'APACHE'

    def organization = [name: 'Marcos Carceles', url: 'http://www.marcoscarceles.com/']

    def developers = [
            [name: 'Marcos Carceles', email: 'marcos.carceles@gmail.com']
    ]

    def issueManagement = [system: 'github', url: 'https://github.com/marcoscarceles/elasticsearch-lite/issues']

    def scm = [url: 'https://github.com/marcoscarceles/elasticsearch-lite']

    def author = 'Marcos Carceles'
    def authorEmail = 'me@marcoscarceles.com'
    def title = 'ElasticSearch Lite'
    def description = """Lightweight Elasticsearch plugin for Grails (for seasoned ES users)"""
    def documentation = ''

    def profiles = ['web']

    Closure doWithSpring() {
        { ->
            elasticSearchLiteContext(ElasticSearchLiteContext) {
                grailsApplication = grailsApplication
            }
            liteMigrationManager(LiteMigrationManager) {
                grailsApplication = grailsApplication
                elasticSearchLiteContext = ref('elasticSearchLiteContext')
                elasticSearchAdminService = ref('elasticSearchAdminService')
            }
            elasticSearchClientFactory (ElasticSearchClientFactory) { bean ->
                grailsApplication = grailsApplication
                elasticSearchLiteContext = ref('elasticSearchLiteContext')
                elasticSearchAdminService = ref('elasticSearchAdminService')
                liteMigrationManager = ref('liteMigrationManager')
                bean.initMethod = 'setup'
            }
        }
    }

    void doWithDynamicMethods() {
        // Define the custom ElasticSearch mapping for searchable domain classes
		if(grailsApplication.config.elasticSearch.disableDynamicMethodsInjection == false) {
			DomainDynamicMethodsUtils.injectDynamicMethods(grailsApplication, applicationContext)
		}
    }
}
