/*
 * Copyright 2002-2010 the original author or authors.
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
package grails.plugins.elasticsearch.util

import grails.core.GrailsDomainClass
import grails.plugins.elasticsearch.lite.ElasticSearchLiteContext
import grails.plugins.elasticsearch.lite.ElasticSearchService
import grails.plugins.elasticsearch.lite.mapping.Searchable
import groovy.util.logging.Slf4j
import org.elasticsearch.index.query.QueryBuilder

/**
 * Created by marcoscarceles on 13/02/2017.
 */
@Slf4j
class DomainDynamicMethodsUtils {

    ElasticSearchLiteContext elasticSearchLiteContext

    /**
     * Injects the dynamic methods in the searchable domain classes.
     * Considers that the mapping has been resolved beforehand.
     *
     * @param grailsApplication
     * @param applicationContext
     * @return
     */
    static injectDynamicMethods(grailsApplication, applicationContext) {
        ElasticSearchService elasticSearchService = applicationContext.getBean(ElasticSearchService)
        ElasticSearchLiteContext elasticSearchLiteContext = applicationContext.getBean(ElasticSearchLiteContext)

        for (GrailsDomainClass domain in grailsApplication.domainClasses) {
            if (!domain.clazz.isAnnotationPresent(Searchable)) {
                continue
            }

            String searchMethodName = grailsApplication.config.elasticSearch.searchMethodName ?: 'search'

            // static search() method
            domain.metaClass.'static'."$searchMethodName" << { QueryBuilder q ->
                elasticSearchService.search(delegate, q)
            }

            // index() method on domain instance
            domain.metaClass.index << {
                elasticSearchService.index(delegate)
            }

            // unindex() method on domain instance
            domain.metaClass.unindex << {
                elasticSearchService.unindex(delegate)
            }
        }
    }
}
