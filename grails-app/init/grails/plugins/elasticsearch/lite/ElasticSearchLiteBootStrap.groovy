package grails.plugins.elasticsearch.lite

import grails.async.Promises

/**
 * Created by marcoscarceles on 20/02/2017.
 */
class ElasticSearchLiteBootStrap {

    ElasticSearchBootStrapHelper elasticSearchBootStrapHelper

    def init = { servletContext ->
        Promises.task {
            elasticSearchBootStrapHelper?.bulkIndexOnStartup()
        }
    }
}
