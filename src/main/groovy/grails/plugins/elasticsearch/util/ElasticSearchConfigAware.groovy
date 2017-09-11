package grails.plugins.elasticsearch.util

import grails.core.GrailsApplication
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue

/**
 * Created by marcoscarceles on 02/06/2016.
 */
trait ElasticSearchConfigAware {

    abstract GrailsApplication getGrailsApplication()

    ConfigObject getEsConfig() {
        grailsApplication?.config?.elasticSearch as ConfigObject
    }

    ConfigObject getMigrationConfig() {
        (grailsApplication?.config?.elasticSearch as ConfigObject)?.migration as ConfigObject
    }

    int getBulkBatchSize() {
        esConfig.bulkBatchSize ?: 500
    }

    ByteSizeValue getBulkMemorySize() {
        long megs = (esConfig.bulkMemorySize ?: 5) as long
        new ByteSizeValue(5, ByteSizeUnit.MB)
    }

    int getConcurrentRequests() {
        (esConfig.bulkConcurrency ?: 0) as int //Although default is 1, I don't want to override 0
    }

    TimeValue getBulkInterval() {
        long millis = (esConfig.bulkInterval ?: 500) as long
        TimeValue.timeValueMillis(millis)
    }

}
