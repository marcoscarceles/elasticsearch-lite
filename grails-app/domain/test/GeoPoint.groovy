package test

import grails.plugins.elasticsearch.lite.mapping.ElasticSearchMarshaller
import grails.plugins.elasticsearch.lite.mapping.Mapping
import groovy.json.JsonBuilder
import org.elasticsearch.common.xcontent.XContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory

@Mapping(GeoPointMarshaller)
class GeoPoint {

    Double lat
    Double lon

    static searchable = {
        root false
    }
}

class GeoPointMarshaller implements ElasticSearchMarshaller<GeoPoint> {

    @Override
    JsonBuilder getMapping() {
        throw new UnsupportedOperationException()
    }

    @Override
    XContentBuilder toSource(XContentBuilder source = XContentFactory.jsonBuilder(), GeoPoint instance) {
        source.startObject()
                .field('lat', instance.lat)
                .field('lon', instance.lon)
        source.endObject()
    }
}
