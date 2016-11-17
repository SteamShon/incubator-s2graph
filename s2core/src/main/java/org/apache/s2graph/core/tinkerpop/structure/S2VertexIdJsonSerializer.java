package org.apache.s2graph.core.tinkerpop.structure;

import java.io.IOException;

import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

public class S2VertexIdJsonSerializer extends StdSerializer<S2VertexId> {
    public S2VertexIdJsonSerializer() {
        super((Class<S2VertexId>) null);
    }

    protected S2VertexIdJsonSerializer(Class<S2VertexId> t) {
        super(t);
    }

    @Override
    public void serialize(S2VertexId s2VertexID, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("serviceName", s2VertexID.getServiceName());
        jsonGenerator.writeStringField("columnName", s2VertexID.getColumnName());
        jsonGenerator.writeStringField("id", s2VertexID.getId().toString());
        jsonGenerator.writeEndObject();
    }
}
