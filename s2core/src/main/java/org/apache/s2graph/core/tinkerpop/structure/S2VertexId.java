package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.s2graph.core.mysqls.Service;
import org.apache.s2graph.core.mysqls.ServiceColumn;
import org.apache.tinkerpop.shaded.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = S2VertexIdJsonSerializer.class)
public class S2VertexId {
    private Service service;
    private ServiceColumn column;
    private Object id;
    private String serviceName;
    private String columnName;

    public S2VertexId(Service service, ServiceColumn column, Object id) {
        this.service = service;
        this.column = column;
        this.id = id;

        this.serviceName = service.serviceName();
        this.columnName = column.columnName();
    }

    public S2Vertex toS2Vertex(S2Graph graph) {
        return new S2Vertex(graph, this);
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        S2VertexId that = (S2VertexId) o;
        if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null)
            return false;
        if (columnName != null ? !columnName.equals(that.columnName) : that.columnName != null)
            return false;
        return !(id != null ? !id.equals(that.id) : that.id != null);

    }

    @Override
    public int hashCode() {
        int result = serviceName != null ? serviceName.hashCode() : 0;
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    public ServiceColumn getColumn() {
        return column;
    }

    public void setColumn(ServiceColumn column) {
        this.column = column;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
