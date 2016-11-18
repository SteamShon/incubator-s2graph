/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.gremlin.structure;

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
    private Integer serviceId;
    private Integer columnId;

    public S2VertexId(Service service, ServiceColumn column, Object id) {
        this.service = service;
        this.column = column;
        this.id = id;

        this.serviceName = service.serviceName();
        this.columnName = column.columnName();

        this.serviceId = (Integer) service.id().get();
        this.columnId = (Integer) column.id().get();
    }

    public S2Vertex toS2Vertex(S2Graph graph) {
        return new S2Vertex(graph, this);
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

    @Override
    public String toString() {
        return "S2VertexId{" +
                "serviceName='" + serviceName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        S2VertexId that = (S2VertexId) o;
        if (!service.equals(that.service)) return false;
        if (!column.equals(that.column)) return false;
        //TODO: Check this is valid.
        return id.toString().equals(that.id.toString());
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + service.hashCode();
        result = 31 * result + column.hashCode();
        return result;
    }

    public Integer serviceId() { return serviceId; }

    public Integer columnId() { return columnId; }
}
