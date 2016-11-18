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


import org.apache.s2graph.core.*;
import org.apache.s2graph.core.mysqls.ColumnMeta;
import org.apache.s2graph.core.mysqls.ServiceColumn;
import org.apache.s2graph.core.types.InnerValLike;
import org.apache.s2graph.core.types.VertexId;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import scala.collection.JavaConversions;
import scala.concurrent.Await;

import java.util.*;

public class S2Vertex implements Vertex {
    private S2Graph graph;
    private S2VertexId vertexId;
    private Map<String, VertexProperty<?>> props;
    private long ts;
    private String operation;


    public S2Vertex(S2Graph graph, Object... keyValues) {
        if (keyValues.length < 6) throw new RuntimeException("not enough parameter for S2VertexId.");
        Pair<S2VertexId, Map<String, Object>> pair = S2GraphUtil.toS2VertexParam(keyValues);

        this.graph = graph;
        this.vertexId = pair.getValue0();
        this.props = new HashMap<>();
        this.ts = System.currentTimeMillis();
        this.operation = "insert";
        for (Map.Entry<String, Object> e : pair.getValue1().entrySet()) {
            property(VertexProperty.Cardinality.single, e.getKey(), e.getValue());
        }
    }

    public S2Vertex(S2Graph graph, S2VertexId vertexId, Map<String, Object> keyValues) {
        this.graph = graph;
        this.vertexId = vertexId;
        this.props = new HashMap<>();
        this.ts = System.currentTimeMillis();
        this.operation = "insert";
        for (Map.Entry<String, Object> e : keyValues.entrySet()) {
            property(e.getKey(), e.getValue());
        }
    }

    public S2Vertex(S2Graph graph, S2VertexId vertexId, Object... keyValues) {
        this.graph = graph;
        this.vertexId = vertexId;
        this.props = new HashMap<>();
        this.ts = System.currentTimeMillis();
        this.operation = "insert";
        final Map<String, Object> kvs = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> e : kvs.entrySet()) {
            property(e.getKey(), e.getValue());
        }
    }

    // TODO: Need to add more constructor for handling property and ts and operaiton.
    public S2Vertex(S2Graph graph, org.apache.s2graph.core.Vertex vertex) {
        this.graph = graph;
        this.vertexId = new S2VertexId(vertex.service(), vertex.serviceColumn(), vertex.innerIdVal());
        this.ts = vertex.ts();
        this.operation = vertex.operation();
        this.props = new HashMap<>();
        for (Map.Entry<ColumnMeta, Object> e : JavaConversions.mapAsJavaMap(vertex.toProperties()).entrySet()) {
            ColumnMeta meta = e.getKey();
            property(VertexProperty.Cardinality.single, meta.name(), e.getValue());
        }
    }

    @Override
    public Edge addEdge(String label, Vertex outV, Object... objects) {
        S2Vertex s2OutV = (S2Vertex) outV;
        Map<String, Object> props = ElementHelper.asMap(objects);
        String direction = (String) props.getOrDefault("direction", "out");
        Long ts = (Long) props.getOrDefault("timestamp", System.currentTimeMillis());
        String operation = (String) props.getOrDefault("operation", GraphUtil.defaultOp());
        S2Edge s2Edge = new S2Edge(graph, this, s2OutV, label, direction, props, ts, operation);
        try {
            org.apache.s2graph.core.Edge innerE = s2Edge.toInnerEdge();
            boolean success = (boolean) Await.result(graph.getG().mutateEdge(innerE, true), S2Graph.timeout);
            System.out.println("[AddEdge]: " + innerE + ", " + success);
            return s2Edge;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        List<Edge> results = new ArrayList<>();
        List<org.apache.s2graph.core.Vertex> vertices = new ArrayList<>();
        List<Step> steps = new ArrayList<>();
        List<QueryParam> queryParams = new ArrayList<>();

        // step 1. set up start vertices.
        vertices.add(toInnerVertex());
        System.out.println("[SrcVertex]: " + Arrays.toString(vertices.toArray()));
        // step 2. build QueryParam for this step.
        for (int i = 0; i < labels.length; i++) {
            String labelName = labels[i];
            QueryParam queryParam = new QueryParam(
                    labelName,
                    QueryParam$.MODULE$.apply$default$2(),
                    QueryParam$.MODULE$.apply$default$3(),
                    QueryParam$.MODULE$.apply$default$4(),
                    QueryParam$.MODULE$.apply$default$5(),
                    QueryParam$.MODULE$.apply$default$6(),
                    QueryParam$.MODULE$.apply$default$7(),
                    QueryParam$.MODULE$.apply$default$8(),
                    QueryParam$.MODULE$.apply$default$9(),
                    QueryParam$.MODULE$.apply$default$10(),
                    QueryParam$.MODULE$.apply$default$11(),
                    QueryParam$.MODULE$.apply$default$12(),
                    QueryParam$.MODULE$.apply$default$13(),
                    QueryParam$.MODULE$.apply$default$14(),
                    QueryParam$.MODULE$.apply$default$15(),
                    QueryParam$.MODULE$.apply$default$16(),
                    QueryParam$.MODULE$.apply$default$17(),
                    QueryParam$.MODULE$.apply$default$18(),
                    QueryParam$.MODULE$.apply$default$19(),
                    QueryParam$.MODULE$.apply$default$20(),
                    QueryParam$.MODULE$.apply$default$21(),
                    QueryParam$.MODULE$.apply$default$22(),
                    QueryParam$.MODULE$.apply$default$23(),
                    QueryParam$.MODULE$.apply$default$24(),
                    QueryParam$.MODULE$.apply$default$25(),
                    QueryParam$.MODULE$.apply$default$26(),
                    QueryParam$.MODULE$.apply$default$27(),
                    QueryParam$.MODULE$.apply$default$28());
            queryParams.add(queryParam);
        }

        // 3. set up current step.
        steps.add(new Step(JavaConversions.asScalaBuffer(queryParams),
                Step$.MODULE$.apply$default$2(),
                Step$.MODULE$.apply$default$3(),
                Step$.MODULE$.apply$default$4(),
                Step$.MODULE$.apply$default$5(),
                Step$.MODULE$.apply$default$6()
        ));

        // 4. build Query.
        Query query = new Query(JavaConversions.asScalaBuffer(vertices),
                JavaConversions.asScalaBuffer(steps),
                Query.DefaultQueryOption(),
                Query.DefaultJsonQuery());

        System.out.println("[Query]: " + query);

        try {
            final StepResult stepResult = Await.result(graph.getG().getEdges(query), S2Graph.timeout);
            final List<EdgeWithScore> edgeWithScores = JavaConversions.seqAsJavaList(stepResult.edgeWithScores());
            System.out.println("[Result]: " + edgeWithScores.size());
            for (EdgeWithScore es : edgeWithScores) {
                S2Edge s2Edge = new S2Edge(graph, es.edge());
                System.out.println("[S2Edge]: " + s2Edge);
                results.add(s2Edge);
            }
            return results.iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        List<Vertex> vertices = new ArrayList<>();
        Iterator<Edge> iter = edges(direction, edgeLabels);
        while (iter.hasNext()) {
            Edge edge = iter.next();
            if (direction == Direction.OUT) {
                vertices.add(edge.inVertex());
            } else if (direction == Direction.IN) {
                vertices.add(edge.outVertex());
            } else {
                vertices.add(edge.inVertex());
                vertices.add(edge.outVertex());
            }
        }
        return vertices.iterator();
    }


    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (cardinality == VertexProperty.Cardinality.single) {
            VertexProperty<V> newProperty = new S2VertexProperty<V>(this, key, value);
            props.put(key, newProperty);
            return newProperty;
        } else {
            throw new RuntimeException("only support single cardinalrity currently." + cardinality);
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        return null;
    }

    @Override
    public Object id() {
        return vertexId;
    }

    @Override
    public String label() {
        return null;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        // to nothing.
    }


    public S2Graph getGraph() {
        return graph;
    }

    public void setGraph(S2Graph graph) {
        this.graph = graph;
    }

    public S2VertexId getVertexId() {
        return vertexId;
    }

    public void setVertexId(S2VertexId vertexId) {
        this.vertexId = vertexId;
    }

    public Map<String, VertexProperty<?>> getProps() {
        return props;
    }

    public void setProps(Map<String, VertexProperty<?>> props) {
        this.props = props;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "S2Vertex{" +
                "graph=" + graph +
                ", vertexId=" + vertexId +
                ", props=" + props +
                ", ts=" + ts +
                ", operation='" + operation + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        S2Vertex s2Vertex = (S2Vertex) o;
        return vertexId.equals(s2Vertex.getVertexId());
    }

    @Override
    public int hashCode() {
        int result = graph.hashCode();
        result = 31 * result + vertexId.hashCode();
        result = 31 * result + (int) (ts ^ (ts >>> 32));
        result = 31 * result + operation.hashCode();
        return result;
    }

    public ServiceColumn column() {
        return vertexId.getColumn();
    }

    public org.apache.s2graph.core.Vertex toInnerVertex() {
        InnerValLike innerVal = JSONParser.toInnerVal(getVertexId().getId(),
                column().columnType(),
                column().schemaVersion());
        Byte op = (Byte) GraphUtil.toOp(operation).get();
        VertexId vId = new VertexId(vertexId.columnId(), innerVal);
        Map<String, Object> innerProps = new HashMap<>();

        for (Map.Entry<String, VertexProperty<?>> e: props.entrySet()) {
            if (column().metasInvMap().contains(e.getKey())) {
                innerProps.put(e.getKey(), props.get(e.getKey()).value());
            }
        }

        return new org.apache.s2graph.core.Vertex(vId,
                ts,
                org.apache.s2graph.core.Vertex.toInnerProperties(column(), innerProps, ts),
                op,
                org.apache.s2graph.core.Vertex.apply$default$5());
    }
}
