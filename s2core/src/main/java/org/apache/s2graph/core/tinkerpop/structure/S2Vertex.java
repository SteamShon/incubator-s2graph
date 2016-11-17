package org.apache.s2graph.core.tinkerpop.structure;


import org.apache.s2graph.core.*;
import org.apache.s2graph.core.mysqls.ColumnMeta;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import scala.collection.JavaConversions;
import scala.concurrent.Await;

import java.util.*;
import java.util.stream.Collectors;

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
        return new S2Edge(graph, this, s2OutV, label, direction, props, ts, operation);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        List<Edge> results = new ArrayList<>();
        List<org.apache.s2graph.core.Vertex> vertices = new ArrayList<>();
        List<org.apache.s2graph.core.Step> steps = new ArrayList<>();
        List<org.apache.s2graph.core.QueryParam> queryParams = new ArrayList<>();

        // step 1. set up start vertices.
        vertices.add(org.apache.s2graph.core.Vertex.fromS2Vertex(this));

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

        try {
            final StepResult stepResult = Await.result(graph.getG().getEdges(query), S2Graph.timeout);
            results.addAll(JavaConversions.seqAsJavaList(stepResult.edgeWithScores())
                    .stream().map(edgeWithScore -> new S2Edge(graph, edgeWithScore.edge()))
                    .collect(Collectors.toList()));
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
        return ElementHelper.areEqual(this, s2Vertex);
    }

    @Override
    public int hashCode() {
        int result = graph.hashCode();
        result = 31 * result + vertexId.hashCode();
        result = 31 * result + props.hashCode();
        result = 31 * result + (int) (ts ^ (ts >>> 32));
        result = 31 * result + operation.hashCode();
        return result;
    }
}
