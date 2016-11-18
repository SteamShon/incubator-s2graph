package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.s2graph.core.GraphUtil;
import org.apache.s2graph.core.JSONParser;
import org.apache.s2graph.core.mysqls.Label;
import org.apache.s2graph.core.mysqls.LabelMeta;
import org.apache.s2graph.core.types.InnerValLikeWithTs;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import scala.collection.JavaConversions;
import scalikejdbc.AutoSession$;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class S2Edge implements Edge {

    private S2Graph graph;
    private S2Vertex srcV;
    private S2Vertex tgtV;
    private String label;
    private String direction;
    private Map<String, Property<?>> props;
    private Long ts;
    private String operation;
    private Label innerLabel;
    private Map<String, LabelMeta> labelMetas;

    public S2Edge(S2Graph graph, org.apache.s2graph.core.Edge edge) {
        this(graph,
                new S2Vertex(graph, edge.srcForVertex()),
                new S2Vertex(graph, edge.tgtForVertex()),
                edge.labelName());

        this.innerLabel = Label.findByName(this.label, true, AutoSession$.MODULE$).get();
        this.direction = edge.direction();
        this.ts = edge.ts();
        this.operation = edge.operation();
        for (Map.Entry<LabelMeta, InnerValLikeWithTs> e : JavaConversions.mapAsJavaMap(edge.propsWithTs()).entrySet()) {
            property(e.getKey().name(), JSONParser.innerValToAny(e.getValue().innerVal(), e.getKey().dataType()));
        }
        for (Map.Entry<String, LabelMeta> e : JavaConversions.mapAsJavaMap(edge.label().metaPropsInvMap()).entrySet()) {
            labelMetas.put(e.getKey(), e.getValue());
        }
    }

    public S2Edge(S2Graph graph,
                  S2Vertex srcV,
                  S2Vertex tgtV,
                  String label) {
        this.graph = graph;
        this.srcV = srcV;
        this.tgtV = tgtV;
        this.label = label;
        this.direction = "out";
        this.ts = System.currentTimeMillis();
        this.operation = GraphUtil.defaultOp();
        this.props = new HashMap<>();
        this.labelMetas = new HashMap<>();
        this.innerLabel = Label.findByName(this.label, true, AutoSession$.MODULE$).get();
        for (Map.Entry<String, LabelMeta> e: JavaConversions.mapAsJavaMap(this.innerLabel.metaPropsInvMap()).entrySet()) {
            labelMetas.put(e.getKey(), e.getValue());
        }
    }

    public S2Edge(S2Graph graph,
                  S2Vertex srcV,
                  S2Vertex tgtV,
                  String label,
                  String direction,
                  Map<String, Object> props,
                  Long ts,
                  String operation) {
        this.graph = graph;
        this.srcV = srcV;
        this.tgtV = tgtV;
        this.label = label;
        this.direction = direction;
        this.ts = ts;
        this.operation = operation;
        this.props = new HashMap<>();
        this.labelMetas = new HashMap<>();

        this.innerLabel = Label.findByName(this.label, true, AutoSession$.MODULE$).get();
        for (Map.Entry<String, LabelMeta> e: JavaConversions.mapAsJavaMap(this.innerLabel.metaPropsInvMap()).entrySet()) {
            labelMetas.put(e.getKey(), e.getValue());
        }

        props.entrySet().stream().filter(e -> labelMetas.containsKey(e.getKey())).forEach(e -> {
            LabelMeta meta = labelMetas.get(e.getKey());
            property(e.getKey(), JSONParser.toInnerVal(e.getValue(), meta.dataType(), this.innerLabel.schemaVersion()));
        });
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return null;
    }

    @Override
    public Object id() {
        return srcV.getVertexId() + "|" + tgtV.getVertexId() + "|" + label() + "|" + direction();
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <V> Property<V> property(String key) {
        if (props.containsKey(key)) return (Property<V>) props.get(key);
        else {
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Property<V> newProperty = new S2Property<>(this, key, value);
        props.put(key, newProperty);
        return newProperty;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... strings) {
        return null;
    }

    public S2Vertex srcV() {
        return srcV;
    }

    public S2Vertex tgtV() {
        return tgtV;
    }

    public String direction() {
        return direction;
    }

    public String operation() {
        return operation;
    }

    public Long ts() {
        return ts;
    }

    public Map<String, Property<?>> getProps() {
        return props;
    }

    @Override
    public String toString() {
        return "S2Edge{" +
                "graph=" + graph +
                ", srcV=" + srcV +
                ", tgtV=" + tgtV +
                ", label='" + label + '\'' +
                ", direction='" + direction + '\'' +
                ", props=" + props +
                ", ts=" + ts +
                ", operation='" + operation + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        S2Edge s2Edge = (S2Edge) o;

        if (!graph.equals(s2Edge.graph)) return false;
        if (!srcV.equals(s2Edge.srcV)) return false;
        if (!tgtV.equals(s2Edge.tgtV)) return false;
        if (!label.equals(s2Edge.label)) return false;
        if (!direction.equals(s2Edge.direction)) return false;

        return ElementHelper.areEqual(this, s2Edge);
    }

    @Override
    public int hashCode() {
        int result = graph.hashCode();
        result = 31 * result + srcV.hashCode();
        result = 31 * result + tgtV.hashCode();
        result = 31 * result + label.hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + ts.hashCode();
        return result;
    }
}
