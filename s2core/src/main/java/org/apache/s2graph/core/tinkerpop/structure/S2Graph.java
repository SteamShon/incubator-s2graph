package org.apache.s2graph.core.tinkerpop.structure;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class S2Graph implements Graph {
    private Configuration configuration;
    private Config config;
    private ExecutionContext ec;
    private org.apache.s2graph.core.Graph g;

    public S2Graph(Configuration configuration, ExecutionContext ec) {
        this.configuration = configuration;
        this.config = toTypeSafeConfig(configuration);
        this.ec = ec;
        this.g = new org.apache.s2graph.core.Graph(config, ec);
    }

    public S2Graph(Config config, ExecutionContext ec) {
        this.config = config;
        this.ec = ec;
        this.g = new org.apache.s2graph.core.Graph(config, ec);
    }
    public Config toTypeSafeConfig(Configuration configuration) {
        Config config = ConfigFactory.load();
        Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = configuration.getProperty(key);
            Map<String, Object> current = new HashMap<>();
            current.put(key, value);
            config = config.withFallback(ConfigFactory.parseMap(current));
        }
        return config;
    }


    @Override
    public Vertex addVertex(Object... keyValues) {
        S2Vertex s2Vertex = new S2Vertex(this, keyValues);
        org.apache.s2graph.core.Vertex innerV = org.apache.s2graph.core.Vertex.fromS2Vertex(s2Vertex);
        try {
            boolean success = (boolean) Await.result(g.mutateVertex(innerV, true), Duration.apply(10, TimeUnit.SECONDS));
            if (success) return s2Vertex;
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return null;
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }


    public org.apache.s2graph.core.Graph getG() {
        return g;
    }

    public Config getConfig() {
        return config;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }
}
