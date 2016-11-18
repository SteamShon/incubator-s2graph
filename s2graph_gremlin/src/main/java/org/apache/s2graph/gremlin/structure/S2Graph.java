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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.collection.JavaConversions;
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

    public static Duration timeout = Duration.apply(60, TimeUnit.SECONDS);

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

        try {
            org.apache.s2graph.core.Vertex innerV = s2Vertex.toInnerVertex();

            boolean success = (boolean) Await.result(g.mutateVertex(innerV, true), timeout);
            if (success) return s2Vertex;
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... keyValues) {
        List<org.apache.s2graph.core.Vertex> vs = new ArrayList<>();
        List<Vertex> results = new ArrayList<>();

        for (int i = 0; i + 6 < keyValues.length + 1; i += 6) {
            List<Object> current = new ArrayList<>();
            for (int j = 0; j < 6; j++) {
                current.add(keyValues[i + j]);
            }
            org.apache.s2graph.core.Vertex innerV = (new S2Vertex(this, current.toArray())).toInnerVertex();
            vs.add(innerV);
        }
        try {
            for (org.apache.s2graph.core.Vertex v : JavaConversions.seqAsJavaList(Await.result(g.getVertices(vs), timeout))) {
                S2Vertex s2Vertex = new S2Vertex(this, v);
                results.add(s2Vertex);
            }
            return results.iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
