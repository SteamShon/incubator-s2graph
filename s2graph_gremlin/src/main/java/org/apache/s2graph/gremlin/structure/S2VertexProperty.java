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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class S2VertexProperty<V> implements VertexProperty<V> {
    private S2Vertex element;
    private String key;
    private V value;
    private Map<String, Property<?>> props;

    public S2VertexProperty(S2Vertex element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.props = new HashMap<>();
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return element;
    }

    @Override
    public void remove() {

    }

    @Override
    public Object id() {
        S2VertexId vertexId = element.getVertexId();
        return vertexId.getServiceName() + "|" +
                vertexId.getColumnName() + "|" +
                vertexId.getId() + "|" + key;
    }

    @Override
    public <V> Property<V> property(String s, V v) {
        VertexProperty<V> newProperty = new S2VertexProperty<V>(element, key, v);
        props.put(key, newProperty);
        return newProperty;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        List<Property<U>> ls = new ArrayList<>();
        for (String propertyKey : propertyKeys) {
            ls.add(property(propertyKey));
        }
        return ls.iterator();
    }
}
