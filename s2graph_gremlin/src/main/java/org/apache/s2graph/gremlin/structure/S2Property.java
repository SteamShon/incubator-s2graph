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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

public class S2Property<V> implements Property<V> {
    private Element element;
    private String key;
    private V value;

    public S2Property(Element element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {

    }


}
