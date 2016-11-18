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

package org.apache.s2graph.core.tinkerpop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class S2EdgeWritable implements WritableComparable<S2EdgeWritable> {
    S2VertexWritable srcVertex;
    S2VertexWritable tgtVertex;
    String labelName;
    int direction;
    long timestamp;
    Map<String, Object> propertyMap;

    public S2VertexWritable getSrcVertex() {
        return srcVertex;
    }

    public S2EdgeWritable setSrcVertex(S2VertexWritable srcVertex) {
        this.srcVertex = srcVertex;
        return this;
    }

    public S2VertexWritable getTgtVertex() {
        return tgtVertex;
    }

    public S2EdgeWritable setTgtVertex(S2VertexWritable tgtVertex) {
        this.tgtVertex = tgtVertex;
        return this;
    }

    public String getLabelName() {
        return labelName;
    }

    public S2EdgeWritable setLabelName(String labelName) {
        this.labelName = labelName;
        return this;
    }

    public int getDirection() {
        return direction;
    }

    public S2EdgeWritable setDirection(int direction) {
        this.direction = direction;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public S2EdgeWritable setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public S2EdgeWritable setPropertyMap(Map<String, Object> propertyMap) {
        this.propertyMap = propertyMap;
        return this;
    }


    public int getIdentityHash() {
        int result = srcVertex.hashCode();
        result = 31 * result + tgtVertex.hashCode();
        result = 31 * result + labelName.hashCode();
        result = 31 * result + direction;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        srcVertex.write(out);
        tgtVertex.write(out);
        WritableUtils.writeString(out, labelName);
        WritableUtils.writeVInt(out, direction);
        WritableUtils.writeVLong(out, timestamp);
        WritableUtils.writeCompressedByteArray(out, ObjectUtils.anyToBytes(propertyMap));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (srcVertex == null)
            srcVertex = new S2VertexWritable();
        srcVertex.readFields(in);
        if (tgtVertex == null)
            tgtVertex = new S2VertexWritable();
        tgtVertex.readFields(in);
        labelName = WritableUtils.readString(in);
        direction = WritableUtils.readVInt(in);
        timestamp = WritableUtils.readVLong(in);
        propertyMap =
                (Map<String, Object>) ObjectUtils.bytesToAny(WritableUtils.readCompressedByteArray(in));
    }

    @Override
    public int compareTo(S2EdgeWritable o) {
        int comp;
        if (!srcVertex.id.equals(o.srcVertex.id)) {
            comp = ObjectUtils.objectCompare(srcVertex.id, o.srcVertex.id);
            if (comp != 0)
                return comp;
        }
        if (!tgtVertex.id.equals(o.tgtVertex.id)) {
            comp = ObjectUtils.objectCompare(tgtVertex.id, o.tgtVertex.id);
            if (comp != 0)
                return comp;
        }
        comp = labelName.compareTo(o.labelName);
        if (comp != 0) {
            return comp;
        }
        comp = direction - o.direction;
        if (comp != 0) {
            return comp;
        }
        comp = Long.compare(timestamp, o.timestamp);
        if (comp != 0) {
            return comp;
        }

        return o.propertyMap.size() - propertyMap.size();
    }

    public S2EdgeWritable copy() {
        return new S2EdgeWritable().setDirection(direction).setLabelName(labelName)
                .setSrcVertex(srcVertex.copy()).setTgtVertex(tgtVertex.copy()).setTimestamp(timestamp)
                .setPropertyMap(propertyMap);
    }

    public boolean mergeSnapshot(S2EdgeWritable other) {
        if (getIdentityHash() != other.getIdentityHash()) {
            return false;
        }
        Map<String, Object> newObject = new HashMap<>();
        for (Map.Entry<String, Object> entry : other.propertyMap.entrySet()) {
            Object prevData = propertyMap.get(entry.getKey());
            if (prevData != null) {
                if (!prevData.equals(entry.getValue())) {
                    return false;
                }
            }
            newObject.put(entry.getKey(), entry.getValue());
        }
        propertyMap = newObject;
        return true;
    }

    @Override
    public String toString() {
        return "S2EdgeWritable{" + "srcVertex=" + srcVertex + ", tgtVertex=" + tgtVertex
                + ", labelName='" + labelName + '\'' + ", direction=" + direction + ", timestamp="
                + timestamp + ", propertyMap=" + propertyMap + '}';
    }
}
