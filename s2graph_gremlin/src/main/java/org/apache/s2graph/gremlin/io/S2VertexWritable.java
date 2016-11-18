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

package org.apache.s2graph.gremlin.io;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class S2VertexWritable implements WritableComparable<S2VertexWritable> {
    int columnId;
    int sort = 0;
    Object id;

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, columnId);
        WritableUtils.writeVInt(out, sort);
        WritableUtils.writeCompressedByteArray(out, ObjectUtils.anyToBytes(id));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        columnId = WritableUtils.readVInt(in);
        sort = WritableUtils.readVInt(in);
        id = ObjectUtils.bytesToAny(WritableUtils.readCompressedByteArray(in));
    }

    @Override
    public int hashCode() {
        int result = columnId;
        result = 31 * result + ObjectUtils.objectHashCode(id);
        return result;
    }

    public int getColumnId() {
        return columnId;
    }

    public S2VertexWritable setColumnId(int columnId) {
        this.columnId = columnId;
        return this;
    }

    public Object getId() {
        return id;
    }

    public S2VertexWritable setId(Object id) {
        this.id = id;
        return this;
    }

    public S2VertexWritable setSort(int sort) {
        this.sort = sort;
        return this;
    }

    public int getSort(){
        return sort;
    }

    @Override
    public String toString() {
        return "S2VertexWritable{" + "columnId=" + columnId + ", id=" + id + '}';
    }

    @Override
    public int compareTo(S2VertexWritable o) {
        int comp = columnId - o.columnId;
        if (comp != 0) {
            return comp;
        }
        comp = ObjectUtils.objectCompare(id, o.id);
        if(comp != 0) {
            return comp;
        }
        return sort - o.sort;
    }

    public S2VertexWritable copy(){
        return new S2VertexWritable().setColumnId(columnId).setId(id).setSort(sort);
    }
}
