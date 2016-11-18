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

import java.io.*;

public class ObjectUtils {
    public static int objectCompare(Object o1, Object o2) {
        Class<?> cls1 = o1.getClass();
        Class<?> cls2 = o2.getClass();

        if (cls1 == Integer.class && cls2 == Integer.class) {
            return Integer.compare((Integer) o1, (Integer) o2);
        } else if (cls1== Double.class && cls2 == Double.class) {
            return Double.compare((Double) o1, (Double) o2);
        } else if (cls1 == Long.class && cls2 == Long.class) {
            return Long.compare((Long) o1, (Long) o2);
        }
        return o1.toString().compareTo(o2.toString());
    }

    public static int objectHashCode(Object o){
        Class<?> cls = o.getClass();
        if(cls == Integer.class){
            return Integer.hashCode((Integer) o);
        } else if(cls == Double.class){
            return  Double.hashCode((Double) o);
        } else if(cls == Long.class) {
            return Long.hashCode((Long) o);
        }
        return o.toString().hashCode();
    }

    public static <T> T bytesToAny(byte[] bytes) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bis);
        try {
            return (T) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            in.close();
            bis.close();
        }
        return null;
    }

    public static <T> byte[] anyToBytes(T obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        byte[] ret = bos.toByteArray();
        out.close();
        bos.close();
        return ret;
    }

}
