package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.s2graph.core.mysqls.Service;
import org.apache.s2graph.core.mysqls.ServiceColumn;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;

import java.util.Map;

public class S2GraphUtil {
    public static Pair<S2VertexId, Map<String, Object>> toS2VertexParam(Object... keyValues) {
        Map<String, Object> params = ElementHelper.asMap(keyValues);

        // TODO: error check and validataion. extract constant string into common.
        Service service = (Service) params.get("service");
        ServiceColumn column = (ServiceColumn) params.get("column");
        Object id = params.get("id");



        S2VertexId s2VertexId = new S2VertexId(service, column, id);

        System.out.println(s2VertexId.toString());

        params.remove("service");
        params.remove("column");
        params.remove("id");

        return new Pair(s2VertexId, params);
    }
//    public static List<S2VertexId> toVertexIds(Object... vertexIds) {
//        List<S2VertexId> s2VertexIds = new ArrayList<>();
//        List<Object> others = new ArrayList<>();
//        // first partition input parameter based on type of each element in vertexIds.
//        for (Object vertexId : vertexIds) {
//            if (vertexId instanceof S2VertexId) {
//                s2VertexIds.add((S2VertexId) vertexId);
//            } else {
//                others.add(vertexId);
//            }
//        }
//        for (int i = 0; i < others.size(); i += 6) {
//            String serviceNameValue = "";
//            String columnNameValue = "";
//            Object idValue = null;
//
//            for (int j = 0; j+1 < 6; j+=2) {
//                String inputName = others.get(j).toString().toLowerCase();
//                Object inputValue = others.get(j+1);
//
//                if (inputName == "servicename") {
//                    serviceNameValue = (String) inputValue;
//                } else if (inputName == "columnname") {
//                    columnNameValue = (String) inputValue;
//                } else if (inputName == "id") {
//                    idValue = inputValue;
//                } else {
//                    throw new RuntimeException("wrong input type sequence in given vertexIds.");
//                }
//            }
//            s2VertexIds.add(new S2VertexId(serviceNameValue, columnNameValue, idValue));
//        }
//        return s2VertexIds;
//    }
//    public static S2VertexId toS2VertexId(ServiceColumn serviceColumn, Object idValue) {
//        return new S2VertexId(serviceColumn.service().serviceName(), serviceColumn.columnName(), idValue);
//    }
}


