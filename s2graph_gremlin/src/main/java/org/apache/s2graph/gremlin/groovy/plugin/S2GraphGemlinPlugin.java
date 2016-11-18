package org.apache.s2graph.gremlin.groovy.plugin;


import org.apache.s2graph.gremlin.structure.S2Graph;
import org.apache.tinkerpop.gremlin.groovy.plugin.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class S2GraphGemlinPlugin extends AbstractGremlinPlugin {
    public static Set<String> imports = new HashSet<>(Arrays.asList(AbstractGremlinPlugin.IMPORT_SPACE + S2Graph.class.getPackage().getName() + AbstractGremlinPlugin.DOT_STAR));

    @Override
    public String getName() {
        return "tinkerpop.s2graph";
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return null;
    }

    @Override
    public void afterPluginTo(PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }

    @Override
    public void pluginTo(PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        pluginAcceptor.addImports(S2GraphGemlinPlugin.imports);
    }
}
