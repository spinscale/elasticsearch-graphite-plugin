package org.elasticsearch.plugin.graphite;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.service.graphite.GraphiteService;

import java.util.Collection;

public class GraphitePlugin extends AbstractPlugin {

    public String name() {
        return "graphite";
    }

    public String description() {
        return "Graphite Monitoring Plugin";
    }

    @SuppressWarnings("rawtypes")
    @Override public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(GraphiteService.class);
        return services;
    }

}
