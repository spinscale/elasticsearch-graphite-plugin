package org.elasticsearch.plugin.graphite;

import com.google.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.service.graphite.GraphiteService;

import java.util.Collection;

public class GraphitePlugin extends Plugin {

    public String name() {
        return "graphite";
    }

    public String description() {
        return "Graphite Monitoring Plugin";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(GraphiteService.class);
        return services;
    }

}
