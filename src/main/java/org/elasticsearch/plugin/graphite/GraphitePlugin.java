package org.elasticsearch.plugin.graphite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.service.graphite.GraphiteService;

public class GraphitePlugin extends Plugin {
    public String name() {
        return "graphite";
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(GraphiteService.EVERY_SETTING, GraphiteService.EXCLUDE,
                GraphiteService.HOST_SETTING, GraphiteService.INCLUDE, GraphiteService.PER_INDEX,
                GraphiteService.PORT_SETTING, GraphiteService.PREFIX, GraphiteService.INCLUDE_INDEXES);
    }

    public String description() {
        return "Graphite Monitoring Plugin";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(GraphiteService.class);
        return services;
    }

}
