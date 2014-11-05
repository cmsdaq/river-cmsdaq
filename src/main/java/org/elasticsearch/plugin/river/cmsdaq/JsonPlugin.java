package org.elasticsearch.plugin.river.cmsdaq;
import org.elasticsearch.river.cmsdaq.SwitchesModule;
import org.elasticsearch.river.cmsdaq.XdaqLasModule;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;


/**
 *
 */
public class JsonPlugin extends AbstractPlugin {

    @Inject
    public JsonPlugin() {
    }

    @Override
    public String name() {
        return "river-cmsdaq";
    }

    @Override
    public String description() {
        return "Inject JSON data from CMS DAQ";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("xdaqlas", XdaqLasModule.class);
        module.registerRiver("switches", SwitchesModule.class);
    }
}
