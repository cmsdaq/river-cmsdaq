package org.elasticsearch.plugin.river.xdaqlas;
import org.elasticsearch.river.xdaqlas.XdaqLasModule;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;


/**
 *
 */
public class XdaqLasPlugin extends AbstractPlugin {

    @Inject
    public XdaqLasPlugin() {
    }

    @Override
    public String name() {
        return "river-xdaqlas";
    }

    @Override
    public String description() {
        return "Inject flashlists from XDAQ LAS";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("xdaqlas", XdaqLasModule.class);
    }
}
