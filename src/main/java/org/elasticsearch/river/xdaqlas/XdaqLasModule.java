package org.elasticsearch.river.xdaqlas;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class XdaqLasModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(XdaqLas.class).asEagerSingleton();
    }
}
