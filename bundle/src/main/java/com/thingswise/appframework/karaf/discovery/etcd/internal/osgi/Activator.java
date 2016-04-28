package com.thingswise.appframework.karaf.discovery.etcd.internal.osgi;

import java.util.Hashtable;

import org.apache.karaf.util.tracker.BaseActivator;
import org.apache.karaf.util.tracker.annotation.ProvideService;
import org.apache.karaf.util.tracker.annotation.Services;
import org.osgi.framework.Constants;
import org.osgi.service.cm.ManagedServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thingswise.appframework.karaf.discovery.etcd.EtcdDiscoveryServiceFactory;

@Services(
        provides = {
                @ProvideService(ManagedServiceFactory.class)
        }
)
public class Activator extends BaseActivator {

    private static Logger logger = LoggerFactory.getLogger(Activator.class);

    @Override
    public void doStart() throws Exception {
        logger.debug("TW Cellar Etcd: init discovery service factory");
        Hashtable props = new Hashtable();
        props.put(Constants.SERVICE_PID, "com.thingswise.appframework.karaf.discovery.etcd");
        EtcdDiscoveryServiceFactory factory = new EtcdDiscoveryServiceFactory(bundleContext);
        register(ManagedServiceFactory.class, factory, props);
    }

    @Override
    public void doStop() {
        super.doStop();
    }

}

