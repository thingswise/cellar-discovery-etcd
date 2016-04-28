package com.thingswise.appframework.karaf.discovery.etcd;

import java.util.Dictionary;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.karaf.cellar.core.discovery.DiscoveryService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdDiscoveryServiceFactory implements ManagedServiceFactory {
	
	private Logger logger = LoggerFactory.getLogger(EtcdDiscoveryService.class);

	private final Map<String, ServiceRegistration> registrations = new ConcurrentHashMap<String, ServiceRegistration>();

    private final BundleContext bundleContext;
    
    private final String ETCD_URL = "tw.appfwk.etcdUrl";
    private final String DNS_DIRECTORY = "tw.appfwk.dnsDirectory";

    public EtcdDiscoveryServiceFactory(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

	@Override
	public String getName() {
		return "Thingswise Etcd Discovery service for CELLAR";
	}

	@Override
	public void updated(String pid, Dictionary properties) throws ConfigurationException {
		ServiceRegistration newServiceRegistration = null;
        try {
            if (properties != null) {

                logger.info("TW Cellar Etcd: creating the discovery service ...");

                Properties serviceProperties = new Properties();
                for (Map.Entry entry : serviceProperties.entrySet()) {
                    Object key = entry.getKey();
                    Object value = entry.getValue();
                    serviceProperties.put(key, value);
                }

                String etcdUrl = (String) properties.get(ETCD_URL);
                if (etcdUrl == null) {
                    etcdUrl = getEnvOrDefault("ETCD_URL", "http://172.17.42.1:4001");
                }
                String dnsDir = (String) properties.get(DNS_DIRECTORY);
                if (dnsDir == null) {
                    dnsDir = getEnvOrDefault("DNS_DIRECTORY", "/skydns/local/skydns/appfwk");
                }

                EtcdDiscoveryService etcdDiscoveryService = new EtcdDiscoveryService(etcdUrl, dnsDir);

                newServiceRegistration = bundleContext.registerService(DiscoveryService.class.getName(), etcdDiscoveryService, (Dictionary) serviceProperties);
            }
        } finally {
            ServiceRegistration oldServiceRegistration = (newServiceRegistration == null) ? registrations.remove(pid) : registrations.put(pid, newServiceRegistration);
            if (oldServiceRegistration != null) {
                oldServiceRegistration.unregister();
            }
        }
	}

	@Override
	public void deleted(String pid) {
		logger.debug("TW Cellar Etcd: delete discovery service {}", pid);
        ServiceRegistration oldServiceRegistration = registrations.remove(pid);
        if (oldServiceRegistration != null) {
            oldServiceRegistration.unregister();
        }
	}
	
	private static String getEnvOrDefault(String var, String def) {
        final String val = System.getenv(var);
        return val == null ? def : val;
    }

}
