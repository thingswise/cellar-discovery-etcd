package com.thingswise.appframework.karaf.discovery.etcd;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.karaf.cellar.core.discovery.DiscoveryService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.requests.EtcdRequest;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import mousio.etcd4j.responses.EtcdKeysResponse.EtcdNode;

@Component(	
	configurationPid = { "com.thingswise.appframework.karaf.discovery.etcd" },
	configurationPolicy = ConfigurationPolicy.OPTIONAL,
	immediate = true)
public class EtcdDiscoveryService implements DiscoveryService {
	
    private final String ETCD_URL = "etcdUrl";
    private final String DNS_DIRECTORY = "dnsDirectory";	
	
	private static Logger logger = LoggerFactory.getLogger(EtcdDiscoveryService.class);	
	
	private String etcdUrl;
	private EtcdClient client;
	private String keyDir;
	
	@Activate
	public void activate(final Map<String, ?> properties) {
		logger.info("Activating EtcdDiscoveryService...");
		
		update(properties);				
	}

	@Deactivate
	public void deactivate() {
		logger.info("Deactivating EtcdDiscoveryService...");
	}
	
	@Modified
	public void modified(final Map<String, ?> properties) {
		logger.info("Updating EtcdDiscoveryService...");
		
		update(properties);
	}

	public void update(Map<String, ?> properties) {
		
        String etcdUrl = (String) properties.get(ETCD_URL);
        if (etcdUrl == null) {
            etcdUrl = getEnvOrDefault("ETCD_URL", "http://172.17.42.1:4001");
        }
        String dnsDir = (String) properties.get(DNS_DIRECTORY);
        if (dnsDir == null) {
            dnsDir = getEnvOrDefault("DNS_DIRECTORY", "/skydns/local/skydns/appframework");
        }
		
		this.etcdUrl = etcdUrl;
		this.client = new EtcdClient(URI.create(etcdUrl));	
		this.keyDir = dnsDir;
	}

	@Override
	public void signIn() {
		// do nothing
	}

	@Override
	public void refresh() {
		// do nothing
	}

	@Override
	public void signOut() {
		// do nothing
	}
	
	protected static <K, T extends EtcdRequest<K>> T configureRequest(T req) {
		return req;
	}
	
	private static ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
		@Override
		protected Gson initialValue() {
			return new Gson();
		}		
	};
	
	static Gson getGson() {
		return gson.get();
	}	

	@Override
	public Set<String> discoverMembers() {
		Set<String> result = new HashSet<String>();
		EtcdKeysResponse response;
		try {
			response = configureRequest(client.getDir(keyDir).consistent().recursive()).send().get();
		} catch (IOException e) {
			logger.error(String.format("Error contacting etcd server at %s (I/O error)", etcdUrl), e);
			return result;
		} catch (EtcdException e) {
			logger.error(String.format("Error contacting etcd server at %s", etcdUrl), e);
			return result;
		} catch (TimeoutException e) {
			logger.error(String.format("Etcd request to %s timed out", etcdUrl), e);
			return result;
		}
		if (response.node != null) {
			if (response.node.dir && response.node.nodes != null) {
				for (EtcdNode child : response.node.nodes) {
					if (!child.dir) {
						String dnsEntryStr = child.value;
						if (dnsEntryStr != null) {
							Map<String, Object> dnsEntry = 
									getGson().fromJson(dnsEntryStr, new TypeToken<List<Map<String, Object>>>(){}.getType());
							Object host = dnsEntry.get("Host");
							if (host != null && host instanceof String) {
								result.add((String)host);
							}
						}
					}
				}
			}
		}
		return result;
	}

	private static String getEnvOrDefault(String var, String def) {
        final String val = System.getenv(var);
        return val == null ? def : val;
    }

}
