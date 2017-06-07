package org.terry.elasticsearch.query.client;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetAddress;

import static org.apache.commons.lang3.StringUtils.*;

@Slf4j
public class TransportClientFactoryBean implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {

    private TransportClient client;
    private String clusterName;
    private String clusterNodes;

    static final String COLON = ":";
    static final String COMMA = ",";

    @Override
    public void destroy() throws Exception {
        try {
            log.info("Closing ElasticSearch transport client");
            if (client != null) {
                client.close();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch transport client: ", e);
        }
    }

    @Override
    public TransportClient getObject() throws Exception {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return TransportClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        client = new PreBuiltTransportClient(settings());

        if (isBlank(clusterNodes)) {
            clusterNodes = "127.0.0.1:9300";
        }
        for (String clusterNode : split(clusterNodes, COMMA)) {
            String hostName = substringBeforeLast(clusterNode, COLON);
            if (isBlank(hostName)) {
                hostName = "127.0.0.1";
            }
            String port = substringAfterLast(clusterNode, COLON);
            if (isBlank(port)) {
                port = "9300";
            }

            log.info("adding transport node : " + clusterNode);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName),
                    Integer.parseInt(port)));
        }
        client.connectedNodes();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    private Settings settings() {
        return Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false)
                .build();
    }

}
