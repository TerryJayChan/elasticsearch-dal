package org.terry.elasticsearch.query.client;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashSet;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.*;

@Slf4j
public class JestClientFactoryBean implements FactoryBean<JestClient>, InitializingBean, DisposableBean {

    private JestClient client;

    private String esUrls;
    private int maxTotalConnection = 100;
    private int defaultMaxTotalConnectionPerRoute = 50;
    private int readTimeout = 5000;
    private Boolean multiThreaded = true;

    static final String COMMA = ",";

    @Override
    public void destroy() throws Exception {
        try {
            log.info("Closing elasticSearch jest client");
            if (client != null) {
                client.shutdownClient();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch jest client: ", e);
        }
    }

    @Override
    public JestClient getObject() throws Exception {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return JestClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Set<String> urls = bulidESUrl();

        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder(urls)
                .readTimeout(readTimeout)
                .maxTotalConnection(maxTotalConnection)
                .defaultMaxTotalConnectionPerRoute(defaultMaxTotalConnectionPerRoute)
                .multiThreaded(true);
        factory.setHttpClientConfig(builder.build());
        client = factory.getObject();
    }

    public String getEsUrls() {
        return esUrls;
    }

    public void setEsUrls(String esUrls) {
        this.esUrls = esUrls;
    }

    public int getMaxTotalConnection() {
        return maxTotalConnection;
    }

    public void setMaxTotalConnection(int maxTotalConnection) {
        this.maxTotalConnection = maxTotalConnection;
    }

    public int getDefaultMaxTotalConnectionPerRoute() {
        return defaultMaxTotalConnectionPerRoute;
    }

    public void setDefaultMaxTotalConnectionPerRoute(int defaultMaxTotalConnectionPerRoute) {
        this.defaultMaxTotalConnectionPerRoute = defaultMaxTotalConnectionPerRoute;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    private Set<String> bulidESUrl() {
        Set<String> urls = new HashSet<>();
        if (isBlank(esUrls)) {
            esUrls = "http://127.0.0.1:9200";
        }
        for (String url : split(esUrls)) {
            if (isNotBlank(url)) {
                urls.add(url);
            }
            log.info("adding transport node : " + url);
        }
        return urls;
    }
}
