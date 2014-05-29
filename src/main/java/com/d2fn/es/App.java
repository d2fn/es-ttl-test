package com.d2fn.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static final String indexName = "ttl_index";
    private static final String typeName = "ttl_doc";

    private final JestClient client;

    public App(JestClient client, String ttl) throws Exception {
        this.client = client;
        JestResult createIndexResult = client.execute(new CreateIndex.Builder(indexName).build());
        if(!createIndexResult.isSucceeded()) {
            log.error("couldn't create index: " + createIndexResult.getErrorMessage());
        }
        PutMapping putMapping = new PutMapping.Builder(
                indexName,
                typeName,
                "{" +
                    "\"" + typeName + "\" : { " +
                        (ttl == null ? "" : "\"_ttl\" : {\"enabled\": true, \"default\": \"" + ttl + "\"},") +
                        "\"properties\" : { " +
                            "\"now\" : {\"type\" : \"long\", \"store\" : \"yes\"}" +
                        "}" +
                    "}" +
                "}"
        ).build();
        client.execute(putMapping);
    }

    public void run() {
        while(true) {
            long now = System.nanoTime();
            String source = "{\"now\": " + now + "}";
            Index index = new Index.Builder(source)
                    .index(indexName)
                    .type(typeName)
                    .id(String.valueOf(now))
                    .build();
            try {
                JestResult result = client.execute(index);
                if(!result.isSucceeded()) {
                    log.error("failed to add document at: " + now);
                    log.error("received error message: " + result.getErrorMessage());
                }
            }
            catch(Exception e) {
                log.error("error creating document at: " + now, e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("creating client");
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
        new App(client, "10m").run();
    }

    public static final Logger log = LoggerFactory.getLogger(App.class);
}
