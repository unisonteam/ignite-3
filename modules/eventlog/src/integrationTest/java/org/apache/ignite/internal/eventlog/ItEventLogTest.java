package org.apache.ignite.internal.eventlog;

import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.eventlog.configuration.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.configuration.sink.FileSinkChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ItEventLogTest extends ClusterPerTestIntegrationTest {

    private static final String TEST_CHANNEL = "test-channel";
    public static final String TEST_SINK = "test-sink";
    public static final String EVENT_LOG_FILENAME = "test-event.log";

    private HttpClient client;

    EventLogConfiguration eventLogConfiguration;

    SecurityConfiguration securityConfiguration;

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        client = HttpClient.newHttpClient();
        IgniteImpl ignite = cluster.aliveNode();
        eventLogConfiguration = ignite.clusterConfiguration().getConfiguration(EventLogConfiguration.KEY);
        securityConfiguration = ignite.clusterConfiguration().getConfiguration(SecurityConfiguration.KEY);
    }

    @Test
    void enableDisableChannel() throws Exception {
        // Given Channel configured.
        eventLogConfiguration.channels().change(
                c -> c.create(TEST_CHANNEL, channelChange -> channelChange.changeEnabled(false))
        ).get();
        // And File Sink configured with the Channel.
        eventLogConfiguration.sinks().change(
                s -> s.create(TEST_SINK, sinkChange -> sinkChange.convert(FileSinkChange.class)
                        .changePattern(EVENT_LOG_FILENAME)
                        .changeChannels(TEST_CHANNEL))
        ).get();

        // Then Event log file should not be created because the channel is disabled.
        assertThat(workDir.resolve(EVENT_LOG_FILENAME).toFile().exists(), equalTo(false));

        // When Channel is enabled.
        eventLogConfiguration.channels().change(
                c -> c.update(TEST_CHANNEL, channelChange -> channelChange.changeEnabled(true))
        ).get();

        // Then Event log file should not be created because there were not events fired yet.
        assertThat(workDir.resolve(EVENT_LOG_FILENAME).toFile().exists(), equalTo(false));

        // When AUTHENTICATION Event is fired.
        securityConfiguration.enabled().update(true).get();
        readAnyConfigValue("ignite", "ignite");

        // Then Event log file should be created.
        await().untilAsserted(
                () -> assertThat(workDir.resolve(EVENT_LOG_FILENAME).toFile().exists(), equalTo(true))
        );
    }

    private void readAnyConfigValue(String username, String password) {
        URI updateClusterConfigUri = URI.create("http://localhost:10300/management/v1/configuration/cluster/");
        HttpRequest updateClusterConfigRequest = HttpRequest.newBuilder(updateClusterConfigUri)
                .header("content-type", "text/plain")
                .header("Authorization", basicAuthenticationHeader(username, password))
                .method("GET", BodyPublishers.noBody())
                .build();

        assertThat(sendRequest(client, updateClusterConfigRequest), hasStatusCode(200));
    }

    private static String basicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }


    private static HttpResponse<String> sendRequest(HttpClient client, HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
