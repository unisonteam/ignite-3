

import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
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
                c -> c.create("test-channel", channelChange -> channelChange.changeEnabled(false))
        ).get();
        // And File Sink configured with the Channel.
        eventLogConfiguration.sinks().change(
                s -> s.create("test-sink", sinkChange -> sinkChange.convert(FileSinkChange.class)
                        .changePattern("test-event.log")
                        .changeChannels("test-channel"))
        ).get();

        // Then Event log file should not be created because the channel is disabled.
        assertThat(workDir.resolve("test-event.log").toFile().exists(), equalTo(false));


        // When Channel is enabled.
        eventLogConfiguration.channels().change(
                c -> c.update("test-channel", channelChange -> channelChange.changeEnabled(true))
        ).get();

        // Then Event log file should not be created because there were not events fired yet.
        assertThat(workDir.resolve("test-event.log").toFile().exists(), equalTo(false));

        // When AUTHENTICATION Event is fired.
        securityConfiguration.enabled().update(true).get();
        readAnyConfigValue("ignite", "ignite");
        Thread.sleep(10000);

        // Then Event log file should be created.
        assertThat(workDir.resolve("test-event.log").toFile().exists(), equalTo(true));
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
