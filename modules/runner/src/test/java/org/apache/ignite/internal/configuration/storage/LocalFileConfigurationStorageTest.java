/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration.storage;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link LocalFileConfigurationStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class LocalFileConfigurationStorageTest {

    private static final String CONFIG_NAME = "ignite-config.conf";

    @WorkDirectory
    private Path tmpDir;

    /**
     * Hocon root configuration schema.
     */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class RootConfigurationSchema {
        @NamedConfigValue
        public NodeAttrConfigurationSchema attributes;
    }

    /**
     * Configuration schema for testing the support of primitives.
     */
    @Config
    public static class NodeAttrConfigurationSchema {
        /** Name of the node attribute. */
        @InjectedName
        public String name;

        /** Node attribute field. */
        @Value(hasDefault = true)
        public String attribute = "";
    }

    protected ConfigurationStorage storage;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp() {
        storage = getStorage();

        storage.registerConfigurationListener(new ConfigurationStorageListener() {
            @Override
            public CompletableFuture<Void> onEntriesChanged(Data changedEntries) {
                return completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> onRevisionUpdated(long newRevision) {
                return completedFuture(null);
            }
        });
    }

    public ConfigurationStorage getStorage() {
        return new LocalFileConfigurationStorage(getConfigFile(), List.of(RootConfiguration.KEY));
    }

    @Test
    void testReadWrite() {
        var validHocon = "root.attributes.node1={attribute=attr1}";


    }

    @Test
    @Disabled
    void testHocon() throws IOException {
        // All of this is needed because write expects serializable values and only concrete classes are serializable
        HashMap<String, ArrayList<String>> map = new HashMap<>(Map.of("list", new ArrayList<>(List.of("val1", "val2"))));
        var data = Map.of("foo1", "bar1", "foo2", "bar2", "map", map);

        assertThat(storage.write(data, 0), willBe(true));

        String contents = Files.readString(getConfigFile());

        // \n instead of System.lineSeparator because Config library writes \n only
        assertThat(contents, is("foo1=bar1\n"
                + "foo2=bar2\n"
                + "map {\n"
                + "    list=[\n"
                + "        val1,\n"
                + "        val2\n"
                + "    ]\n"
                + "}\n"));
    }

    /**
     * Tests the {@link ConfigurationStorage#readAllLatest} method.
     */
    @Test
    @Disabled
    public void testReadAllLatest() {
        var data = Map.of("foo1", "bar1", "foo2", "bar2");

        assertThat(storage.write(data, 0), willBe(equalTo(true)));

        // test that reading without a prefix retrieves all data
        CompletableFuture<Map<String, ? extends Serializable>> latestData = storage.readAllLatest("");

        assertThat(latestData, willBe(equalTo(data)));

        // test that reading with a common prefix retrieves all data
        latestData = storage.readAllLatest("foo");

        assertThat(latestData, willBe(equalTo(data)));

        // test that reading with a specific prefix retrieves corresponding data
        latestData = storage.readAllLatest("foo1");

        assertThat(latestData, willBe(equalTo(Map.of("foo1", "bar1"))));

        // test that reading with a nonexistent prefix retrieves no data
        latestData = storage.readAllLatest("baz");

        assertThat(latestData, willBe(anEmptyMap()));
    }

    @Test
    @Disabled
    void testMergeHocon() throws IOException {
        var data = Map.of("foo1", "bar");
        assertThat(storage.write(data, 0), willBe(true));

        var append = Map.of("foo1", "baz", "foo2", "bar");
        assertThat(storage.write(append, 1), willBe(true));

        String contents = Files.readString(getConfigFile());
        assertThat(contents, is("foo1=baz\n"
                + "foo2=bar\n"));
    }

    private Path getConfigFile() {
        return tmpDir.resolve(CONFIG_NAME);
    }
}
