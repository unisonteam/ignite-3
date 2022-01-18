package org.apache.ignite.rest;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.rest.presentation.ConfigurationPresentationTest;

/**
 * Test root configuration schema.
 */
@ConfigurationRoot(rootName = "root")
public class TestRootConfigurationSchema {
    /** Foo field. */
    @Value(hasDefault = true)
    public String foo = "foo";
    
    /** Sub configuration schema. */
    @ConfigValue
    public ConfigurationPresentationTest.TestSubConfigurationSchema subCfg;
}
