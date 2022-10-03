package org.apache.ignite.internal.app;

import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;

/** Default value provider which gets values from environment. */
public class EnvironmentDefaultValueProvider implements IDefaultValueProvider {

    private static final String ENV_NAME_PREFIX = "IGNITE_";

    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {
        if (argSpec.isOption()) {
            OptionSpec optionSpec = (OptionSpec) argSpec;
            String longestName = optionSpec.longestName();
            if (longestName.startsWith("--")) {
                return System.getenv(getEnvName(longestName));
            }
        }
        return null;
    }

    /**
     * Gets the name of the environment variable corresponding to the option name.
     *
     * @param optionName Option name.
     * @return Environment variable name.
     */
    public static String getEnvName(String optionName) {
        return ENV_NAME_PREFIX + optionName.substring("--".length()).replace('-', '_').toUpperCase();
    }
}
