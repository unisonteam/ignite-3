package org.apache.ignite.cli.call.configuration;

public class JsonString {
    private final String value;

    private JsonString(String value) {
        this.value = value;
    }

    public static JsonString fromString(String jsonString) {
        return new JsonString(jsonString);
    }

    public String getValue() {
        return value;
    }
}
