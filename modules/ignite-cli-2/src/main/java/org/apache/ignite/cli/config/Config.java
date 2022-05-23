package org.apache.ignite.cli.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * CLI default configuration.
 */
public class Config {
    private static final String XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    private static final String DEFAULT_ROOT = ".config";
    private static final String PARENT_FOLDER_NAME = "ignitecli";
    private static final String CONFIG_FILE_NAME = "defaults";

    private final Properties props;

    public Config(Properties props) {
        this.props = props;
    }

    public Config() {
        this(loadConfig());
    }

    public Properties getProperties() {
        return props;
    }

    public String getProperty(String key) {
        return props.getProperty(key);
    }

    public void setProperty(String key, String value) {
        props.setProperty(key, value);
    }

    private static Properties loadConfig() {
        File configFile = getConfigFile();
        if (configFile.canRead()) {
            try (InputStream is = new FileInputStream(configFile)) {
                Properties p = new Properties();
                p.load(is);
                return p;
            } catch (IOException e) {
                // todo report error?
            }
        }
        return new Properties();
    }

    public void saveConfig() {
        File configFile = getConfigFile();
        if (configFile.getParentFile().mkdirs()) {
            try (OutputStream os = new FileOutputStream(configFile)) {
                props.store(os, null);
            } catch (IOException e) {
                // todo report error?
            }
        }
    }

    public String printConfig() {
        StringBuilder builder = new StringBuilder();
        for (Iterator<Entry<Object, Object>> iterator = props.entrySet().iterator(); iterator.hasNext(); ) {
            Entry<Object, Object> entry = iterator.next();
            builder.append(entry.getKey()).append("=").append(entry.getValue());
            if (iterator.hasNext()) {
                builder.append(System.lineSeparator());
            }
        }
        return builder.toString();
    }

    private static File getConfigFile() {
        String xdgConfigHome = System.getenv(XDG_CONFIG_HOME);
        File root = xdgConfigHome != null ? new File(xdgConfigHome) : new File(System.getProperty("user.home"), DEFAULT_ROOT);
        File parent = new File(root, PARENT_FOLDER_NAME);
        return new File(parent, CONFIG_FILE_NAME);
    }
}
