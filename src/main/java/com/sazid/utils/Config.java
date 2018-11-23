package com.sazid.utils;

import java.io.*;
import java.util.Properties;

public class Config {
    private static Config instance;
    private Properties config;

    private Config() {
        loadDefaultConfig();
    }

    public static Config getConfig() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public void setConfig(Properties config) {
        this.config = config;
    }

    public Object put(String key, Object value) {
        return this.config.put(key, value);
    }

    public Object get(String key) {
        return this.config.get(key);
    }

    public void loadDefaultConfig() {
        String propFileName = "config.properties";
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {

            if (inputStream != null) {
                Properties prop = new Properties();
                prop.load(inputStream);
                this.setConfig(prop);
            } else {
                throw new FileNotFoundException("Default property file '" + propFileName + "' not found in the classpath");
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }

    public void loadConfigFromFile(File file) {
        if (file == null) {
            System.out.println("Given file cannot be loaded. Loading the default.");
            loadDefaultConfig();
            return;
        }
        try (InputStream inputStream = new FileInputStream(file)) {
            Properties prop = new Properties();
            prop.load(inputStream);
            this.setConfig(prop);

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }
}
