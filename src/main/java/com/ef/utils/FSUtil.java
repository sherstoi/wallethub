package com.ef.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.*;

public class FSUtil {

    public static int countRowInFile(String filePath) throws IOException {
        try (InputStream is =  new BufferedInputStream(new FileInputStream(filePath))) {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        }
    }

    public static String readPropertyValue(String propertyName) {
        String propertyValue;
        try {
            Configuration configuration = new PropertiesConfiguration("Application.properties");
            propertyValue = configuration.getString(propertyName);
        } catch (ConfigurationException c) {
            throw new RuntimeException(c.getCause());
        }

        return propertyValue;
    }
}