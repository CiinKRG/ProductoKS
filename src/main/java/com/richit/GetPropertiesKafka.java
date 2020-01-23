package com.richit;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetPropertiesKafka {
    final static Logger logger = Logger.getLogger(GetPropertiesKafka.class);

    public Properties GetKafkaValues(String filename) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(filename);
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
            logger.error("No se encuentra archivo de configuracion");
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }
}
