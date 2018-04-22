package ebs.hmw.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProjectProperties {

    private static ProjectProperties projectProperties = new ProjectProperties();
    private static Properties properties;

    private ProjectProperties() {
    }

    public static void loadProperties() {

        InputStream input = null;

        try {
            String filename = "top.properties";
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            input = loader.getResourceAsStream(filename);
            properties.load(input);

//            properties.load(getClass().getClassLoader().getResourceAsStream(filename));

            if (input == null) {
                System.out.println("Sorry, unable to find " + filename);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally{
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static ProjectProperties getInstance( ) {
        return projectProperties;
    }

    public static Properties getProperties () {
        return properties;
    }
}
