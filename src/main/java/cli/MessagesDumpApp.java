package cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MessagesDumpApp {

    private static Logger log = LoggerFactory.getLogger(MessagesDumpApp.class);
    private Properties properties = new Properties();

    public static void main(String[] args) throws IOException {
        MessagesDumpApp messagesDumpApp = new MessagesDumpApp();
        messagesDumpApp.loadPropertyFile(args[0]);

        messagesDumpApp.startKafkaMessagesDump();
        messagesDumpApp.startCassandraMessagesDump();
    }

    private void loadPropertyFile(String propertyFilePath) throws IOException {
        log.info("Loading Properties...");
        log.info("Property File Path :: " + propertyFilePath);

        try (FileInputStream fileInputStream = new FileInputStream(propertyFilePath)) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void startKafkaMessagesDump() throws IOException {
        new KafkaMessagesDump(this).run();
    }

    private void startCassandraMessagesDump() {
        new CassandraMessagesDump(this).run();
    }

    Properties getProperties() {
        return properties;
    }
}
