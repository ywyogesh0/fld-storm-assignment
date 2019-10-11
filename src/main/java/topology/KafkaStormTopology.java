package topology;

import bolts.DumpMessageTextBolt;
import bolts.DumpTablesCassandraBolt;
import bolts.SearchMessageTopicsBolt;
import bolts.SearchTableCassandraBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.Constants.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class KafkaStormTopology {

    private static Logger log = LoggerFactory.getLogger(KafkaStormTopology.class);
    private static Properties properties = new Properties();
    private static Set<String> topicsSet = new HashSet<>();

    public static void main(String[] args) throws Exception {
        loadProp(args);
        runTopology();
    }

    private static void loadProp(String[] args) throws IOException {
        log.info("Loading Properties...");
        String propertyFilePath = args[0];
        log.info("Property File Path :: " + propertyFilePath);

        try (FileInputStream fileInputStream = new FileInputStream(propertyFilePath)) {
            properties.load(fileInputStream);
        }
    }

    private static void runTopology() throws Exception {
        String bootstrapServers = properties.getProperty("bootstrap.servers");
        String inputAccountNumber = properties.getProperty("input.account.number");
        String cassandraKeyspace = properties.getProperty("cassandra.keyspace");
        String groupId = properties.getProperty("group.id");
        String outputFilePath = properties.getProperty("output.file.path");
        String cassandraHost = properties.getProperty("cassandra.host");
        String accountColumnName = properties.getProperty("account.column.name");

        String topics = properties.getProperty("topics");
        String[] topicsArr = topics.split(",");

        if (topicsArr.length > 1) {
            Collections.addAll(topicsSet, topicsArr);
        } else {
            topicsSet.add(topics);
        }

        Config config = new Config();
        config.put(INPUT_ACCOUNT_NUMBER_KEY, inputAccountNumber);
        config.put(CASSANDRA_KEYSPACE_KEY, cassandraKeyspace);
        config.put(OUTPUT_FILE_PATH_KEY, outputFilePath);
        config.put(CASSANDRA_HOST_KEY, cassandraHost);
        config.put(ACCOUNT_COLUMN_NAME_KEY, accountColumnName);
        config.setDebug(true);

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig.builder(bootstrapServers, topicsSet)
                .setProp("group.id", groupId)
                .setOffsetCommitPeriodMs(1000)
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-topics", new KafkaSpout<>(kafkaSpoutConfig), 1);
        builder.setBolt("search-messages-topics", new SearchMessageTopicsBolt(), 1)
                .shuffleGrouping("kafka-topics");
        builder.setBolt("dump-messages-text", new DumpMessageTextBolt(), 1)
                .shuffleGrouping("search-messages-topics");

        builder.setBolt("search-tables-cassandra", new SearchTableCassandraBolt(), 1);
        builder.setBolt("dump-tables-cassandra", new DumpTablesCassandraBolt(), 1)
                .shuffleGrouping("search-tables-cassandra");

        try {
            StormSubmitter.submitTopology("fld-topology-5", config, builder.createTopology());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new Exception(e.getMessage(), e);
        }
    }
}

