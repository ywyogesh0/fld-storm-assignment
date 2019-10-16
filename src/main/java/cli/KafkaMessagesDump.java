package cli;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static utils.Constants.*;

class KafkaMessagesDump {

    private String partitionKey;
    private String outputFilePath;
    private String topics;
    private Set<String> topicsSet = new HashSet<>();
    private List<JSONObject> jsonObjectList = new ArrayList<>();

    private Properties consumerProperties;

    KafkaMessagesDump(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.partitionKey = properties.getProperty(KAFKA_PARTITION_KEY);
        this.outputFilePath = properties.getProperty(KAFKA_OUTPUT_FILE_PATH);
        this.topics = properties.getProperty(KAFKA_TOPICS);

        String bootstrapServers = properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
        String groupId = properties.getProperty(KAFKA_GROUP_ID);

        // create consumer configs
        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    void run() throws IOException {
        populateTopicsSet();
        dumpTopicMessages();
    }

    private void populateTopicsSet() {
        String[] topicsArr = topics.split(",");
        if (topicsArr.length > 1) {
            Collections.addAll(topicsSet, topicsArr);
        } else {
            topicsSet.add(topics);
        }
    }

    private void dumpTopicMessages() throws IOException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        String filePath = outputFilePath + KafkaMessagesDump.class.getSimpleName() + "-"
                + System.currentTimeMillis() + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {

            for (String topic : topicsSet) {
                TopicPartition topicPartition = new TopicPartition(topic, partition(topic, partitionKey, consumer));
                consumer.assign(Collections.singleton(topicPartition));

                // OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(0, );

                // poll for new data
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                for (ConsumerRecord<String, String> record : records) {
                    if (record.key().equals(partitionKey)) {

                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("Topic", record.partition())
                                .put("Partition", record.offset())
                                .put("Offset", record.offset())
                                .put("Key", record.key())
                                .put("Value", record.value());

                        jsonObjectList.add(jsonObject);
                    }
                }
            }

            for (JSONObject json : jsonObjectList) {
                bufferedWriter.write(json.toString());
                bufferedWriter.newLine();
            }
        }
    }

    private int partition(String topic, String key, KafkaConsumer consumer) {
        if (key == null) {
            return -1;
        }

        List partitions = consumer.partitionsFor(topic);
        int numPartitions = partitions.size();

        // hash the keyBytes to choose a partition
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % numPartitions;
    }
}
