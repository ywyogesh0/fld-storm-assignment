package cli;

import com.cedarsoftware.util.io.JsonWriter;
import org.apache.commons.lang.StringUtils;
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
    private String offset;
    private String timestamp;
    private String poll;
    private Set<String> topicsSet = new HashSet<>();
    private List<JSONObject> jsonObjectList = new ArrayList<>();

    private Properties consumerProperties;

    KafkaMessagesDump(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.partitionKey = properties.getProperty(KAFKA_PARTITION_KEY);
        this.outputFilePath = properties.getProperty(KAFKA_OUTPUT_FILE_PATH);
        this.topics = properties.getProperty(KAFKA_TOPICS);
        this.offset = properties.getProperty(KAFKA_PARTITION_OFFSET);
        this.timestamp = properties.getProperty(KAFKA_PARTITION_TIMESTAMP);
        this.poll = properties.getProperty(KAFKA_POLL_DURATION_MS);

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
        prepareTopicMessagesDump();
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

    private void prepareTopicMessagesDump() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        for (String topic : topicsSet) {
            TopicPartition topicPartition = new TopicPartition(topic, partition(topic, partitionKey, consumer));
            consumer.assign(Collections.singleton(topicPartition));

            // filter by offset or timestamp
            if (!StringUtils.isEmpty(offset)) {
                filterByOffSet(consumer, topicPartition);
            } else if (!StringUtils.isEmpty(timestamp)) {
                filterByTimestamp(consumer, topicPartition);
            }

            // poll for data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(poll)));

            for (ConsumerRecord<String, String> record : records) {
                if (record.key().equals(partitionKey)) {

                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("Topic", record.topic())
                            .put("Partition", record.partition())
                            .put("Offset", record.offset())
                            .put("Key", record.key())
                            .put("Value", record.value())
                            .put("Timestamp", record.timestamp());

                    jsonObjectList.add(jsonObject);
                }
            }
        }
    }

    private void filterByOffSet(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        consumer.seek(topicPartition, Long.parseLong(offset));
    }

    private void filterByTimestamp(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        Map<TopicPartition, Long> timestampMap = new HashMap<>();
        timestampMap.put(topicPartition, Long.parseLong(timestamp));

        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(timestampMap);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp : offsetAndTimestampMap.entrySet()) {
            consumer.seek(topicPartition, offsetAndTimestamp.getValue().offset());
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

    private void dumpTopicMessages() throws IOException {
        String filePath = outputFilePath + KafkaMessagesDump.class.getSimpleName() + "-"
                + System.currentTimeMillis() + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            for (JSONObject json : jsonObjectList) {
                bufferedWriter.write(JsonWriter.formatJson(json.toString()));
                bufferedWriter.newLine();
            }
        }
    }
}
