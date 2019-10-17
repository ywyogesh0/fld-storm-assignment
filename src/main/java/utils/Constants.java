package utils;

public class Constants {
    public final static String CASSANDRA_HOST_KEY = "CASSANDRA_HOST_KEY";
    public final static String CASSANDRA_KEYSPACE_KEY = "CASSANDRA_KEYSPACE_KEY";
    public final static String INPUT_ACCOUNT_NUMBER_KEY = "INPUT_ACCOUNT_NUMBER_KEY";
    public final static String OUTPUT_FILE_PATH_KEY = "OUTPUT_FILE_PATH_KEY";
    public final static String ACCOUNT_COLUMN_NAME_KEY = "ACCOUNT_COLUMN_NAME_KEY";

    // Kafka Params
    public final static String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public final static String KAFKA_TOPICS = "kafka.topics";
    public final static String KAFKA_GROUP_ID = "kafka.group.id";
    public final static String KAFKA_PARTITION_KEY = "kafka.partition.key";
    public final static String KAFKA_PARTITION_OFFSET = "kafka.partition.offset";
    public final static String KAFKA_PARTITION_TIMESTAMP = "kafka.partition.timestamp";
    public final static String KAFKA_OUTPUT_FILE_PATH = "kafka.output.file.path";
    public final static String KAFKA_POLL_DURATION_MS = "kafka.poll.duration.ms";

    // Cassandra Params
    public final static String CASSANDRA_HOST = "cassandra.host";
    public final static String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public final static String CASSANDRA_PARTITION_COLUMN_NAME = "cassandra.partition.column.name";
    public final static String CASSANDRA_PARTITION_COLUMN_VALUE = "cassandra.partition.column.value";
    public final static String CASSANDRA_OUTPUT_FILE_PATH = "cassandra.output.file.path";
}
