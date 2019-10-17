package cli;

import com.cedarsoftware.util.io.JsonWriter;
import com.datastax.driver.core.*;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static utils.Constants.*;

class CassandraMessagesDump {

    private Cluster cluster;
    private Session session;
    private String partitionColumnValue;
    private String cassandraKeyspace;
    private String outputFilePath;
    private String cassandraHost;
    private String partitionColumnName;
    private Set<String> tableNamesSet = new HashSet<>();
    private List<String> tableStringList = new ArrayList<>();

    CassandraMessagesDump(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.partitionColumnValue = properties.getProperty(CASSANDRA_PARTITION_COLUMN_VALUE);
        this.cassandraKeyspace = properties.getProperty(CASSANDRA_KEYSPACE);
        this.outputFilePath = properties.getProperty(CASSANDRA_OUTPUT_FILE_PATH);
        this.cassandraHost = properties.getProperty(CASSANDRA_HOST);
        this.partitionColumnName = properties.getProperty(CASSANDRA_PARTITION_COLUMN_NAME);
    }

    void run() {
        try {
            connectCassandraCluster();
            filterTableNames();
            prepareTablesDumpData();
            dumpDataInFile();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (session != null) {
                session.close();
            }

            if (cluster != null) {
                cluster.close();
            }
        }
    }

    private void connectCassandraCluster() {
        System.out.println("Connecting Cassandra Cluster on " + cassandraHost + "...");
        cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        session = cluster.connect();
        System.out.println("Cassandra Cluster Connected Successfully...");
    }

    private void filterTableNames() {
        Metadata metadata = cluster.getMetadata();

        Collection<TableMetadata> tablesMetadata = metadata.getKeyspace(cassandraKeyspace).getTables();
        for (TableMetadata tm : tablesMetadata) {

            String tableName = tm.getName();

            Collection<ColumnMetadata> columnsMetadata = tm.getColumns();

            for (ColumnMetadata cm : columnsMetadata) {
                String columnName = cm.getName().toLowerCase();

                if (columnName.equals(partitionColumnName)) {
                    tableNamesSet.add(tableName);
                }
            }
        }
    }

    private void prepareTablesDumpData() {
        for (String tableName : tableNamesSet) {
            JSONObject tableString = new JSONObject();
            String cqlQuery = "select * from " + cassandraKeyspace + "." + tableName + " where " + partitionColumnName + "=" +
                    Integer.parseInt(partitionColumnValue);

            ResultSet resultSet = session.execute(cqlQuery);

            List<Row> rowList = resultSet.all();
            int rowCount = rowList.size();
            if (rowCount > 0) {
                List<JSONObject> parentRowList = new ArrayList<>();
                ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
                int columnsCount = columnDefinitions.size();

                for (Row row : rowList) {
                    JSONObject rowString = new JSONObject();
                    for (int j = 0; j < columnsCount; j++) {
                        rowString.put(columnDefinitions.getName(j), row.getObject(j));
                    }

                    parentRowList.add(rowString);
                }

                tableString.put(tableName, parentRowList);
            }

            tableStringList.add(tableString.toString());
        }
    }

    private void dumpDataInFile() {
        String filePath = outputFilePath + CassandraMessagesDump.class.getSimpleName() + "-"
                + System.currentTimeMillis() + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            for (String tableDump : tableStringList) {
                bufferedWriter.write(JsonWriter.formatJson(tableDump));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
