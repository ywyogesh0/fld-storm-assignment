package cli;

import com.datastax.driver.core.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static utils.Constants.*;

public class CassandraMessagesDumpThread implements Runnable {

    private static Logger log = LoggerFactory.getLogger(CassandraMessagesDumpThread.class);

    private Cluster cluster;
    private Session session;
    private String inputAccountNumber;
    private String cassandraKeyspace;
    private String outputFilePath;
    private String cassandraHost;
    private String accountColumnName;
    private Set<String> tableNamesSet = new HashSet<>();
    private List<String> tableStringList = new ArrayList<>();

    CassandraMessagesDumpThread(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.inputAccountNumber = properties.getProperty(INPUT_ACCOUNT_NUMBER);
        this.cassandraKeyspace = properties.getProperty(CASSANDRA_KEYSPACE);
        this.outputFilePath = properties.getProperty(OUTPUT_FILE_PATH);
        this.cassandraHost = properties.getProperty(CASSANDRA_HOST);
        this.accountColumnName = properties.getProperty(ACCOUNT_COLUMN_NAME);
    }

    @Override
    public void run() {
        try {
            connectCassandraCluster();
            filterTableNames();
            dumpTablesData();
            displayDumpInFile();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
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
        log.info("Connecting Cassandra Cluster on " + cassandraHost + "...");
        cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        session = cluster.connect();
        log.info("Cassandra Cluster Connected Successfully...");
    }

    private void filterTableNames() {
        Metadata metadata = cluster.getMetadata();

        Collection<TableMetadata> tablesMetadata = metadata.getKeyspace(cassandraKeyspace).getTables();
        for (TableMetadata tm : tablesMetadata) {

            String tableName = tm.getName();
            log.info("Table name:" + tableName);

            Collection<ColumnMetadata> columnsMetadata = tm.getColumns();

            for (ColumnMetadata cm : columnsMetadata) {
                String columnName = cm.getName().toLowerCase();
                log.info("Column name:" + columnName);

                if (columnName.equals(accountColumnName)) {
                    tableNamesSet.add(tableName);
                }
            }
        }
    }

    private void dumpTablesData() {
        log.info("inputAccountNumber = " + inputAccountNumber);

        for (String tableName : tableNamesSet) {
            JSONObject tableString = new JSONObject();
            String cqlQuery = "select * from " + cassandraKeyspace + "." + tableName + " where " + accountColumnName + "=" +
                    Integer.parseInt(inputAccountNumber);

            log.info("Executing CQL Query :: " + cqlQuery);
            ResultSet resultSet = session.execute(cqlQuery);

            List<Row> rowList = resultSet.all();
            int rowCount = rowList.size();
            if (rowCount > 0) {
                JSONObject rowString = new JSONObject();
                ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
                int columnsCount = columnDefinitions.size();

                for (Row row : rowList) {
                    for (int j = 0; j < columnsCount; j++) {
                        rowString.put(columnDefinitions.getName(j), row.getObject(j));
                    }
                }

                tableString.put(tableName, rowString);
            }

            tableStringList.add(tableString.toString());
        }
    }

    private void displayDumpInFile() {
        String filePath = outputFilePath + CassandraMessagesDumpThread.class.getSimpleName() + "-"
                + System.currentTimeMillis() + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            for (String tableDump : tableStringList) {
                bufferedWriter.write(tableDump);
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
