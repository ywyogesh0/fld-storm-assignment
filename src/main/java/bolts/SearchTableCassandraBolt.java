package bolts;

import com.datastax.driver.core.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static utils.Constants.*;

public class SearchTableCassandraBolt implements IRichBolt {

    private Cluster cluster;
    private Session session;

    private String keyspaceName;
    private String accountColumnName;
    private Set<String> tableNamesSet = new HashSet<>();
    private Set<String> tableNamesResultSet = new HashSet<>();

    private Map<String, Object> map;
    private OutputCollector outputCollector;

    private Logger log = LoggerFactory.getLogger(SearchTableCassandraBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.outputCollector = outputCollector;

        keyspaceName = (String) map.get(CASSANDRA_KEYSPACE_KEY);
        accountColumnName = (String) map.get(ACCOUNT_COLUMN_NAME_KEY);
        String cassandraHost = (String) map.get(CASSANDRA_HOST_KEY);

        log.info("Connecting Cassandra Cluster on " + cassandraHost + "...");
        cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        session = cluster.connect();
        log.info("Cassandra Cluster Connected Successfully...");

        setTableNames();
        log.info("Table Names :: " + tableNamesSet);

        emitTableNames();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("account_no", "tableNames"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void setTableNames() {
        Metadata metadata = cluster.getMetadata();

        Collection<TableMetadata> tablesMetadata = metadata.getKeyspace(keyspaceName).getTables();
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

    private void emitTableNames() {
        {
            String inputAccountNumber = (String) map.get(INPUT_ACCOUNT_NUMBER_KEY);
            log.info("inputAccountNumber = " + inputAccountNumber);

            for (String tableName : tableNamesSet) {
                String cqlQuery = "select count(1) from " + keyspaceName + "." + tableName + " where " + accountColumnName + "=" +
                        Integer.parseInt(inputAccountNumber) + " limit 1";

                log.info("Executing CQL Query :: " + cqlQuery);
                ResultSet resultSet = session.execute(cqlQuery);

                if (resultSet.all().size() > 0) {
                    tableNamesResultSet.add(tableName);
                }
            }

            if (tableNamesResultSet.size() > 0) {
                outputCollector.emit(new Values(inputAccountNumber, String.join(",", tableNamesResultSet)));
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void cleanup() {
        if (session != null) {
            session.close();
        }

        if (cluster != null) {
            cluster.close();
        }
    }
}
