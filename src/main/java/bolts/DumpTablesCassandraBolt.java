package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static utils.Constants.ACCOUNT_COLUMN_NAME_KEY;
import static utils.Constants.OUTPUT_FILE_PATH_KEY;

public class DumpTablesCassandraBolt implements IRichBolt {

    private BufferedWriter bufferedWriter;
    private OutputCollector outputCollector;
    private String accountColumnName;

    private Logger logger = LoggerFactory.getLogger(DumpTablesCassandraBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        accountColumnName = (String) map.get(ACCOUNT_COLUMN_NAME_KEY);

        String outputFilePath = (String) map.get(OUTPUT_FILE_PATH_KEY);
        String fileName = topologyContext.getThisComponentId() + "-" + topologyContext.getThisTaskId() + "-" +
                System.currentTimeMillis() + ".json";

        String filePath = outputFilePath + "/" + fileName;
        logger.info("output file path :: " + filePath);

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath));
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String accountColumnValue = tuple.getStringByField(accountColumnName);
        String tableNames = tuple.getStringByField("tableNames");

        logger.info("account_no :: " + accountColumnValue);
        logger.info("tableNames :: " + tableNames);

        String jsonString = new JSONObject()
                .put(accountColumnName, accountColumnValue)
                .put("tableNames", tableNames)
                .toString();

        logger.info("jsonString ::: " + jsonString);

        try {
            bufferedWriter.write(jsonString + "\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            logger.info(e.getMessage());
        }

        outputCollector.emit(tuple, new Values(accountColumnName, tableNames));
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
        if (bufferedWriter != null) {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                logger.info(e.getMessage());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("account_no", "tableNames"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
