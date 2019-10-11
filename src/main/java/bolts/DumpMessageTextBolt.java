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

import static utils.Constants.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class DumpMessageTextBolt implements IRichBolt {

    private BufferedWriter bufferedWriter;
    private OutputCollector outputCollector;

    private Logger logger = LoggerFactory.getLogger(DumpMessageTextBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

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
        String accountColumnValue = tuple.getStringByField("account_no");
        String message = tuple.getStringByField("message");

        logger.info("account_no :: " + accountColumnValue);
        logger.info("message :: " + message);

        String jsonString = new JSONObject()
                .put("account_no", accountColumnValue)
                .put("message", message)
                .toString();

        logger.info("jsonString ::: " + jsonString);

        try {
            bufferedWriter.write(jsonString + "\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            logger.info(e.getMessage());
        }

        outputCollector.emit(tuple, new Values("account_no", message));
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
        outputFieldsDeclarer.declare(new Fields("account_no", "message"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
