package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.Constants.*;

import java.util.Map;

public class SearchMessageTopicsBolt extends BaseRichBolt {

    private Logger log = LoggerFactory.getLogger(SearchMessageTopicsBolt.class);
    private OutputCollector outputCollector;
    private Map<String, Object> map;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getStringByField("topic");
        String key = tuple.getStringByField("key");
        String value = tuple.getStringByField("value");

        log.info("topic = " + topic);
        log.info("key = " + key);
        log.info("value = " + value);

        String inputAccountNumber = (String) map.get(INPUT_ACCOUNT_NUMBER_KEY);
        log.info("inputAccountNumber = " + inputAccountNumber);

        if (key.equals(inputAccountNumber)) {
            outputCollector.emit(tuple, new Values(key, value));
        }

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("account_no", "message"));
    }
}
