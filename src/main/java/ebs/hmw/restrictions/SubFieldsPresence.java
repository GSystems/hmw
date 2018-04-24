package ebs.hmw.restrictions;

import ebs.hmw.util.SubFieldsEnum;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static ebs.hmw.util.GeneralConstants.RAW_SUB_OUTPUT;

public class SubFieldsPresence extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Integer> subscriptionFields;
    private Integer totalFieldNo;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        subscriptionFields = new HashMap<>();
        totalFieldNo = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        String field = tuple.getStringByField(RAW_SUB_OUTPUT);

        if (checkForFieldName(field)) {

            Double percForField = calculatePercForField(field);

            if (percForField <= SubFieldsEnum.valueOf(field).getPerc()) {

                Integer fieldCount = getCounterForField(field);
                subscriptionFields.put(field, fieldCount);

                totalFieldNo++;

                collector.emit(new Values(field));

                String operator = tuple.getStringByField(RAW_SUB_OUTPUT);
                String fieldValue = tuple.getStringByField(RAW_SUB_OUTPUT);

                collector.emit(new Values(operator));
                collector.emit(new Values(fieldValue));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private Integer getCounterForField(String word) {
        Integer fieldCount = 0;

        if (subscriptionFields.get(word) != null) {
            fieldCount = subscriptionFields.get(word);
        }

        return fieldCount++;
    }

    private boolean checkForFieldName(String word) {

        if (SubFieldsEnum.valueOf(word) != null) {
            return true;
        }

        return false;
    }

    private Double calculatePercForField(String word) {
        Integer fieldCount = subscriptionFields.get(word);

        return Double.valueOf(fieldCount * 100 / totalFieldNo);
    }
}
