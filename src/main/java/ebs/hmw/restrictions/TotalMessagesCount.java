package ebs.hmw.restrictions;

import ebs.hmw.util.GeneralConstants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static ebs.hmw.util.ConfigurationConstants.PUB_TOTAL_MESSAGES_NUMBER;

public class TotalMessagesCount extends BaseRichBolt {

    private int totalMessagesNumber;
    private OutputCollector collector;
    private String incomeFlux;
    private String outcomeFlux;
    private int messagesCount = 0;

//    private List<String> totalWords = new ArrayList<>();
//    private final Integer TOTAL_WORDS_IN_A_MESSAGE = 5;
//    private int wordsCount = 0;


    public TotalMessagesCount(String incomeFlux, String outcomeFlux) {
        this.incomeFlux = incomeFlux;
        this.outcomeFlux = outcomeFlux;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
//        ProjectProperties projectProperties = ProjectProperties.getInstance();
        totalMessagesNumber = PUB_TOTAL_MESSAGES_NUMBER; //Integer.valueOf(projectProperties.getProperties().getProperty("pub.total.number"));
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(incomeFlux);

        if (totalMessagesNumber > 0) {
            if (word.startsWith(GeneralConstants.INDEX_IDENTIFIER_KEYWORD)) {
                messagesCount = Character.getNumericValue(word.charAt(3));
            } else if (messagesCount < totalMessagesNumber) {
                collector.emit(new Values(word));
            }
        } else {
            collector.emit(new Values(word));
        }

//        if (wordsCount == TOTAL_WORDS_IN_A_MESSAGE) {
//            messagesCount++;
//        }
//
//        if (messagesCount < PUB_TOTAL_MESSAGES_NUMBER) {
//            totalWords.add(word);
//        }
//        wordsCount++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(outcomeFlux));
    }
}
