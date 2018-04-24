package ebs.hmw.spouts;

import ebs.hmw.model.SubModel;
import ebs.hmw.util.PubSubGeneratorConfiguration;
import ebs.hmw.util.TopoConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.PubSubGeneratorConfiguration.*;
import static ebs.hmw.util.FieldsGenerator.generateFieldFromArray;
import static ebs.hmw.util.FieldsGenerator.generateDoubleFromRange;
import static ebs.hmw.util.GeneralConstants.*;
import static ebs.hmw.util.SubFieldsEnum.COMPANY_FIELD;
import static ebs.hmw.util.SubFieldsEnum.VALUE_FIELD;
import static ebs.hmw.util.SubFieldsEnum.VARIATION_FIELD;

public class SubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<List<SubModel>> subscriptions;
    private int totalMessagesNumber;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        subscriptions = generateSubscriptionsMap();
//        ProjectProperties projectProperties = ProjectProperties.getInstance();
        totalMessagesNumber = SUB_TOTAL_MESSAGES_NUMBER; //Integer.valueOf(projectProperties.getProperties().getProperty("pub.total.number"));
    }

    @Override
    public void nextTuple() {
        for (List<SubModel> subscription : subscriptions) {
            for (SubModel model : subscription) {
                collector.emit(new Values(model.getFieldValue().getLeft()));
                collector.emit(new Values(model.getFieldValue().getRight()));
                collector.emit(new Values(model.getOperator()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(RAW_SUBSCRIPTIONS_KEYWD));
    }

    private List<List<SubModel>> convertFieldsToType(List<List<SubModel>> inputSubs) {
        List<List<SubModel>> outputSubs = new ArrayList<>();
        List<SubModel> outputSub;

        for (List<SubModel> inputSub : inputSubs) {
            outputSub = new ArrayList<>();

            for (SubModel inputSubModel : inputSub) {
                Pair pair = Pair.of(
                        inputSubModel.getFieldValue().getLeft(),
                        TopoConverter.convertToType(inputSubModel.getFieldValue().getRight()));

                SubModel subModel = new SubModel(pair, inputSubModel.getOperator());

                outputSub.add(subModel);
            }

            outputSubs.add(outputSub);
        }

        return outputSubs;
    }

    private List<List<SubModel>> generateSubscriptionsMap() {
        List<List<SubModel>> subscriptionsList = new ArrayList<>();

        for (int i = 0; i < PubSubGeneratorConfiguration.SUB_TOTAL_MESSAGES_NUMBER; i++) {
            List<SubModel> subscription = new ArrayList<>();

            subscription.add(new SubModel(Pair.of(COMPANY_FIELD.getCode(), generateFieldFromArray(COMPANIES)), "="));
            subscription.add(new SubModel(Pair.of(VALUE_FIELD.getCode(),
                    generateDoubleFromRange(SUB_VALUE_MIN_RANGE, SUB_VALUE_MAX_RANGE).toString()), ">="));
            subscription.add(new SubModel(Pair.of(VARIATION_FIELD.getCode(),
                    generateDoubleFromRange(SUB_VARIATION_MIN_RANGE, SUB_VARIATION_MAX_RANGE).toString()), "<"));

            subscriptionsList.add(subscription);
        }

        return subscriptionsList;
    }
}
