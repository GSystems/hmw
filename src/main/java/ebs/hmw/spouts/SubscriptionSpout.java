package ebs.hmw.spouts;

import ebs.hmw.model.SubModel;
import ebs.hmw.util.GeneralConstants;
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

import static ebs.hmw.util.ConfigurationConstants.SUB_TOTAL_MESSAGES_NUMBER;

public class SubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<List<SubModel>> subscriptions;
    private int totalMessagesNumber;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        subscriptions = convertFieldsToType(populateSubscriptionsMap());
//        ProjectProperties projectProperties = ProjectProperties.getInstance();
        totalMessagesNumber = SUB_TOTAL_MESSAGES_NUMBER; //Integer.valueOf(projectProperties.getProperties().getProperty("pub.total.number"));
    }

    @Override
    public void nextTuple() {
        for (List<SubModel> subscription : subscriptions) {
            if (totalMessagesNumber > 0) {
                collector.emit(new Values(subscriptions.indexOf(subscription)));
            }

            for (SubModel model : subscription) {
                collector.emit(new Values(model.getFieldValue().getLeft()));
                collector.emit(new Values(model.getOperator()));
                collector.emit(new Values(model.getFieldValue().getRight()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(GeneralConstants.RAW_SUBSCRIPTIONS_KEYWD));
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

    private List<List<SubModel>> populateSubscriptionsMap() {
        List<List<SubModel>> subscriptionsList = new ArrayList<>();

        List<SubModel> subscritpion0 = new ArrayList<>();
        List<SubModel> subscritpion1 = new ArrayList<>();
        List<SubModel> subscritpion2 = new ArrayList<>();

        subscritpion0.add(new SubModel(Pair.of("company", "Google"), "="));
        subscritpion0.add(new SubModel(Pair.of("value", "90"), ">="));
        subscritpion0.add(new SubModel(Pair.of("variation", "0.8"), "<"));

        subscritpion1.add(new SubModel(Pair.of("company", "Apple"), "="));
        subscritpion1.add(new SubModel(Pair.of("variation", "5"), "<"));

        subscritpion2.add(new SubModel(Pair.of("value", "5000"), ">="));

        subscriptionsList.add(subscritpion0);
        subscriptionsList.add(subscritpion1);
        subscriptionsList.add(subscritpion2);

        return subscriptionsList;
    }
}
