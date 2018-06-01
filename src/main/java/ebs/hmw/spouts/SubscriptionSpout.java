package ebs.hmw.spouts;

import ebs.hmw.model.SubModel;
import ebs.hmw.util.PubSubGenConf;
import ebs.hmw.util.SubFieldsEnum;
import ebs.hmw.util.TopoConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.FieldsGenerator.generateDoubleFromRange;
import static ebs.hmw.util.FieldsGenerator.generateValueFromArray;
import static ebs.hmw.util.GeneralConstants.*;
import static ebs.hmw.util.PubSubGenConf.*;
import static ebs.hmw.util.SubFieldsEnum.*;

public class SubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Map<String, List<SubModel>> subscriptions;
    private int presenceOfEqualsOperator = 0;
    private int allOperatorsCount = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        subscriptions = generateSubscriptions();
    }

    @Override
    public void nextTuple() {

        for (Map.Entry<String, List<SubModel>> subscription : subscriptions.entrySet()) {

            collector.emit(new Values(subscription.getKey()));

            for (SubModel model : subscription.getValue()) {
                collector.emit(new Values(model.getFieldValue().getLeft()));
                collector.emit(new Values(model.getOperator()));
                collector.emit(new Values(model.getFieldValue().getRight()));
                collector.emit(new Values("\n"));
            }
            collector.emit(new Values("\n"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(RAW_SUBSCRIPTIONS_KEYWD));
    }

    private Map<String, List<SubModel>> generateSubscriptions() {
        Map<String, List<SubModel>> subscriptionsList = new HashMap<>();

        Map<SubFieldsEnum, Integer> presenceOfFileds = initializePresenceOfFieldsMap();

        for (long i = 0; i < PubSubGenConf.SUB_TOTAL_MESSAGES_NUMBER; i++) {
            List<SubModel> subscription = new ArrayList<>();

            if (fieldForAdd(COMPANY_FIELD, presenceOfFileds)) {
                subscription.add(new SubModel(Pair.of(COMPANY_FIELD.getCode(),
                        generateValueFromArray(COMPANIES)), "="));

                Integer oldCountOfField = presenceOfFileds.get(COMPANY_FIELD);
                presenceOfFileds.put(COMPANY_FIELD, ++oldCountOfField);
                presenceOfEqualsOperator++;
                allOperatorsCount++;
            }

            if (fieldForAdd(VALUE_FIELD, presenceOfFileds)) {
                subscription.add(new SubModel(Pair.of(VALUE_FIELD.getCode(),
                        generateDoubleFromRange(SUB_VALUE_MIN_RANGE, SUB_VALUE_MAX_RANGE).toString()), addOperator()));

                Integer oldCountOfField = presenceOfFileds.get(VALUE_FIELD);
                presenceOfFileds.put(VALUE_FIELD, ++oldCountOfField);
            }

            if (fieldForAdd(VARIATION_FIELD, presenceOfFileds)) {
                subscription.add(new SubModel(Pair.of(VARIATION_FIELD.getCode(),
                        generateDoubleFromRange(SUB_VARIATION_MIN_RANGE, SUB_VARIATION_MAX_RANGE).toString()), addOperator()));

                Integer oldCountOfField = presenceOfFileds.get(VARIATION_FIELD);
                presenceOfFileds.put(VARIATION_FIELD, ++oldCountOfField);
            }

            String id = "id = " + i + "\n";

            subscriptionsList.put(id, subscription);
        }

        return subscriptionsList;
    }

    private String addOperator() {
        String operator;
        Double actualEqualsPerc = 0.0;

        if (allOperatorsCount > 0) {
            actualEqualsPerc = Double.valueOf(presenceOfEqualsOperator) / Double.valueOf(allOperatorsCount) * 100.0;
        }

        if (actualEqualsPerc <= SUB_EQUALS_OPERATOR_PRESENCE) {
            operator = "=";
            presenceOfEqualsOperator++;
        } else {
            operator = generateValueFromArray(OPERATORS);
        }

        allOperatorsCount++;

        return operator;
    }

    private boolean fieldForAdd(SubFieldsEnum field, Map<SubFieldsEnum, Integer> presenceOfFileds) {

        Double totalNoOfFields = Double.valueOf(presenceOfFileds.get(COMPANY_FIELD) +
                presenceOfFileds.get(VALUE_FIELD) + presenceOfFileds.get(VARIATION_FIELD));

        Double actualFieldPresencePerc = 0.0;

        if (totalNoOfFields > 0) {
            actualFieldPresencePerc = presenceOfFileds.get(field) / totalNoOfFields * 100.0;
        }

        if (actualFieldPresencePerc <= field.getPerc()) {
            return true;
        }

        return false;
    }

    private Map<SubFieldsEnum, Integer> initializePresenceOfFieldsMap() {
        Map<SubFieldsEnum, Integer> presenceOfFileds = new HashMap<>();

        presenceOfFileds.put(COMPANY_FIELD, 0);
        presenceOfFileds.put(VALUE_FIELD, 0);
        presenceOfFileds.put(VARIATION_FIELD, 0);

        return presenceOfFileds;
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

}
