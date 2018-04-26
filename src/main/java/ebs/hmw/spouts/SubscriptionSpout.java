package ebs.hmw.spouts;

import ebs.hmw.model.SubModel;
import ebs.hmw.util.PubSubGeneratorConfiguration;
import ebs.hmw.util.SubFieldsEnum;
import ebs.hmw.util.TopoConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

import static ebs.hmw.util.PubSubGeneratorConfiguration.*;
import static ebs.hmw.util.FieldsGenerator.generateValueFromArray;
import static ebs.hmw.util.FieldsGenerator.generateDoubleFromRange;
import static ebs.hmw.util.GeneralConstants.*;
import static ebs.hmw.util.SubFieldsEnum.COMPANY_FIELD;
import static ebs.hmw.util.SubFieldsEnum.VALUE_FIELD;
import static ebs.hmw.util.SubFieldsEnum.VARIATION_FIELD;

public class SubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<List<SubModel>> subscriptions;
    private int presenceOfEqualsOperator = 0;
    private int allOperatorsCount = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        subscriptions = generateSubscriptions();
//        ProjectProperties projectProperties = ProjectProperties.getInstance();
    }

    @Override
    public void nextTuple() {

        for (List<SubModel> subscription : subscriptions) {
            for (SubModel model : subscription) {
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

    private List<List<SubModel>> generateSubscriptions() {
        List<List<SubModel>> subscriptionsList = new ArrayList<>();
        Map<SubFieldsEnum, Integer> presenceOfFileds = initializePresenceOfFieldsMap();

        for (int i = 0; i < PubSubGeneratorConfiguration.SUB_TOTAL_MESSAGES_NUMBER; i++) {
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

            subscriptionsList.add(subscription);
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
        } else {
            return false;
        }
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
