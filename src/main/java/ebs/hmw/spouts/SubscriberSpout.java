package ebs.hmw.spouts;

import ebs.hmw.model.Subscription;
import ebs.hmw.util.PubSubGenConf;
import ebs.hmw.util.SubFieldsEnum;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.FieldsGenerator.generateDoubleFromRange;
import static ebs.hmw.util.FieldsGenerator.generateValueFromArray;
import static ebs.hmw.util.GeneralConstants.*;
import static ebs.hmw.util.PubSubGenConf.*;
import static ebs.hmw.util.SubFieldsEnum.*;
import static ebs.hmw.util.TopoConverter.extractSubFromLine;

public class SubscriberSpout extends BaseRichSpout {

	private static final Integer MAX_FAILS =
			PubSubGenConf.PUB_TOTAL_MESSAGES_NUMBER * 2 / 100;
	static Logger LOG = Logger.getLogger(SubscriberSpout.class);

	private SpoutOutputCollector collector;
	private int subscriberId;
	private String subscriptionsFile;

	private Map<Integer, Subscription> subs;
	private Map<Integer, Subscription> toSend;
	private Map<Integer, Integer> subsFailureCount;
	private int subscriptionsCountId = 0;
	private int presenceOfEqualsOperator = 0;
	private int allOperatorsCount = 0;
	private int noOfSubscriptions;

	public SubscriberSpout(int subscriberId, String subscriptionsFile, int noOfSubscriptions) {
		this.subscriberId = subscriberId;
		this.subscriptionsFile = subscriptionsFile;
		this.noOfSubscriptions = noOfSubscriptions;
	}

	@Override
	public void open(Map configs, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		subs = new HashMap<>();
		toSend = new HashMap<>();
		subsFailureCount = new HashMap<>();

		generateAndSaveSubsriptionsToFile();
		readSubsFromFile();

		toSend.putAll(subs);
	}

	@Override
	public void nextTuple() {

		if (!toSend.isEmpty()) {
			for (Map.Entry<Integer, Subscription> entry : subs.entrySet()) {
				collector.emit(new Values(subscriberId, entry.getValue(), entry.getValue().getId()));
			}

			toSend.clear();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(SUBSCRIBER_SPOUT_STREAM, SUBSCRIBER_SPOUT_OUT, SUB_TRANSACTION_ID));
	}

	public void ack(Object pubId) {
		subs.remove(pubId);
		LOG.info("Message fully processed [" + pubId + "]");
	}

	public void fail(Object subId) {

		if (!subsFailureCount.containsKey(subId)) {
			throw new RuntimeException("Error, transaction id not found [" + subId + "]");
		}

		Integer transactionId = (Integer) subId;
		Integer failures = subsFailureCount.get(transactionId) + 1;

		if (failures >= MAX_FAILS) {
			//If exceeds the max fails will go down the topology
			throw new RuntimeException("Error, transaction id [" + transactionId + "] has had many errors [" + failures + "]");
		}

		//If not exceeds the max fails we save the new fails quantity and re-send the message
		subsFailureCount.put(transactionId, failures);

		toSend.put(transactionId, subs.get(transactionId));

		LOG.info("Re-sending message [" + subId + "]");
	}

	private void readSubsFromFile() {
		FileReader fileReader;

		try {
			fileReader = new FileReader(subscriptionsFile);

		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + subscriptionsFile + "]");
		}

		try {
			BufferedReader reader = new BufferedReader(fileReader);

			String line;
			while ((line = reader.readLine()) != null) {
				extractSubFromLineToMap(line);
			}

			reader.close();

		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple ", e);
		}
	}

	private Subscription extractSubFromLineToMap(String line) {
		Subscription subscription = extractSubFromLine(line);
		subscription.setPublications(new ArrayList<>());
		subscription.setId(subscriptionsCountId);

		subs.put(subscriptionsCountId, subscription);

		subscriptionsCountId++;

		return subscription;
	}

	private void generateAndSaveSubsriptionsToFile() {
		List<Subscription> subscriptions = generateSubscriptions();

		try {
			FileWriter writer = new FileWriter(subscriptionsFile);

			for (Subscription subscription : subscriptions) {
				StringBuilder stringBuilder = new StringBuilder();

				stringBuilder.append("{");

				if (subscription.getCompany() != null) {
					stringBuilder.append(generateStringForSubParam(COMPANY_FIELD,
							subscription.getCompany().getRight(), subscription.getCompany().getLeft()));
				}

				if (subscription.getValue() != null) {
					stringBuilder.append(generateStringForSubParam(VALUE_FIELD,
							subscription.getValue().getRight(), subscription.getValue().getLeft().toString()));
				}

				if (subscription.getVariation() != null) {
					stringBuilder.append(generateStringForSubParam(VARIATION_FIELD,
							subscription.getVariation().getRight(), subscription.getVariation().getLeft().toString()));
				}

				stringBuilder.append("}\n");
				stringBuilder.deleteCharAt(stringBuilder.indexOf(Character.toString('}')) - 1);

				writer.write(stringBuilder.toString());
			}

			writer.close();

		} catch (IOException e) {
			throw new RuntimeException("Error writing file [" + subscriptionsFile + "]");
		}
	}

	private String generateStringForSubParam(SubFieldsEnum field, String operator, String value) {
		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("(");
		stringBuilder.append(field.getCode());
		stringBuilder.append(",");
		stringBuilder.append(operator);
		stringBuilder.append(",");
		stringBuilder.append(value);
		stringBuilder.append(");");

		return stringBuilder.toString();
	}

	private List<Subscription> generateSubscriptions() {
		List<Subscription> subscriptions = new ArrayList<>();

		Map<SubFieldsEnum, Integer> presenceOfFileds = initializePresenceOfFieldsMap();

		for (long i = 0; i < noOfSubscriptions; i++) {
			Subscription subscription = new Subscription();

			if (fieldForAdd(COMPANY_FIELD, presenceOfFileds)) {
				subscription.setCompany(Pair.of(generateValueFromArray(COMPANIES), "="));

				Integer oldCountOfField = presenceOfFileds.get(COMPANY_FIELD);
				presenceOfFileds.put(COMPANY_FIELD, ++oldCountOfField);
				presenceOfEqualsOperator++;
				allOperatorsCount++;
			}

			if (fieldForAdd(VALUE_FIELD, presenceOfFileds)) {
				subscription.setValue(
						Pair.of(generateDoubleFromRange(SUB_VALUE_MIN_RANGE, SUB_VALUE_MAX_RANGE), addOperator()));

				Integer oldCountOfField = presenceOfFileds.get(VALUE_FIELD);
				presenceOfFileds.put(VALUE_FIELD, ++oldCountOfField);
			}

			if (fieldForAdd(VARIATION_FIELD, presenceOfFileds)) {
				subscription.setVariation(
						Pair.of(generateDoubleFromRange(SUB_VARIATION_MIN_RANGE, SUB_VARIATION_MAX_RANGE), addOperator()));

				Integer oldCountOfField = presenceOfFileds.get(VARIATION_FIELD);
				presenceOfFileds.put(VARIATION_FIELD, ++oldCountOfField);
			}

			subscriptions.add(subscription);
		}

		return subscriptions;
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
}
