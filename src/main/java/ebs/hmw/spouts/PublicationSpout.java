package ebs.hmw.spouts;

import ebs.hmw.model.Publication;
import ebs.hmw.util.GeneralConstants;
import ebs.hmw.util.PubSubGenConf;
import ebs.hmw.util.TopoConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.joda.time.DateTime;
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
import static ebs.hmw.util.PubFieldsEnum.*;
import static ebs.hmw.util.PubSubGenConf.*;
import static ebs.hmw.util.TopoConverter.extractPubFromLine;

public class PublicationSpout extends BaseRichSpout {

	private static final Integer MAX_FAILS =
			PubSubGenConf.PUB_TOTAL_MESSAGES_NUMBER * 2 / 100;
	static Logger LOG = Logger.getLogger(PublicationSpout.class);

	private SpoutOutputCollector collector;
	private Map<Integer, Publication> pubs;
	private Map<Integer, Publication> toSend;
	private Map<Integer, Integer> pubsFailureCount;
	private int countId = 0;

	@Override
	public void open(Map configs, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		pubs = new HashMap<>();
		toSend = new HashMap<>();
		pubsFailureCount = new HashMap<>();

		savePublicationsToFile(configs);
		readPubsFromFile(configs);

		toSend.putAll(pubs);
	}

	@Override
	public void nextTuple() {

		if (!toSend.isEmpty()) {
			for (Map.Entry<Integer, Publication> entry : pubs.entrySet()) {
				Integer transactionId = entry.getKey();

				entry.getValue().setSendTime(DateTime.now());
				collector.emit(new Values(entry.getValue(), transactionId));

				toSend.clear();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(PUB_SPOUT_OUT, PUB_TRANSACTION_ID));
	}

	public void ack(Object pubId) {
		pubs.remove(pubId);
		LOG.info("Message fully processed [" + pubId + "]");
	}

	public void fail(Object pubId) {

		if (!pubsFailureCount.containsKey(pubId)) {
			throw new RuntimeException("Error, transaction id not found [" + pubId + "]");
		}

		Integer transactionId = (Integer) pubId;
		Integer failures = pubsFailureCount.get(transactionId) + 1;

		if (failures >= MAX_FAILS) {
			//If exceeds the max fails will go down the topology
			throw new RuntimeException("Error, transaction id [" + transactionId + "] has had many errors [" + failures + "]");
		}

		//If not exceeds the max fails we save the new fails quantity and re-send the message
		pubsFailureCount.put(transactionId, failures);

		toSend.put(transactionId, pubs.get(transactionId));

		LOG.info("Re-sending message [" + pubId + "]");
	}

	private void readPubsFromFile(Map configs) {
		FileReader fileReader;

		try {
			fileReader = new FileReader(configs.get(PUBS_FILE_PARAM).toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + configs.get(PUBS_FILE_PARAM) + "]");
		}

		try {
			BufferedReader reader = new BufferedReader(fileReader);

			String line;
			while ((line = reader.readLine()) != null) {
				extractPubFromLineToMap(line);
			}

			reader.close();
		} catch (IOException e) {
			throw new RuntimeException("Error reading tuple ", e);
		}
	}

	private void extractPubFromLineToMap(String line) {
		Publication publication = extractPubFromLine(line);
		publication.setPublicationId(countId);

		pubs.put(countId, publication);

		countId++;
	}

	private void savePublicationsToFile(Map configs) {
		List<List<Pair>> publications = generatePublications();

		try {
			FileWriter writer = new FileWriter(configs.get(PUBS_FILE_PARAM).toString());

			for (List<Pair> publication : publications) {
				StringBuilder stringBuilder = new StringBuilder();

				stringBuilder.append("{");

				for (Pair pair : publication) {
					stringBuilder.append("(");
					stringBuilder.append(String.valueOf(pair.getLeft()));
					stringBuilder.append(",");
					stringBuilder.append(String.valueOf(pair.getRight()));
					stringBuilder.append(");");
				}

				stringBuilder.append("}\n");
				stringBuilder.deleteCharAt(stringBuilder.indexOf(Character.toString('}')) - 1);

				writer.write(stringBuilder.toString());
			}

			writer.close();
		} catch (IOException e) {
			throw new RuntimeException("Error writing file [" + configs.get(PUBS_FILE_PARAM) + "]");
		}
	}

	private List<List<Pair>> generatePublications() {
		List<List<Pair>> publications = new ArrayList<>();

		for (int i = 0; i < PUB_TOTAL_MESSAGES_NUMBER; i++) {
			List<Pair> publication = new ArrayList<>();

			publication.add(Pair.of(COMPANY_FIELD.getCode(), generateValueFromArray(COMPANIES)));
			publication.add(Pair.of(VALUE_FIELD.getCode(), generateDoubleFromRange(PUB_VALUE_MIN_RANGE, PUB_VALUE_MAX_RANGE).toString()));
			publication.add(Pair.of(DROP_FIELD.getCode(), generateDoubleFromRange(PUB_DROP_MIN_RANGE, PUB_DROP_MAX_RANGE).toString()));
			publication.add(Pair.of(VARIATION_FIELD.getCode(), generateDoubleFromRange(PUB_VARIATION_MIN_RANGE, PUB_VARIATION_MAX_RANGE).toString()));
			publication.add(Pair.of(DATE_FIELD.getCode(), generateValueFromArray(DATES)));

			publications.add(publication);
		}

		return publications;
	}

	private List<List<Pair>> convertFieldsToType(List<List<Pair>> inputPublications) {
		List<List<Pair>> outputPublications = new ArrayList<>();
		List<Pair> outputPairs;

		for (List<Pair> inputPublication : inputPublications) {
			outputPairs = new ArrayList<>();

			for (Pair inputPair : inputPublication) {
				Pair outputPair = Pair.of(inputPair.getLeft(), TopoConverter.convertToType(inputPair.getRight()));
				outputPairs.add(outputPair);
			}

			outputPublications.add(outputPairs);
		}

		return outputPublications;
	}
}
