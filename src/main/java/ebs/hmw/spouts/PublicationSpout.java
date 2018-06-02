package ebs.hmw.spouts;

import ebs.hmw.util.GeneralConstants;
import ebs.hmw.util.TopoConverter;
import org.apache.commons.lang3.tuple.Pair;
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

public class PublicationSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private boolean completed = false;
	private FileReader fileReader;

	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	@Override
	public void open(Map configs, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		savePublicationsToFile(configs);
		openPublicationsFile(configs);
	}

	@Override
	public void nextTuple() {

		if (completed) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {

			}
			return;
		}

		try {
			BufferedReader reader = new BufferedReader(fileReader);

			String line;
			while ((line = reader.readLine()) != null) {
				collector.emit(new Values(line));
			}

			reader.close();

		} catch (IOException e) {
			throw new RuntimeException("Error reading tuple ", e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(GeneralConstants.PRINTER_INPUT_KEYWD));
	}

	private void savePublicationsToFile(Map configs) {
		Map<String, List<Pair>> publications = generatePublications();

		try {
			FileWriter writer = new FileWriter(configs.get(PUBS_FILE_PARAM).toString());

			for (Map.Entry<String, List<Pair>> publication : publications.entrySet()) {
				int counter = 1;

				writer.write("{(id," + publication.getKey() + ");");

				for (Pair parameter : publication.getValue()) {
					writer.write("(");
					writer.write(String.valueOf(parameter.getLeft()));
					writer.write(",");
					writer.write(String.valueOf(parameter.getRight()));

					if (counter == publication.getValue().size()) {
						writer.write(")");
					} else {
						writer.write(");");
					}

					counter++;
				}

				writer.write("}\n");
			}

			writer.close();

		} catch (IOException e) {
			throw new RuntimeException("Error writing file [" + configs.get(PUBS_FILE_PARAM) + "]");
		}
	}

	private void openPublicationsFile(Map configs) {
		try {
			fileReader = new FileReader(configs.get(PUBS_FILE_PARAM).toString());

		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + configs.get(PUBS_FILE_PARAM) + "]");
		}
	}

	private Map<String, List<Pair>> generatePublications() {
		Map<String, List<Pair>> publications = new HashMap<>();

		for (long i = 0; i < PUB_TOTAL_MESSAGES_NUMBER; i++) {
			List<Pair> publication = new ArrayList<>();

			publication.add(Pair.of(COMPANY_FIELD.getCode(), generateValueFromArray(COMPANIES)));
			publication.add(Pair.of(VALUE_FIELD.getCode(), generateDoubleFromRange(PUB_VALUE_MIN_RANGE, PUB_VALUE_MAX_RANGE).toString()));
			publication.add(Pair.of(DROP_FIELD.getCode(), generateDoubleFromRange(PUB_DROP_MIN_RANGE, PUB_DROP_MAX_RANGE).toString()));
			publication.add(Pair.of(VARIATION_FIELD.getCode(), generateDoubleFromRange(PUB_VARIATION_MIN_RANGE, PUB_VARIATION_MAX_RANGE).toString()));
			publication.add(Pair.of(DATE_FIELD.getCode(), generateValueFromArray(DATES)));

			publications.put(String.valueOf(i), publication);
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
