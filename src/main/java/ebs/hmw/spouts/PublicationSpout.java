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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.FieldsGenerator.generateDoubleFromRange;
import static ebs.hmw.util.FieldsGenerator.generateValueFromArray;
import static ebs.hmw.util.GeneralConstants.COMPANIES;
import static ebs.hmw.util.GeneralConstants.DATES;
import static ebs.hmw.util.PubFieldsEnum.*;
import static ebs.hmw.util.PubSubGenConf.*;

public class PublicationSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Map<String, List<Pair>> publications;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		publications = generatePublications();
	}

	@Override
	public void nextTuple() {
		for (Map.Entry<String, List<Pair>> publication : publications.entrySet()) {

			collector.emit(new Values(publication.getKey()));

			for(Pair parameter : publication.getValue()) {

				collector.emit(new Values(parameter.getLeft()));
				collector.emit(new Values(parameter.getRight()));
				collector.emit(new Values("\n"));
			}
			collector.emit(new Values("\n"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(GeneralConstants.RAW_PUBLICATIONS_KEYWD));
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

	private Map<String, List<Pair>> generatePublications() {
		Map<String, List<Pair>> publications = new HashMap<>();

		for (long i = 0; i < PUB_TOTAL_MESSAGES_NUMBER; i++) {
			List<Pair> publication = new ArrayList<>();

			publication.add(Pair.of(COMPANY_FIELD.getCode(), generateValueFromArray(COMPANIES)));
            publication.add(Pair.of(VALUE_FIELD.getCode(), generateDoubleFromRange(PUB_VALUE_MIN_RANGE, PUB_VALUE_MAX_RANGE).toString()));
            publication.add(Pair.of(DROP_FIELD.getCode(), generateDoubleFromRange(PUB_DROP_MIN_RANGE, PUB_DROP_MAX_RANGE).toString()));
            publication.add(Pair.of(VARIATION_FIELD.getCode(), generateDoubleFromRange(PUB_VARIATION_MIN_RANGE, PUB_VARIATION_MAX_RANGE).toString()));
			publication.add(Pair.of(DATE_FIELD.getCode(), generateValueFromArray(DATES)));

			String id = "id = " + i + "\n";

			publications.put(id, publication);
		}

		return publications;
	}
}
