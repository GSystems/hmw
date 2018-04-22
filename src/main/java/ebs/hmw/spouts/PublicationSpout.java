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
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.ConfigurationConstants.PUB_TOTAL_MESSAGES_NUMBER;

public class PublicationSpout extends BaseRichSpout {

	private int totalMessagesNumber;
	private SpoutOutputCollector collector;
	private List<List<Pair>> publications;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		publications = convertFieldsToType(populatePublicationsMap());
//		ProjectProperties projectProperties = ProjectProperties.getInstance();
		totalMessagesNumber = PUB_TOTAL_MESSAGES_NUMBER; //Integer.valueOf(projectProperties.getProperties().getProperty("pub.total.number"));
	}

	@Override
	public void nextTuple() {
		for (List<Pair> publication : publications) {
			if (totalMessagesNumber > 0) {
				collector.emit(new Values(GeneralConstants.INDEX_IDENTIFIER_KEYWORD + publications.indexOf(publication)));
			}

			for(Pair parameter : publication) {
				collector.emit(new Values(parameter.getLeft()));
				collector.emit(new Values(parameter.getRight()));
			}
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

	private List<List<Pair>> populatePublicationsMap() {
		List<List<Pair>> publications = new ArrayList<>();

		List<Pair> publication0 = new ArrayList<>();
		List<Pair> publication1 = new ArrayList<>();
		List<Pair> publication2= new ArrayList<>();

		publication0.add(Pair.of("company", "Sika"));
		publication0.add(Pair.of("value", "90.0"));
		publication0.add(Pair.of("drop", "10.0"));
		publication0.add(Pair.of("variation", "0.73"));
		publication0.add(Pair.of("date", "2.02.2018"));

		publication1.add(Pair.of("company", "Apple"));
		publication1.add(Pair.of("value", "2400.0"));
		publication1.add(Pair.of("drop", "5.0"));
		publication1.add(Pair.of("variation", "0.2"));
		publication1.add(Pair.of("date", "8.03.2018"));

		publication2.add(Pair.of("company", "Tesla"));
		publication2.add(Pair.of("value", "1200.0"));
		publication2.add(Pair.of("drop", "2.0"));
		publication2.add(Pair.of("variation", "0.1"));
		publication2.add(Pair.of("date", "9.02.2017"));

		publications.add(publication0);
		publications.add(publication1);
		publications.add(publication2);

		return publications;
	}
}
