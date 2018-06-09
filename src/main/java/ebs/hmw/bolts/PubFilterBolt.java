package ebs.hmw.bolts;

import ebs.hmw.model.Publication;
import ebs.hmw.model.Subscription;
import ebs.hmw.util.GeneralConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubFilterBolt extends BaseRichBolt {

	private OutputCollector collector;
	private Map<Integer, List<Pair>> pubs;
	private List<Pair> pairs;
	private String aux;
	private int count;
	private int pubId;
	private List<Subscription> subscriptions;
	private String outputStream;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairs = new ArrayList<>();
		aux = StringUtils.EMPTY;
		pubs = new HashMap<>();
		count = 1;
		pubId = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		String input = tuple.getStringByField(GeneralConstants.FILTER_1_KEYWD);

		Publication publication = (Publication) tuple.getValueByField(GeneralConstants.FILTER_1_KEYWD);

		if (count % 2 == 0) {
			pairs.add(Pair.of(aux, input));
		}

		if (count % 10 == 0) {
			pubs.put(pubId, pairs);
			pairs = new ArrayList<>();

			pubId++;
		}

		aux = input;
		count++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(GeneralConstants.PRINTER_INPUT_KEYWD));
	}

	public void cleanup() {
		for (Map.Entry<Integer, List<Pair>> entry : pubs.entrySet()) {
			for (Pair pair : entry.getValue()) {
				System.out.println(pair.getLeft() + " = " + pair.getRight());
			}
		}
	}
}
