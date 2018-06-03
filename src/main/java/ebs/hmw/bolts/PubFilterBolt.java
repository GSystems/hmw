package ebs.hmw.bolts;

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
import java.util.List;
import java.util.Map;

public class PubFilterBolt extends BaseRichBolt {

	private OutputCollector collector;
	private List<Pair> pairs;
	private String aux;
	private int count;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairs = new ArrayList<>();
		aux = StringUtils.EMPTY;
		count = 1;
	}

	@Override
	public void execute(Tuple tuple) {
		String input = tuple.getStringByField(GeneralConstants.FILTER_1_KEYWD);

		if (count % 2 == 0) {
			pairs.add(Pair.of(aux, input));
		}

		aux = input;
		count++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(GeneralConstants.PRINTER_INPUT_KEYWD));
	}

	public void cleanup() {
		for (Pair pair : pairs) {
			System.out.println(pair.getLeft() + " = " + pair.getRight());
		}
	}
}
