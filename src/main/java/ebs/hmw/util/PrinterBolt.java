package ebs.hmw.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrinterBolt extends BaseRichBolt {

	private String incomingFlux;
	private List<Pair> pairs;
	private String aux;
	private int count;

	public PrinterBolt(String incomingFlux) {
		this.incomingFlux = incomingFlux;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		pairs = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String input = tuple.getStringByField(incomingFlux);

		if (count % 2 == 0) {
			pairs.add(Pair.of(aux, input));
		}

		aux = input;
		count++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}

	public void cleanup() {
		for (Pair pair : pairs) {
			System.out.println(pair.getLeft() + " = " + pair.getRight());
		}
	}
}