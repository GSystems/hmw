package ebs.hmw.test;

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

public class PubSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Map<Integer, List<Pair>> publications = new HashMap<>();

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		List<Pair> publication0 = new ArrayList<>();

		publication0.add(Pair.of("company", "Sika"));
		publication0.add(Pair.of("value", "90.0"));
		publication0.add(Pair.of("drop", "10.0"));
		publication0.add(Pair.of("variation", "0.73"));
		publication0.add(Pair.of("date", "2.02.2022"));
		publication0.add(Pair.of("company", "Sika"));

		publications.put(1, publication0);
	}

	@Override
	public void nextTuple() {
		for (Map.Entry<Integer, List<Pair>> entry : publications.entrySet()) {
			for(Pair parameter : entry.getValue()) {
				collector.emit(new Values(parameter.getLeft()));
				collector.emit(new Values(parameter.getRight()));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("words"));
	}
}
