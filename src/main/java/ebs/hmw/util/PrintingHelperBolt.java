package ebs.hmw.util;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrintingHelperBolt extends BaseRichBolt {

	private OutputCollector collector;
	private List<String> incomingWords;
	private String incomingFlux;

	public PrintingHelperBolt(String incomingFlux) {
		this.incomingFlux = incomingFlux;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
		incomingWords = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField(incomingFlux);
		incomingWords.add(word);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}

	public void cleanup() {
		System.out.println("results");

		for (String word : incomingWords) {
			System.out.println(word);
		}
	}
}
