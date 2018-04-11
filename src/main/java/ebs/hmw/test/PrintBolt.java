package ebs.hmw.test;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrintBolt extends BaseRichBolt {

	private OutputCollector outputCollector;
	private List<String> incomingWords = new ArrayList<>();

	@Override public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
	}

	@Override public void execute(Tuple tuple) {
		String word = tuple.getStringByField("words");
		incomingWords.add(word);
	}

	@Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}

	public void cleanup() {
		System.out.println("results");
		for (String word : incomingWords) {
			System.out.println(word);
		}
	}
}
