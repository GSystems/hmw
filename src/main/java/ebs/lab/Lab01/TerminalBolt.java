package ebs.lab.Lab01;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class TerminalBolt extends BaseRichBolt {

	private HashMap<String, Integer> count;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.count = new HashMap<>();
	}

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		this.count.put(word, count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void cleanup() {
		System.out.println("Topology Result:");
		for (Map.Entry<String, Integer> entry : this.count.entrySet()) {
		    System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}
}