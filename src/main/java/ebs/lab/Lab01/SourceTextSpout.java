package ebs.lab.Lab01;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SourceTextSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private int i = 0;
	private String[] sourcetext = {
			"text one",
			"text two",
			"text three",
			"text four",
			"too much text after one"
	};

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	public void nextTuple() {
		this.collector.emit(new Values(sourcetext[i]));
		i++;
		if (i >= sourcetext.length) {
			i = 0;
		}

		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}
}