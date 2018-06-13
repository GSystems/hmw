package ebs.hmw.bolts;

import ebs.hmw.model.Publication;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static ebs.hmw.util.GeneralConstants.METRICS_BOLT_STREAM;

public class MetricsBolt extends BaseRichBolt {

	private double latencyTotalSeconds;
	private int pubCount;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		latencyTotalSeconds = 0;
		pubCount = 0;
	}

	@Override
	public void execute(Tuple input) {
		Publication publication = (Publication) input.getValueByField(METRICS_BOLT_STREAM);
		long seconds = publication.getReceivedTime().getSecondOfDay() - publication.getSendTime().getSecondOfDay();
		pubCount++;
		latencyTotalSeconds += (double) seconds;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}

	@Override
	public void cleanup() {
		System.out.println("\n\n #Pubs = " + pubCount);
		System.out.println("\nlatency = " + latencyTotalSeconds / pubCount + "\n\n");
	}
}
