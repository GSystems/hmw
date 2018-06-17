package ebs.hmw.bolts;

import ebs.hmw.model.Publication;
import ebs.hmw.model.Subscription;
import ebs.hmw.util.Matcher;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static ebs.hmw.util.GeneralConstants.*;

public class FilterBolt extends BaseRichBolt {

	private OutputCollector collector;
	private Matcher matcher;
	private String outputStream;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		matcher = new Matcher();
	}

	@Override
	public void execute(Tuple tuple) {
		if (tuple.size() == 2) {
			Publication publication = (Publication) tuple.getValueByField(PUB_SPOUT_OUT);

			publication.setReceivedTime(DateTime.now());

			if (matcher.matchPublication(publication)) {
				collector.emit(new Values(publication));
			}
		}

		if (tuple.size() == 3) {
			int subscriberId = (int) tuple.getValueByField(SUBSCRIBER_1_STREAM);
			matcher.addSubscriber(subscriberId);
			matcher.addSubscription(subscriberId, (Subscription) tuple.getValueByField(SUBSCRIBER_SPOUT_OUT));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(SUBSCRIBER_BOLT_STREAM));
	}
}
