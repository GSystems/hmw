package ebs.hmw.bolts;

import ebs.hmw.model.Publication;
import ebs.hmw.util.GeneralConstants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ebs.hmw.util.GeneralConstants.SUBSCRIBER_1_STREAM;
import static ebs.hmw.util.GeneralConstants.SUBSCRIBER_BOLT_STREAM;
import static ebs.hmw.util.GeneralConstants.SUBSCRIBER_ID;

public class SubscriberBolt extends BaseRichBolt {

	List<Publication> publications;
	private Integer subscriberId;

	public SubscriberBolt(Integer subscriberId) {
		this.subscriberId = subscriberId;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		publications = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		Publication publication = (Publication) tuple.getValueByField(SUBSCRIBER_BOLT_STREAM);
		publications.add(publication);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(GeneralConstants.SUBSCRIBER_1_STREAM));
	}

	@Override
	public void cleanup() {
		System.out.println("\n\nSubscriber: " + subscriberId
				+ " has " + publications.size() + " mathces\n\n");
	}
}
