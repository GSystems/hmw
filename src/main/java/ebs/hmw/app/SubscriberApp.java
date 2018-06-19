package ebs.hmw.app;

import ebs.hmw.bolts.MetricsBolt;
import ebs.hmw.bolts.SubscriberBolt;
import ebs.hmw.spouts.SubscriberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class SubscriberApp {

	private static final String SUBSCRIBER_TOPOLOGY = "subscriber_topology";

	public static void main(String[] args) {
		TopologyBuilder subscriberTopology = createSubscriberTopology();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(SUBSCRIBER_TOPOLOGY, new Config(), subscriberTopology.createTopology());

		try {
			Thread.sleep( 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(SUBSCRIBER_TOPOLOGY);
		cluster.shutdown();
	}

	private static TopologyBuilder createSubscriberTopology() {
		TopologyBuilder subscriberTopology = new TopologyBuilder();

		SubscriberSpout subscriber1 = new SubscriberSpout(1, "subs1.txt", 5);
		SubscriberBolt subscriberBolt1 = new SubscriberBolt(1);

		SubscriberSpout subscriber2 = new SubscriberSpout(2, "subs2.txt", 10);
		SubscriberBolt subscriberBolt2 = new SubscriberBolt(2);

		SubscriberSpout subscriber3 = new SubscriberSpout(3, "subs3.txt", 1000);
		SubscriberBolt subscriberBolt3 = new SubscriberBolt(3);

		subscriberTopology.setSpout(SUBSCRIBER_SPOUT_1, subscriber1);
		subscriberTopology.setSpout(SUBSCRIBER_SPOUT_2, subscriber2);
		subscriberTopology.setSpout(SUBSCRIBER_SPOUT_3, subscriber3);

		subscriberTopology.setBolt(SUBSCRIBER_BOLT_1, subscriberBolt1).shuffleGrouping(FILTER_BOLT_ID);
		subscriberTopology.setBolt(SUBSCRIBER_BOLT_2, subscriberBolt2).shuffleGrouping(FILTER_BOLT_ID);
		subscriberTopology.setBolt(SUBSCRIBER_BOLT_3, subscriberBolt3).shuffleGrouping(FILTER_BOLT_ID);

		MetricsBolt metricsBolt = new MetricsBolt();

		subscriberTopology.setBolt(METRICS_BOLT_ID, metricsBolt)
				.allGrouping(SUBSCRIBER_BOLT_1)
				.allGrouping(SUBSCRIBER_BOLT_2)
				.allGrouping(SUBSCRIBER_BOLT_3);

		return subscriberTopology;
	}
}
