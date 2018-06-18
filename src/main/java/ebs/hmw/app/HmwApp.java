package ebs.hmw.app;

import ebs.hmw.bolts.FilterBolt;
import ebs.hmw.bolts.MetricsBolt;
import ebs.hmw.bolts.SubscriberBolt;
import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.spouts.SubscriberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class HmwApp {

	private static final String CURRENT_TOPOLOGY = "test_topology";

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		PublicationSpout publisher = new PublicationSpout();
		SubscriberSpout subscriber1 = new SubscriberSpout(1, "subs1.txt", 5);
		SubscriberBolt subscriberBolt1 = new SubscriberBolt(1);

		SubscriberSpout subscriber2 = new SubscriberSpout(2, "subs2.txt", 10);
		SubscriberBolt subscriberBolt2 = new SubscriberBolt(2);

		SubscriberSpout subscriber3 = new SubscriberSpout(3, "subs3.txt", 1000);
		SubscriberBolt subscriberBolt3 = new SubscriberBolt(3);

		FilterBolt filterBolt = new FilterBolt();
		MetricsBolt metricsBolt = new MetricsBolt();

		builder.setSpout(PUBLISHER_SPOUT_1, publisher);
		builder.setSpout(SUBSCRIBER_SPOUT_1, subscriber1);
		builder.setSpout(SUBSCRIBER_SPOUT_2, subscriber2);
		builder.setSpout(SUBSCRIBER_SPOUT_3, subscriber3);

		builder.setBolt(SUBSCRIBER_BOLT_1, subscriberBolt1).shuffleGrouping(FILTER_BOLT_ID);
		builder.setBolt(SUBSCRIBER_BOLT_2, subscriberBolt2).shuffleGrouping(FILTER_BOLT_ID);
		builder.setBolt(SUBSCRIBER_BOLT_3, subscriberBolt3).shuffleGrouping(FILTER_BOLT_ID);

		builder.setBolt(FILTER_BOLT_ID, filterBolt)
				.shuffleGrouping(PUBLISHER_SPOUT_1)
				.shuffleGrouping(SUBSCRIBER_SPOUT_1)
				.shuffleGrouping(SUBSCRIBER_SPOUT_2)
				.shuffleGrouping(SUBSCRIBER_SPOUT_3);

		builder.setBolt(METRICS_BOLT_ID, metricsBolt)
				.allGrouping(SUBSCRIBER_BOLT_1)
				.allGrouping(SUBSCRIBER_BOLT_2)
				.allGrouping(SUBSCRIBER_BOLT_3);

		Config config = new Config();
		config.put(PUBS_FILE_PARAM, PUBS_FILE);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(CURRENT_TOPOLOGY, config, builder.createTopology());

		try {
			Thread.sleep( 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(CURRENT_TOPOLOGY);
		cluster.shutdown();
	}
}
