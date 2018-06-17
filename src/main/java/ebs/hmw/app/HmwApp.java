package ebs.hmw.app;

import ebs.hmw.bolts.FilterBolt;
import ebs.hmw.bolts.SubscriberBolt;
import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.spouts.SubscriberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static ebs.hmw.util.GeneralConstants.*;

public class HmwApp {

	private static final String CURRENT_TOPOLOGY = "test_topology";

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		PublicationSpout publisher = new PublicationSpout();
		SubscriberSpout subscriber1 = new SubscriberSpout(1, "subs1.txt");
//		SubscriberSpout subscriber2 = new SubscriberSpout(2, "subs2.txt");
//		SubscriberSpout subscriber3 = new SubscriberSpout(3, "subs3.txt");

		SubscriberBolt subscriberBolt1 = new SubscriberBolt(1);

		FilterBolt filterBolt = new FilterBolt();
//		MetricsBolt metricsBolt = new MetricsBolt();

		builder.setSpout(PUBLISHER_SPOUT_1, publisher);
		builder.setSpout(SUBSCRIBER_SPOUT_1, subscriber1);

		builder.setBolt(FILTER_BOLT_1, filterBolt)
				.shuffleGrouping(PUBLISHER_SPOUT_1)
				.fieldsGrouping(SUBSCRIBER_SPOUT_1, new Fields(SUBSCRIBER_1_STREAM));

		builder.setBolt(SUBSCRIBER_BOLT_1, subscriberBolt1).shuffleGrouping(FILTER_BOLT_1);

//		builder.setBolt(METRICS_BOLT_ID, metricsBolt).allGrouping(FILTER_BOLT_1);

		Config config = new Config();
		config.put(PUBS_FILE_PARAM, PUBS_FILE);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(CURRENT_TOPOLOGY, config, builder.createTopology());

		try {
			Thread.sleep(1 * 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(CURRENT_TOPOLOGY);
		cluster.shutdown();
	}
}
