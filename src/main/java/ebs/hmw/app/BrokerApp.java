package ebs.hmw.app;

import ebs.hmw.bolts.FilterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class BrokerApp {

	private static final String BROKER_TOPOLOGY = "broker_topology";

	public static void main(String[] args) {
		TopologyBuilder brokerTopology = createBrokerTopology();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(BROKER_TOPOLOGY, new Config(), brokerTopology.createTopology());

		try {
			Thread.sleep( 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(BROKER_TOPOLOGY);
		cluster.shutdown();
	}

	private static TopologyBuilder createBrokerTopology() {
		TopologyBuilder brokerTopology = new TopologyBuilder();

		FilterBolt filterBolt = new FilterBolt();

		brokerTopology.setBolt(FILTER_BOLT_ID, filterBolt)
				.shuffleGrouping(PUBLISHER_SPOUT_1)
				.shuffleGrouping(SUBSCRIBER_SPOUT_1)
				.shuffleGrouping(SUBSCRIBER_SPOUT_2)
				.shuffleGrouping(SUBSCRIBER_SPOUT_3);

		return brokerTopology;
	}
}
