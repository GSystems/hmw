package ebs.hmw.app;

import ebs.hmw.bolts.PublisherBolt;
import ebs.hmw.spouts.PublisherSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class PublisherApp {

	private static final String PUBLISHER_TOPOLOGY = "publisher_topology";

	public static void main(String[] args) {

		TopologyBuilder publisherTopology = createPublisherTopology();

		Config publisherTopologyConfig = new Config();
		publisherTopologyConfig.put(PUBS_FILE_PARAM, PUBS_FILE);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(PUBLISHER_TOPOLOGY, publisherTopologyConfig, publisherTopology.createTopology());

		try {
			Thread.sleep( 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(PUBLISHER_TOPOLOGY);
		cluster.shutdown();
	}

	private static TopologyBuilder createPublisherTopology() {
		TopologyBuilder publisherTopology = new TopologyBuilder();

		PublisherSpout publisherSpout = new PublisherSpout();
		PublisherBolt publisherBolt = new PublisherBolt();

		publisherTopology.setSpout(PUBLISHER_SPOUT_1, publisherSpout);
		publisherTopology.setBolt("bolt_id", publisherBolt).shuffleGrouping(PUBLISHER_SPOUT_1);

		return publisherTopology;
	}
}
