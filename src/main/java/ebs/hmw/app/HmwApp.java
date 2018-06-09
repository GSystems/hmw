package ebs.hmw.app;

import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.spouts.SubscriptionSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class HmwApp {

	private static final String CURRENT_TOPOLOGY = "test_topology";

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		PublicationSpout rawPublications = new PublicationSpout();
		SubscriptionSpout rawSubscriptions = new SubscriptionSpout();

//		PubFilterBolt pubFilterBolt = new PubFilterBolt();

		builder.setSpout(RAW_PUB_1_SPOUT_ID, rawPublications);
		builder.setSpout(RAW_SUB_1_SPOUT_ID, rawSubscriptions);

//		builder.setBolt(PUB_FILTER_ID_1, pubFilterBolt).shuffleGrouping(RAW_PUB_1_SPOUT_ID);
//		builder.setBolt(PUB_FILTER_ID_2, pubFilterBolt).shuffleGrouping(RAW_SUB_1_SPOUT_ID);

		Config config = new Config();
		config.put(PUBS_FILE_PARAM, PUBS_FILE);
		config.put(SUBS_FILE_PARAM, SUBS_FILE);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(CURRENT_TOPOLOGY, config, builder.createTopology());

		try {
			Thread.sleep(12000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(CURRENT_TOPOLOGY);
		cluster.shutdown();
	}
}
