package ebs.hmw.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class HmwApp {

	private static final String SPOUT_ID = "source";
	private static final String SPLIT_BOLT_ID = "split_bolt";

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		PubSpout spout = new PubSpout();
		PrintBolt bolt = new PrintBolt();

		builder.setSpout(SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, bolt).shuffleGrouping(SPOUT_ID);

		Config config = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test_topology", config, builder.createTopology());

		try {
			Thread.sleep(3500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology("test_topology");
		cluster.shutdown();
	}
}
