package ebs.lab.Lab01;

import ebs.hmw.test.PubSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {
	private static final String SPOUT_ID = "source_text_spout";
	private static final String SPLIT_BOLT_ID = "split_bolt";
	private static final String COUNT_BOLT_ID = "count_bolt";
	private static final String TERMINAL_BOLT_ID = "terminal_bolt";

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		PubSpout pubSpout = new PubSpout();
		SplitTextBolt splitbolt = new SplitTextBolt();
		WordCountBolt countbolt = new WordCountBolt();
		TerminalBolt terminalbolt = new TerminalBolt();

		builder.setSpout(SPOUT_ID, pubSpout);
		builder.setBolt(SPLIT_BOLT_ID, splitbolt).shuffleGrouping(SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, countbolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(TERMINAL_BOLT_ID, terminalbolt).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("count_topology", config, builder.createTopology());

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology("count_topology");
		cluster.shutdown();

	}
}