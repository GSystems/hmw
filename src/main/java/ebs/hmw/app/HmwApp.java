package ebs.hmw.app;

import ebs.hmw.restrictions.TotalMessagesCount;
import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.restrictions.PrintingHelper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class HmwApp {

	private static final String CURRENT_TOPOLOGY = "test_topology";

	public static void main(String[] args) {

//		ProjectProperties projectProperties = ProjectProperties.getInstance();
//		projectProperties.loadProperties();

		TopologyBuilder builder = new TopologyBuilder();

		PublicationSpout rawPublications = new PublicationSpout();
		TotalMessagesCount messagesCountBolt = new TotalMessagesCount(RAW_PUBLICATIONS_KEYWD, "messages");
		PrintingHelper printerBolt = new PrintingHelper("messages");

		builder.setSpout(RAW_PUBLICATIONS_SOURCE, rawPublications);
		builder.setBolt("total_count_id", messagesCountBolt).shuffleGrouping(RAW_PUBLICATIONS_SOURCE);
		builder.setBolt(PRINTER_BOLT_ID, printerBolt).globalGrouping("total_count_id");

		Config config = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(CURRENT_TOPOLOGY, config, builder.createTopology());

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.killTopology(CURRENT_TOPOLOGY);
		cluster.shutdown();
	}
}
