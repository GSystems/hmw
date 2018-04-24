package ebs.hmw.app;

import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.util.PrintingHelperBolt;
import ebs.hmw.spouts.SubscriptionSpout;
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
		SubscriptionSpout rawSubscriptions = new SubscriptionSpout();

		PrintingHelperBolt pubPrinterBolt = new PrintingHelperBolt(RAW_PUBLICATIONS_KEYWD);
		PrintingHelperBolt subPrinterBolt = new PrintingHelperBolt(RAW_SUBSCRIPTIONS_KEYWD);

		builder.setSpout(RAW_PUB_ID, rawPublications);
		builder.setSpout(RAW_SUB_ID, rawSubscriptions);

		builder.setBolt(RAW_PUB_PRINTER_BOLT_ID, pubPrinterBolt).shuffleGrouping(RAW_PUB_ID);
		builder.setBolt(RAW_SUB_PRINTER_BOLT_ID, subPrinterBolt).shuffleGrouping(RAW_SUB_ID);

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
