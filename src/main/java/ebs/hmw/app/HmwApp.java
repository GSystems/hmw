package ebs.hmw.app;

import ebs.hmw.bolts.PubFilterBolt;
import ebs.hmw.spouts.PublicationSpout;
import ebs.hmw.util.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static ebs.hmw.util.GeneralConstants.*;

public class HmwApp {

	private static final String CURRENT_TOPOLOGY = "test_topology";

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		PublicationSpout rawPublications = new PublicationSpout();
		PubFilterBolt pubFilterBolt = new PubFilterBolt();
		PrinterBolt printerBolt = new PrinterBolt(PRINTER_INPUT_KEYWD);

		builder.setSpout(RAW_PUB_ID_1, rawPublications);

		builder.setBolt(RAW_PUB_ID_2, pubFilterBolt).shuffleGrouping(RAW_PUB_ID_1);
		builder.setBolt(RAW_PUB_1_PRINTER_BOLT_ID, printerBolt).shuffleGrouping(RAW_PUB_ID_2);

		Config config = new Config();
		config.put(PUBS_FILE_PARAM, PUBS_OUT_FILE);

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
