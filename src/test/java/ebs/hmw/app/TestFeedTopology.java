package ebs.hmw.app;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;
import org.junit.Test;

public class TestFeedTopology {

	@Test
	public void test() {

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);

//		LocalCluster pubCluster = new LocalCluster();
//		pubCluster.submitTopology("test", conf, PublisherApp.buildPubTopology());
//		Utils.sleep(1000);
//		pubCluster.deactivate("test");
//		pubCluster.killTopology("test");
//		pubCluster.shutdown();
//
//		LocalCluster subCluster = new LocalCluster();
//		subCluster.submitTopology("test", conf, PublisherApp.buildSubTopology());
//		Utils.sleep(1000);
//		subCluster.deactivate("test");
//		subCluster.killTopology("test");
//		subCluster.shutdown();
	}
}