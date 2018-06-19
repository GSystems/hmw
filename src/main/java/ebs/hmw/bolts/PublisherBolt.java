package ebs.hmw.bolts;

import ebs.hmw.gooProBuf.pub.ProtoPub;
import ebs.hmw.gooProBuf.pub.SerializePubs;
import ebs.hmw.model.Publication;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import static ebs.hmw.util.GeneralConstants.PUB_SPOUT_OUT;

public class PublisherBolt extends BaseRichBolt {

	private OutputCollector collector;
	private static final String FILE_NAME = "publicationList";

	@Override public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override public void execute(Tuple tuple) {
		Publication publication = (Publication) tuple.getValueByField(PUB_SPOUT_OUT);

		try {
			FileOutputStream output = new FileOutputStream(FILE_NAME, true);

			ProtoPub.ProtoPublication protoPublication = SerializePubs.mapFieldsForSave(publication);
			protoPublication.writeTo(output);

			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
