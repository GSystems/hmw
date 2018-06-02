package ebs.hmw.util;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrinterBolt extends BaseRichBolt {

	private String incomingFlux;

	public PrinterBolt(String incomingFlux) {
		this.incomingFlux = incomingFlux;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println(tuple.getStringByField(incomingFlux));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}