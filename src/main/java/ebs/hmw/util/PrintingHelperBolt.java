package ebs.hmw.util;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrintingHelperBolt extends BaseRichBolt {

	private List<String> incomingWords;
	private String incomingFlux;
	private String outputFile;

	public PrintingHelperBolt(String incomingFlux, String outputFile) {
		this.incomingFlux = incomingFlux;
		this.outputFile = outputFile;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		incomingWords = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		incomingWords.add(tuple.getStringByField(incomingFlux));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}

	public void cleanup() {

	    try {
			FileWriter writer = new FileWriter(outputFile);
			for (String word : incomingWords) {
				writer.write(word + " ");
				writer.flush();
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
