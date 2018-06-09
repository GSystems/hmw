package ebs.hmw.model;


import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.shade.org.joda.time.DateTime;

@Data
public class Subscription {

	private Pair<String, String> company;
	private Pair<Double, String> value;
	private Pair<Double, String> variation;
}
