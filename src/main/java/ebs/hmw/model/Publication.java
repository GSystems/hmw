package ebs.hmw.model;

import lombok.Data;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.io.Serializable;

@Data
public class Publication implements Serializable {

	private String company;
	private Double value;
	private Double variation;
	private Double drop;
	private DateTime date;
}
