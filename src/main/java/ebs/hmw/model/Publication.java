package ebs.hmw.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.io.Serializable;

@Data
@EqualsAndHashCode
public class Publication implements Serializable {

	private Integer publicationId;
	private String company;
	private Double value;
	private Double variation;
	private Double drop;
	private DateTime date;

	private DateTime sendTime;
	private DateTime receivedTime;
}
