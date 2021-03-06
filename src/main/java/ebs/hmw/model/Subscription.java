package ebs.hmw.model;


import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

@Data
@EqualsAndHashCode
public class Subscription {

	private Integer id;

	private Pair<String, String> company;
	private Pair<Double, String> value;
	private Pair<Double, String> variation;

	private List<Publication> publications;
}
