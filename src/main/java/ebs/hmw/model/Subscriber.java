package ebs.hmw.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Subscriber {

	private Integer id;
	private List<Subscription> subscriptions;
}
