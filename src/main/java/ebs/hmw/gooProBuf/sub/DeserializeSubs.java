package ebs.hmw.gooProBuf.sub;

import ebs.hmw.gooProBuf.sub.ProtoSub.ProtoSubscription;
import ebs.hmw.gooProBuf.sub.ProtoSub.ProtoSubscriptionList;

import ebs.hmw.model.Subscription;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeserializeSubs {

	private static final String FILE_NAME = "subscriptionList";

	List<Subscription> readPublications(ProtoSubscriptionList protoSubscriptionList) {

		List<Subscription> subscriptions = new ArrayList<>();
		List<ProtoSubscription> protoSubscriptions = new ArrayList<>();

		try {
			protoSubscriptions = ProtoSubscriptionList.parseFrom(new FileInputStream(FILE_NAME)).getSubscriptionList();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (ProtoSubscription protoSubscription : protoSubscriptions) {
			subscriptions.add(mapFields(protoSubscription));
		}


		return subscriptions;
	}

	private Subscription mapFields(ProtoSubscription protoSubscription) {
		Subscription subscription = new Subscription();

		subscription.setId(protoSubscription.getId());
		subscription.setCompany(Pair.of(protoSubscription.getCompany(), protoSubscription.getCompanySign()));
		subscription.setValue(Pair.of(protoSubscription.getValue(), protoSubscription.getValueSign()));
		subscription.setVariation(Pair.of(protoSubscription.getVariation(), protoSubscription.getVariationSign()));

		return subscription;
	}
}
