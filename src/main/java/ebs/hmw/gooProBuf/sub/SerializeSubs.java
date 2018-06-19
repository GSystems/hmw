package ebs.hmw.gooProBuf.sub;

import ebs.hmw.model.Subscription;

import ebs.hmw.gooProBuf.sub.ProtoSub.ProtoSubscription;

public class SerializeSubs {

	public static ProtoSubscription mapFieldsForSave(Subscription subscription) {

		ProtoSubscription.Builder protoSubscriptionBuilder = ProtoSubscription.newBuilder();

		protoSubscriptionBuilder.setCompany(subscription.getCompany().getLeft());
		protoSubscriptionBuilder.setCompanySign(subscription.getCompany().getRight());
		protoSubscriptionBuilder.setId(subscription.getId());
		protoSubscriptionBuilder.setValue(subscription.getValue().getLeft());
		protoSubscriptionBuilder.setValueSign(subscription.getValue().getRight());
		protoSubscriptionBuilder.setVariation(subscription.getVariation().getLeft());
		protoSubscriptionBuilder.setVariationSign(subscription.getVariation().getRight());

		return protoSubscriptionBuilder.build();
	}
}
