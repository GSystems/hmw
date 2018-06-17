package ebs.hmw.util;

import ebs.hmw.model.Publication;
import ebs.hmw.model.Subscriber;
import ebs.hmw.model.Subscription;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Matcher {

	private Map<Integer, Subscriber> subscribers = new HashMap<>();

	public Map<Integer, Subscriber> getSubscribers() {
		return subscribers;
	}

	public void addSubscriber(Integer subscriberId) {
		if (subscribers.get(subscriberId) == null) {
			subscribers.put(subscriberId, new Subscriber(subscriberId, new ArrayList<>()));
		}
	}

	public void addSubscription(int subscriberId, Subscription subscription) {
		Subscriber subscriber = subscribers.get(subscriberId);

		if (!subscriber.getSubscriptions().contains(subscription)) {
			subscriber.getSubscriptions().add(subscription);
		}
	}

	public boolean matchPublication(Publication publication) {
		for (Map.Entry<Integer, Subscriber> entry : subscribers.entrySet()) {
			for (Subscription subscription : entry.getValue().getSubscriptions()) {

				if (matchPublicationToSubscription(publication, subscription)) {
					subscription.getPublications().add(publication);
					return true;
				}
			}
		}

		return false;
	}

	private boolean matchPublicationToSubscription(Publication publication, Subscription subscription) {
		if ((subscription.getCompany() == null || checkStringForMatch(publication.getCompany(), subscription.getCompany()))
				&& ((subscription.getValue() == null) || checkDoubleForMatch(publication.getValue(), subscription.getValue()))
				&& ((subscription.getVariation() == null || checkDoubleForMatch(publication.getVariation(), subscription.getVariation())))) {

			return true;
		}

		return false;
	}

	private boolean checkStringForMatch(String publicationString, Pair<String, String> subscriptionString) {
		if (publicationString.equals(subscriptionString.getLeft()) && "=".equals(subscriptionString.getRight())) {
			return true;
		}

		return false;
	}

	private boolean checkDoubleForMatch(Double publicationDouble, Pair<Double, String> subscriptionDouble) {

		if (subscriptionDouble == null) {
			return true;
		}

		if (publicationDouble.compareTo(subscriptionDouble.getLeft()) == 0
				&& (">=".equals(subscriptionDouble.getRight())
				|| ("<=".equals(subscriptionDouble.getRight())))) {

			return true;
		}

		if (publicationDouble.compareTo(subscriptionDouble.getLeft()) == -1
				&& ("<".equals(subscriptionDouble.getRight())
				|| ("<=".equals(subscriptionDouble.getRight())))) {

			return true;
		}

		if (publicationDouble.compareTo(subscriptionDouble.getLeft()) == 1
				&& (">".equals(subscriptionDouble.getRight())
				|| (">=".equals(subscriptionDouble.getRight())))) {

			return true;
		}

		return false;
	}
}
