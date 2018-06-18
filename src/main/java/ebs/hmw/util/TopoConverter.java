package ebs.hmw.util;

import ebs.hmw.model.Publication;
import ebs.hmw.model.Subscription;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.shade.com.google.common.base.CharMatcher;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;

public class TopoConverter {

	public static Object convertToType(Object input) {

		Integer integer = Integer.MIN_VALUE;
		Double number = Double.MIN_VALUE;
		DateTime date = null;
		String string = null;

		try {
			integer = Integer.valueOf(input.toString());
		} catch (NumberFormatException e) {
			try {
				number = Double.valueOf(input.toString());
			} catch (NumberFormatException f) {
				try {
					date = convertStringToDate(input);
				} catch (Exception g) {
					string = input.toString();
				}
			}
		}

		if (integer > Integer.MIN_VALUE) {
			return integer;
		} else if (number > Double.MIN_VALUE) {
			return number;
		} else if (date != null) {
			return date;
		} else if (string != null) {
			return string;
		}

		return null;
	}

	public static Publication extractPubFromLine(String input) {
		String[] words = input.split(";");
		Publication publication = new Publication();

		for (String word : words) {
			String[] splited = word.split(",");

			String aux = StringUtils.EMPTY;
			int count = 1;

			for (String value : splited) {
				String field = removeUnwantedChars(value);

				if (count % 2 == 0) {
					mapFieldForPublication(publication, aux, field);
				}

				aux = field;
				count++;
			}
		}

		return publication;
	}

	private static void mapFieldForPublication(Publication publication, String property, String value) {
		switch (property) {
			case "company":
				publication.setCompany(value);
				break;
			case "value":
				publication.setValue(Double.valueOf(value));
				break;
			case "variation":
				publication.setVariation(Double.valueOf(value));
				break;
			case "drop":
				publication.setDrop(Double.valueOf(value));
				break;
			case "date":
				publication.setDate(convertStringToDate(value));
				break;
		}
	}

	public static Subscription extractSubFromLine(String input) {
		String[] words = input.split(";");
		Subscription subscription = new Subscription();

		for (String word : words) {
			String[] splited = word.split(",");

			String field = removeUnwantedChars(splited[0]);
			String operator = splited[1];
			String value = removeUnwantedChars(splited[2]);

			mapFieldForSubscription(subscription, field, operator, value);
		}

		return subscription;
	}

	private static void mapFieldForSubscription(Subscription subscription, String field, String operator, String value) {
		switch (field) {
			case "company":
				subscription.setCompany(Pair.of(value, operator));
				break;
			case "value":
				subscription.setValue(Pair.of(Double.valueOf(value), operator));
				break;
			case "variation":
				subscription.setVariation(Pair.of(Double.valueOf(value), operator));
				break;
		}
	}

	private static String removeUnwantedChars(String input) {
		CharMatcher alfaNum =
				CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('A', 'Z'))
						.or(CharMatcher.inRange('0', '9')).or(CharMatcher.is('.')).precomputed();

		return alfaNum.retainFrom(input);
	}

	private static DateTime convertStringToDate(Object input) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(GeneralConstants.DATE_FORMAT);

		DateTime date = formatter.parseDateTime(input.toString());

		return date;
	}
}
