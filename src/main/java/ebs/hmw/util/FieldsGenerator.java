package ebs.hmw.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class FieldsGenerator {


	public static String generateValueFromArray(String[] array) {
		int randomNum = ThreadLocalRandom.current().nextInt(0, array.length);

		return array[randomNum];
	}

	public static Double generateDoubleFromRange(double minRange, double maxRange) {
		Random random = new Random();
		Double randomValue = minRange + (maxRange - minRange) * random.nextDouble();

		return round(randomValue, 2);
	}

	public static double round(double value, int places) {
		if (places < 0) {
			throw new IllegalArgumentException();
		}

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);

		return bd.doubleValue();
	}
}
