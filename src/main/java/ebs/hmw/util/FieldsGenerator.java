package ebs.hmw.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class FieldsGenerator {


    public static String generateFieldFromArray(String[] array) {
        int randomNum = ThreadLocalRandom.current().nextInt(0, array.length);

        return array[randomNum];
    }

    public static Double generateDoubleFromRange(double minRange, double maxRange) {
        Random random = new Random();
        Double randomValue = minRange + (maxRange - minRange) * random.nextDouble();

        return randomValue;
    }
}
