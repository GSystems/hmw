package ebs.hmw.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TopoConverter {

	public static Object convertToType(Object input) {

		Integer integer = Integer.MIN_VALUE;
		Double number = Double.MIN_VALUE;
		Date date = null;
		String string = null;

		try {
			integer = Integer.valueOf(input.toString());
		} catch (NumberFormatException e) {
			try {
				number = Double.valueOf(input.toString());
			} catch (NumberFormatException f) {
				try {
					date = convertStringToDate(input);
				} catch (ParseException g) {
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

	private static Date convertStringToDate(Object input) throws ParseException {
		SimpleDateFormat tempDate = new SimpleDateFormat(GeneralConstants.DATE_FORMAT);

		return tempDate.parse(input.toString());
	}
}
