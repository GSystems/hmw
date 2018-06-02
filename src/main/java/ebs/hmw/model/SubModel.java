package ebs.hmw.model;


import org.apache.commons.lang3.tuple.Pair;

public class SubModel {

	private Pair<String, String> fieldValue;
	private String operator;

	public SubModel(Pair<String, String> fieldValue, String operator) {
		this.fieldValue = fieldValue;
		this.operator = operator;
	}

	public Pair<String, String> getFieldValue() {
		return fieldValue;
	}

	public String getOperator() {
		return operator;
	}
}
