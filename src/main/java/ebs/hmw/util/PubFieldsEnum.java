package ebs.hmw.util;

public enum PubFieldsEnum {

    COMPANY_FIELD("company"),
    VALUE_FIELD("value"),
    DROP_FIELD("drop"),
    VARIATION_FIELD("variation"),
    DATE_FIELD("date");

    private String code;

    PubFieldsEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
