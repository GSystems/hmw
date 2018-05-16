package ebs.hmw.util;

import static ebs.hmw.util.PubSubGeneratorConfiguration.*;

public enum SubFieldsEnum {

    COMPANY_FIELD("company", SUB_COMPANY_FIELD_PRESENCE),
    VALUE_FIELD("value", SUB_VALUE_FIELD_PRESENCE),
    VARIATION_FIELD("variation", SUB_VARIATION_FIELD_PRESENCE);

    private String code;
    private Double perc;

    SubFieldsEnum(String code, Double perc) {
        this.code = code;
        this.perc = perc;
    }

    public String getCode() {
        return code;
    }

    public Double getPerc() {
        return perc;
    }
}
