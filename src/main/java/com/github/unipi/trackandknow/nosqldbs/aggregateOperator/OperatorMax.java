package com.github.unipi.trackandknow.nosqldbs.aggregateOperator;

public class OperatorMax extends AggregateOperator {

    private OperatorMax(String fieldName){
        super(fieldName, "max("+fieldName+")");
    }

//    @Override
//    public String getJsonString() {
//        return alias + ": { $max: " + "\"" + "$"+ fieldName +"\""+  " }";
//    }

    public static OperatorMax newOperatorMax(String fieldName){
        return new OperatorMax(fieldName);
    }

    @Override
    protected String getOperatorJsonField() {
        return "max";
    }
}