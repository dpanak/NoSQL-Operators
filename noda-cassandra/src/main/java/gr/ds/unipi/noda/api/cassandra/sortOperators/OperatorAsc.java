package gr.ds.unipi.noda.api.cassandra.sortOperators;

final class OperatorAsc extends SortOperator {

    private OperatorAsc(String fieldName) {
        super(fieldName);
    }

    public static SortOperator newOperatorAsc(String fieldName) {
        return new OperatorAsc(fieldName);
    }

}
