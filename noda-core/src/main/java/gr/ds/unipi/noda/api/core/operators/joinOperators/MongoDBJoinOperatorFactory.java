package gr.ds.unipi.noda.api.core.operators.joinOperators;

/**
 * Factory for join operators.
 */
public class MongoDBJoinOperatorFactory extends BaseJoinOperatorFactory {
	@Override
	public JoinOperator<OperatorStrategy> newOperatorGte (String columnA, String columnB) {
		return new GreaterThanOrEqualsJoinOperator(columnA, columnB);
	}

	@Override
	public JoinOperator<OperatorStrategy> newOperatorGt (String columnA, String columnB) {
		return new GreaterThanJoinOperator(columnA, columnB);
	}

	@Override
	public JoinOperator<OperatorStrategy> newOperatorLte (String columnA, String columnB) {
		return new LowerThanOrEqualsJoinOperator(columnA, columnB);
	}

	@Override
	public JoinOperator<OperatorStrategy> newOperatorLt (String columnA, String columnB) {
		return new LowerThanJoinOperator(columnA, columnB);
	}

	@Override
	public JoinOperator<OperatorStrategy> newOperatorEq (String columnA, String columnB) {
		return new EqualJoinOperator(columnA, columnB);
	}

	@Override
	public JoinOperator<OperatorStrategy> newOperatorNe (String columnA, String columnB) {
		return new NotEqualJoinOperator(columnA, columnB);
	}
}
