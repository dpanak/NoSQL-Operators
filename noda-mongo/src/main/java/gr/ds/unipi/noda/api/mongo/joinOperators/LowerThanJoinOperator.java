package gr.ds.unipi.noda.api.mongo.joinOperators;

import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;

/**
 * Lower than operator expression.
 * 
 * @author panakos dimitrios
 */
public class LowerThanJoinOperator extends JoinOperator<OperatorStrategy> {
	/**
	 * Constructor.
	 * 
	 * @param columnA
	 * @param columnB
	 */
	public LowerThanJoinOperator (String columnA, String columnB) {
		super(columnA, columnB);
	}

	/**
	 * @see gr.ds.unipi.noda.api.core.operators.Operator#getOperatorExpression()
	 */
	@Override
	public OperatorStrategy getOperatorExpression () {
		return OperatorStrategy.LESS_THAN;
	}
}
