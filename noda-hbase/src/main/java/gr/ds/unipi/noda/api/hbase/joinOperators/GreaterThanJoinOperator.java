package gr.ds.unipi.noda.api.hbase.joinOperators;

import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;

/**
 * Greater Than Join Expression.
 * 
 * @author panakos dimitrios
 */
public class GreaterThanJoinOperator extends JoinOperator<OperatorStrategy> {
	/**
	 * Constructor.
	 * 
	 * @param columnA
	 * @param columnB
	 */
	public GreaterThanJoinOperator (String columnA, String columnB) {
		super(columnA, columnB);
	}

	/**
	 * @see gr.ds.unipi.noda.api.core.operators.Operator#getOperatorExpression()
	 */
	@Override
	public OperatorStrategy getOperatorExpression () {
		return OperatorStrategy.GREATER_THAN;
	}
}
