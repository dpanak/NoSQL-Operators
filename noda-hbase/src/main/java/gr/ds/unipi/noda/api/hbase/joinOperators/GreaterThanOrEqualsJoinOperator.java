package gr.ds.unipi.noda.api.hbase.joinOperators;

import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;

/**
 * Greater than or equals expression.
 * 
 * @author panakos dimitrios
 */
public class GreaterThanOrEqualsJoinOperator extends JoinOperator<OperatorStrategy> {
	/**
	 * Constructor.
	 * 
	 * @param columnA
	 * @param columnB
	 */
	public GreaterThanOrEqualsJoinOperator (String columnA, String columnB) {
		super(columnA, columnB);
	}

	/**
	 * @see gr.ds.unipi.noda.api.core.operators.Operator#getOperatorExpression()
	 */
	@Override
	public OperatorStrategy getOperatorExpression () {
		return OperatorStrategy.GREATER_THAN_OR_EQUAL;
	}
}
