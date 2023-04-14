package gr.ds.unipi.noda.api.mongo.joinOperators;

import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;

/**
 * JoinOperator on equals conditions.
 * 
 * @author panakos dimitrios
 */
public class EqualJoinOperator extends JoinOperator<OperatorStrategy> {
	/**
	 * Constructor.
	 * 
	 * @param columnA
	 * @param columnB
	 */
	public EqualJoinOperator (String columnA, String columnB) {
		super(columnA, columnB);
	}

	@Override
	public OperatorStrategy getOperatorExpression () {
		return OperatorStrategy.EQUAL;
	}
}
