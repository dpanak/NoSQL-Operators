package gr.ds.unipi.noda.api.hbase.joinOperators;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;

import scala.collection.JavaConverters;

/**
 * Enumeration of possible operators.
 * 
 * @author panakos dimitrios
 */
public enum OperatorStrategy {
	/**
	 * 
	 */
	EQUAL {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, JavaConverters.asScalaIteratorConverter(Arrays.asList(jo.getColumnAName(), jo.getColumnBName()).iterator()).asScala().toSeq(), condition);
		}
	},
	/**
	 *
	 */
	NOT_EQUAL {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, leftDf.col(jo.getColumnAName()).$eq$bang$eq(rightDf.col(jo.getColumnBName())), condition);
		}
	},
	/**
	*
	*/
	GREATER_THAN {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, leftDf.col(jo.getColumnAName()).$greater(rightDf.col(jo.getColumnBName())), condition);
		}
	},
	/**
	*
	*/
	GREATER_THAN_OR_EQUAL {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, leftDf.col(jo.getColumnAName()).$greater$eq(rightDf.col(jo.getColumnBName())), condition);
		}
	},
	/**
	*
	*/
	LESS_THAN {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, leftDf.col(jo.getColumnAName()).$less(rightDf.col(jo.getColumnBName())), condition);
		}
	},
	/**
	*
	*/
	LESS_THAN_OR_EQUAL {
		@Override
		public Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition) {
			return leftDf.join(rightDf, leftDf.col(jo.getColumnAName()).$less$eq(rightDf.col(jo.getColumnBName())), condition);
		}
	};

	/**
	 * Finds the correct member base on name.
	 * 
	 * @param currentOperator
	 *        The operator for join.
	 * @return The OperatorStrategy member to handle join.
	 * @throws IllegalStateException
	 *         in case of operator not found.
	 */
	public static OperatorStrategy find (OperatorStrategy currentOperator) {
		for (OperatorStrategy operator : OperatorStrategy.values()) {
			if (operator.name().equals(currentOperator.name())) {
				return operator;
			}
		}
		throw new IllegalStateException("Operator not found. Check the method parameter.");
	}

	/**
	 * Handles the join correctly based on each operator.
	 * 
	 * @param leftDf
	 *        The left Dataset
	 * @param rightDf
	 *        The right Dataset
	 * @param jo
	 *        The JoinOperator
	 * @return The result of the join to a Dataset.
	 */
	public abstract Dataset<Row> makeJoin (Dataset<Row> leftDf, Dataset<Row> rightDf, JoinOperator jo, String condition);
}
