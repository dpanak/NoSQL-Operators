package gr.ds.unipi.noda.api.core.operators.joinOperators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbRecord;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbResults;

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

	/**
	 * Joins the two results data with hash join algorithm.
	 * 
	 * @param left
	 * @param right
	 * @param jo
	 * @return The String JSON of the results.
	 */
	@SuppressWarnings ("rawtypes")
	public String makeJoin (NoSqlDbOperators left, NoSqlDbOperators right, JoinOperator jo) {
		// Fetch records containing the necessary columns
		List<NoSqlDbRecord> firstRecords = getResultsWithColumn(left, jo.getColumnAName());
		List<NoSqlDbRecord> secondRecords = getResultsWithColumn(right, jo.getColumnBName());
		Map<String, List<NoSqlDbRecord>> hashTable = new HashMap<>();
		for (NoSqlDbRecord record : firstRecords) {
			String key = record.getString(jo.getColumnAName());
			hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
		}
		List<NoSqlDbRecord> results = new ArrayList<>();
		for (NoSqlDbRecord record : secondRecords) {
			String key = record.getString(jo.getColumnBName());
			if (hashTable.containsKey(key)) {
				for (NoSqlDbRecord matchingRecord : hashTable.get(key)) {
					results.add(matchingRecord);
					results.add(record);
				}
			}
		}
		return new Gson().toJson(results);
	}

	@SuppressWarnings ("rawtypes")
	private List<NoSqlDbRecord> getResultsWithColumn (NoSqlDbOperators noSqlDbOperators, String colName) {
		List<NoSqlDbRecord> list = new ArrayList<>();
		NoSqlDbResults results = noSqlDbOperators.getResults();
		while (results.hasNextRecord()) {
			NoSqlDbRecord record = results.getRecord();
			if (record.containsField(colName)) {
				list.add(record);
			}
		}
		return list;
	}
}
