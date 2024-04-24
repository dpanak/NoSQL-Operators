//
// CustomJoinTest.java
//
// Interamerican 2023, all rights reserved.
//
package gr.ds.unipi.noda.api.client;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.SparkSession;

import com.google.common.base.Stopwatch;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.operators.joinOperators.EqualJoinOperator;
import gr.ds.unipi.noda.api.mongo.filterOperators.comparisonOperators.MongoDBComparisonOperatorFactory;

/**
 * 
 */
public class CustomJoinTest {
	/**
	 * @param args
	 */
	public static void main (String[] args) {
		NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.MongoDB().Builder("admin01", "pass", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_dynamic_new")).build();
		NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("nari_dynamic_new");
		noSqlDbOperators = noSqlDbOperators.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Belgium"));
		NoSqlDbSystem noSqlDbSystem1 = NoSqlDbSystem.MongoDB().Builder("admin01", "pass", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_static_new")).build();
		NoSqlDbOperators noSqlDbOperators1 = noSqlDbSystem1.operateOn("nari_static_new");
		noSqlDbOperators1 = noSqlDbOperators1.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Belgium"));
		Stopwatch stopwatch = Stopwatch.createStarted();
		noSqlDbOperators.joinToJSON(noSqlDbOperators1, new EqualJoinOperator("countryName", "countryName"));
		stopwatch.stop();
		System.out.println(stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
		noSqlDbSystem.closeConnection();
		noSqlDbSystem1.closeConnection();
	}

	private static SparkSession createSparkSession (String database, String collection) {
		return SparkSession.builder().appName("Application Name").master("local").config("spark.mongodb.input.database", database).config("spark.mongodb.input.collection", collection).getOrCreate();
	}
}
