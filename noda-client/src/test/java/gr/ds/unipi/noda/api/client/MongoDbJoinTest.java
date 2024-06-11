package gr.ds.unipi.noda.api.client;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Stopwatch;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.operators.joinOperators.EqualJoinOperator;
import gr.ds.unipi.noda.api.core.operators.joinOperators.MongoDBJoinOperatorFactory;
import gr.ds.unipi.noda.api.mongo.filterOperators.comparisonOperators.MongoDBComparisonOperatorFactory;

/**
 * Join test for mongo db.
 */
public class MongoDbJoinTest {
	/**
	 * 
	 */
	@Test
	public void testMethod () {
		NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.MongoDB().Builder("mongoadmin", "mongoadmin", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_dynamic_new")).build();
		NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("nari_dynamic_new");
		noSqlDbOperators = noSqlDbOperators.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Denmark"));
		NoSqlDbSystem noSqlDbSystem1 = NoSqlDbSystem.MongoDB().Builder("marinetimeClient", "passw0rd", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_static_new")).build();
		NoSqlDbOperators noSqlDbOperators1 = noSqlDbSystem1.operateOn("nari_static_new");
		noSqlDbOperators1 = noSqlDbOperators1.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Denmark"));
		//@format:off
		Stopwatch stopwatch = Stopwatch.createStarted();
		Dataset<Row> dataset1 = noSqlDbOperators.project("countryName", "sourcemmsi", "shipLength").join(noSqlDbOperators1.project("countryName", "shiptype"), new MongoDBJoinOperatorFactory().newOperatorEq("countryName", "countryName")).toDataframe();
		long docs = dataset1.count();
		System.out.println(String.format("Records retrieved size from dataset one is : %s.", docs));
		Dataset<Row> dataset2 =noSqlDbOperators1.join(noSqlDbOperators.project("countryName", "sourcemmsi", "shipLength"), new MongoDBJoinOperatorFactory().newOperatorEq("countryName", "countryName")).project("countryName", "shiptype").toDataframe();
		long docsReversedJoin = dataset2.count();
		dataset1.show();
		dataset2.show();
		Assert.assertEquals(docs, docsReversedJoin);
		stopwatch.stop();
		System.out.println(stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
		System.out.println(String.format("Test PASSED with records retrieved size: %s.", docsReversedJoin + docs));
		//@format:on
		noSqlDbSystem.closeConnection();
		noSqlDbSystem1.closeConnection();
	}

//	@Test
	public void testMethodLarge () {
		NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.MongoDB().Builder("mongoadmin", "mongoadmin", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_dynamic")).build();
		NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("nari_dynamic");
		noSqlDbOperators = noSqlDbOperators.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("sourcemmsi", "227705102"));
		NoSqlDbSystem noSqlDbSystem1 = NoSqlDbSystem.MongoDB().Builder("marinetimeClient", "passw0rd", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_static")).build();
		NoSqlDbOperators noSqlDbOperators1 = noSqlDbSystem1.operateOn("nari_static");
		noSqlDbOperators1 = noSqlDbOperators1.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("sourcemmsi", "227705102"));
		//@format:off
		Dataset<Row> dataset1 = noSqlDbOperators.join(noSqlDbOperators1, new MongoDBJoinOperatorFactory().newOperatorEq("sourcemmsi", "sourcemmsi")).toDataframe();
		long docs = dataset1.count();
		System.out.println(String.format("Records retrieved size from dataset one is : %s.", docs));
		Stopwatch stopwatch = Stopwatch.createStarted();
		Dataset<Row> dataset2 =noSqlDbOperators1.join(noSqlDbOperators, new MongoDBJoinOperatorFactory().newOperatorEq("sourcemmsi", "sourcemmsi")).toDataframe();
		long docsReversedJoin = dataset2.count();
//		dataset1.show();
//		dataset2.show();
		Assert.assertEquals(docs, docsReversedJoin);
		stopwatch.stop();
		System.out.println(stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
		System.out.println(String.format("Test PASSED with records retrieved size: %s.", docsReversedJoin + docs));
		//@format:on
		noSqlDbSystem.closeConnection();
		noSqlDbSystem1.closeConnection();
	}

	private static SparkSession createSparkSession (String database, String collection) {
		return SparkSession.builder().appName("Application Name").master("local").config("spark.mongodb.input.database", database).config("spark.mongodb.input.collection", collection).getOrCreate();
	}
}
