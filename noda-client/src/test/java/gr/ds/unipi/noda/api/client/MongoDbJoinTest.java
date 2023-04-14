package gr.ds.unipi.noda.api.client;

import static org.testng.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.mongo.filterOperators.comparisonOperators.MongoDBComparisonOperatorFactory;
import gr.ds.unipi.noda.api.mongo.joinOperators.MongoDBJoinOperatorFactory;

/**
 * Join test for mongo db.
 */
public class MongoDbJoinTest {
	/**
	 * 
	 */
	@Test
	public void testMethod () {
		NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.MongoDB().Builder("marinetimeClient", "passw0rd", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_dynamic_new")).build();
		NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("nari_dynamic_new");
		noSqlDbOperators = noSqlDbOperators.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Belgium"));
		NoSqlDbSystem noSqlDbSystem1 = NoSqlDbSystem.MongoDB().Builder("marinetimeClient", "passw0rd", "marinetime").host("localhost").sparkSession(createSparkSession("marinetime", "nari_static_new")).build();
		NoSqlDbOperators noSqlDbOperators1 = noSqlDbSystem1.operateOn("nari_static_new");
		noSqlDbOperators1 = noSqlDbOperators1.filter(new MongoDBComparisonOperatorFactory().newOperatorEq("countryName", "Belgium"));
		//@format:off
		Dataset<Row> dataset1 = noSqlDbOperators.project("countryName", "sourcemmsi", "shipLength").join(noSqlDbOperators1.project("countryName", "shiptype"), new MongoDBJoinOperatorFactory().newOperatorEq("countryName", "countryName")).toDataframe();
		long docs = dataset1.count();
		Dataset<Row> dataset2 =noSqlDbOperators1.join(noSqlDbOperators.project("countryName", "sourcemmsi", "shipLength"), new MongoDBJoinOperatorFactory().newOperatorEq("countryName", "countryName")).project("countryName", "shiptype").toDataframe();
		long docsReversedJoin = dataset2.count();
		assertEquals(docs, docsReversedJoin);
		dataset1.show();
		dataset2.show();
		//@format:on
		noSqlDbSystem.closeConnection();
		noSqlDbSystem1.closeConnection();
	}

	private static SparkSession createSparkSession (String database, String collection) {
		return SparkSession.builder().appName("Application Name").master("local").config("spark.mongodb.input.database", database).config("spark.mongodb.input.collection", collection).getOrCreate();
	}
}
