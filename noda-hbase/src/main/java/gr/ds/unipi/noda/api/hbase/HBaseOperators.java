package gr.ds.unipi.noda.api.hbase;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import gr.ds.unipi.noda.api.core.nosqldb.NoSQLExpression;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbResults;
import gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;
import gr.ds.unipi.noda.api.core.operators.joinOperators.OperatorStrategy;
import gr.ds.unipi.noda.api.core.operators.sortOperators.SortOperator;

final class HBaseOperators extends NoSqlDbOperators {

    private final HBaseConnectionManager hbaseConnectionManager = HBaseConnectionManager.getInstance();

    private final Scan scan;
    private final FilterList filterList;
    private final FilterList projectionFilterList;    
    private NoSqlDbOperators otherNoSqlOperator;
	private boolean needJoin;
	private JoinOperator jo;


    private HBaseOperators(NoSqlDbConnector connector, String s, SparkSession sparkSession) {
        super(connector, s, sparkSession);
        scan = new Scan();
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        projectionFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    }

    private HBaseOperators (HBaseOperators hbaseOperators, Scan scan, FilterList filterList, FilterList projectionFilterList, NoSqlDbOperators noSqlDbOperators, JoinOperator jo, boolean needJoin) {
		super(hbaseOperators.getNoSqlDbConnector(), hbaseOperators.getDataCollection(), hbaseOperators.getSparkSession());
		this.scan = scan;
		this.filterList = filterList;
		this.projectionFilterList = projectionFilterList;
		this.otherNoSqlOperator = noSqlDbOperators;
		this.jo = jo;
		this.needJoin = needJoin;
	}

    private FilterList getFilterListCopy() {
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.getFilters().forEach(fl::addFilter);
        return fl;
    }

    private FilterList getProjectionFilterListCopy() {
        FilterList pl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        projectionFilterList.getFilters().forEach(pl::addFilter);
        return pl;
    }

    static HBaseOperators newHBaseOperators(NoSqlDbConnector connector, String s, SparkSession sparkSession) {
        return new HBaseOperators(connector, s, sparkSession);
    }

    @Override
    public NoSqlDbOperators filter(FilterOperator filterOperator, FilterOperator... filterOperators) {

        FilterList fl = getFilterListCopy();

        fl.addFilter((Filter) filterOperator.getOperatorExpression());
        for (FilterOperator fop : filterOperators) {
            fl.addFilter((Filter) fop.getOperatorExpression());
        }
        try {
        	return new HBaseOperators(this, new Scan(scan), fl, getProjectionFilterListCopy(), null, null, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public NoSqlDbOperators groupBy(String fieldName, String... fieldNames) {
        return null;
    }

    @Override
    public NoSqlDbOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        return null;
    }

    @Override
    public NoSqlDbOperators distinct(String fieldName) {
        return null;
    }

    @Override
    public void printScreen() {
        Table table = null;
        ResultScanner resultScanner = null;

        filterList.addFilter(projectionFilterList);
        scan.setFilter(filterList);

        try {

            table = hbaseConnectionManager.getConnection(getNoSqlDbConnector()).getTable(TableName.valueOf(getDataCollection()));
            resultScanner = table.getScanner(scan);
            resultScanner.forEach(System.out::println);

            resultScanner.close();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<Double> max(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> min(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> avg(String fieldName) {
        return Optional.empty();
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public NoSqlDbOperators sort(SortOperator sortOperator, SortOperator... sortingOperators) {
        return null;
    }

    @Override
    public NoSqlDbOperators limit(int limit) {

        FilterList fl = getFilterListCopy();

        fl.addFilter(new PageFilter(limit));
        try {
            return new HBaseOperators(this, new Scan(scan), fl, getProjectionFilterListCopy(), null, null, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void scanProjection(String fieldName, FilterList pl) {
        String[] names = fieldName.split(":");

        if (names.length == 1) {
            pl.addFilter(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(names[0]))));
        } else if (names.length == 2) {

            FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            flist.addFilter(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(names[0]))));
            flist.addFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(names[1]))));

            pl.addFilter(flist);

        } else {
            try {
                throw new Exception("");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public NoSqlDbOperators project(String fieldName, String... fieldNames) {

        FilterList pl = getProjectionFilterListCopy();

        scanProjection(fieldName, pl);

        for (int i = 0; i < fieldNames.length; i++) {
            scanProjection(fieldNames[i], pl);
        }

        try {
        	return new HBaseOperators(this, new Scan(scan), getFilterListCopy(), pl, null, null, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
	public Dataset<Row> toDataframe () {
		filterList.addFilter(projectionFilterList);
		scan.setFilter(filterList);
		Configuration conf = hbaseConnectionManager.getConfiguration(getNoSqlDbConnector());
		conf.set(TableInputFormat.INPUT_TABLE, getDataCollection());
		try {
			conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = JavaSparkContext.fromSparkContext(getSparkSession().sparkContext()).newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		Dataset<Row> df = getSparkSession().sqlContext().createDataFrame(javaPairRDD.values(), Result.class);
		df = df.select(new Column("row"), org.apache.spark.sql.functions.explode_outer(new Column("noVersionMap"))).select(new Column("row"), new Column("key").as("columnFamily"), org.apache.spark.sql.functions.explode_outer(new Column("value"))).select(new Column("row"), new Column("columnFamily"), new Column("key").as("columnQualifier"), new Column("value")).withColumn("column", org.apache.spark.sql.functions.concat(new Column("columnFamily"), org.apache.spark.sql.functions.lit(":"), new Column("columnQualifier"))).drop("columnFamily", "columnQualifier").groupBy("row").pivot("column").agg(org.apache.spark.sql.functions.first(new Column("value")));
		NoSQLExpression.INSTANCE.setExpression(filterList.toString());
		return needJoin ? join(df) : df;
	}

	@Override
	public NoSqlDbOperators join (NoSqlDbOperators noSqlDbOperators, JoinOperator jo) {
		return new HBaseOperators(this, this.scan, getFilterListCopy(), getProjectionFilterListCopy(), noSqlDbOperators, jo, true);
	}

	/**
	 * Common logic for join a Dataset.
	 * 
	 * @return The joined Dataset or #toDataframe directly.
	 */
	private Dataset<Row> join (Dataset<Row> thizz) {
		return OperatorStrategy.find(OperatorStrategy.class.cast(jo.getOperatorExpression())).makeJoin(thizz, otherNoSqlOperator.toDataframe(), jo, jo.getJoinCondition().name());
	}

    @Override
    public NoSqlDbResults getResults() {
        return null;
    }

	/**
	 * @see gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators#joinToJSON(gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators, gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator)
	 */
	@Override
	public String joinToJSON (NoSqlDbOperators noSqlDbOperators, JoinOperator jo) {
		// TODO Implement this method.
		return null;
	}
}
