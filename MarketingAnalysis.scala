import scala.math._;
import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.functions.trim;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.{Column => col};
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.types.{StringType,IntegerType,StructType,StructField};

// 1 Load data and create Spark dataframe, Spark RDD : 
// you are not supposed to bring any modifications in data. Any change in data is to done programmatically only.

// 1.1 RDD -> DF
var dataRDD = sc.textFile("EdurekaSparkProjects/dataset_bank-full.csv").map(input => {
	var cleanedValues = input.replace(";"," ").replace("\"", "");
	var splitvalues = " ".r.split(cleanedValues);
	splitvalues;
});

var topRowRemovedRDD = dataRDD.zipWithIndex().collect {
  case (v, index) if index != 0 => v;
};

var rowRDD = topRowRemovedRDD.map(input => {
	Row(input(0).toInt,input(1),input(2),input(3),input(4),input(5).toInt,input(6),input(7),input(8),input(9).toInt,input(10),input(11).toInt,input(12).toInt,input(13).toInt,input(14).toInt,input(15),input(16));
});

var mySchema = StructType(Array( StructField("age",IntegerType,true),StructField("job", StringType, true),StructField("marital", StringType, true),StructField("education", StringType, true),StructField("default", StringType, true),StructField("balance", IntegerType, true),StructField("housing", StringType, true),StructField("loan", StringType, true),StructField("contact", StringType, true),StructField("day", IntegerType, true),StructField("month", StringType, true),StructField("duration", IntegerType, true),StructField("campaign", IntegerType, true),StructField("pdays", IntegerType, true),StructField("previous", IntegerType, true),StructField("poutcome", StringType, true),StructField("y", StringType, true)));

var dataDF = spark.createDataFrame(rowRDD, mySchema);

// 1.2 RDD -> Save the data -> DF
var commaRDD = sc.textFile("EdurekaSparkProjects/dataset_bank-full.csv").map(input => {
	var commaRows = input.replace(";"," ").replace("\"", "").replace(" ", ",");
	commaRows;
});
commaRDD.saveAsTextFile("EdurekaSparkProjects/rddsavedf.csv");

var savedDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("EdurekaSparkProjects/rddsavedf.csv");

// 1.3 DF
var marketingDF = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").option("quote","").load("EdurekaSparkProjects/dataset_bank-full.csv");

for (colname <- marketingDF.columns) {
	marketingDF = marketingDF.withColumn(colname.replace(""""""", ""),trim(col(colname), """""""));
	marketingDF = marketingDF.drop(colname);
}

// 2 Giving Market Success Rate (No. of people subscribed / total no. of entries)
// 2a Give Marketing Failure Rate
var counts = dataDF.groupBy("y").count();
var rates = counts.withColumn("rates", (counts("count")/dataDF.count())*100);

// 3 Maximum, Mean, Minimum age of average targeted customer
var statValues = dataDF.select(max("age"), mean("age"), min("age"));

// 4 Check quality of customers by checking average balance, median balance of customers
var avgBalance = dataDF.select(mean("balance"));
var sortedBalanceDF = dataDF.orderBy("balance");
var medianBalance = sortedBalanceDF.collect()(round(sortedBalanceDF.select(count("age")).collect()(0) / 2))(5);

// 5 Check if age matters in marketing subscription for deposit
var ageVsSub = data.groupBy("age").count("y").orderBy(desc("count"));

// 6 Check if marital status matters in marketing subscription for deposit
var maritalVsSub = data.groupBy("marital").count("y").orderBy(desc("count"));

// 7 Check if age and marital status matters in marketing subscription for deposit\

// 8 Do feature engineering for column - age and find right age effect on compaign
