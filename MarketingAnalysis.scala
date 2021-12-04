import org.apache.spark.sql.functions._;
import org.apache.spark.sql.functions.trim;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.{Column => col};
import org.apache.spark.ml.regression.LinearRegression;

var spark = SparkSession.builder.getOrCreate();

// 1 Load data and create Spark dataframe, Spark RDD : 
// you are not supposed to bring any modifications in data. Any change in data is to done programmatically only.

// 1.1 RDD -> DF
var dataRDD = sc.textFile("EdurekaSparkProjects/dataset_bank-full.csv");
splitRDD = dataRDD.map(input => {
	var splitvalues = ";".r.split(input);
	splitvalues;
});

dataRDD.take(10)

// 1.2 RDD -> Save the data -> DF


// 1.3 DF
// var marketingDF = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").option("quote","").load("EdurekaSparkProjects/dataset_bank-full.csv");

// for (colname <- marketingDF.columns) {
// 	marketingDF = marketingDF.withColumn(colname.replace(""""""", ""),trim(col(colname), """""""));
// 	marketingDF = marketingDF.drop(colname);
// }

// marketingDF.show();
