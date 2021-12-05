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
