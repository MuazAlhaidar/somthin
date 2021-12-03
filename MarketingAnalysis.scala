import org.apache.spark.sql.functions._;
import org.apache.spark.sql.functions.trim;

var marketingDF = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").option("quote","").load("EdurekaSparkProjects/dataset_bank-full.csv");

for (colname <- marketingDF.columns) {
	marketingDF = marketingDF.withColumn(colname.replace("""""", ""),trim(col(colname), """"""))
	marketingDF = marketingDF.drop(colname)
}

marketingDF.show()
