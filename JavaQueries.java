package test;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;

public class JavaQueries 
{
    static volatile long prevRowValue1 = Long.MIN_VALUE;
    static volatile long prevRowValue2 = Long.MIN_VALUE;
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkSession spark = SparkSession.builder().appName("SparkApp").config("spark.master", "local").getOrCreate();
        
        StructType schema = new StructType()
        .add("", "long")
        .add("id", "long")
        .add("price", "long")
        .add("sales", "long");
        
        Dataset<Row> df = spark.read()
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv("C:/Users/admin/Desktop/GfK/Spark/py-result.csv");
                
        org.apache.spark.sql.expressions.WindowSpec my_window = Window.partitionBy().orderBy("id");
        df = df.withColumn("prev_price", functions.lag(df.col("price"),1).over(my_window));
        df = df.withColumn("prev_sales", functions.lag(df.col("sales"),1).over(my_window));

        df = df.withColumn("PriceElasticty", ( 
                        (functions.when(functions.isnull(df.col("sales").$minus(df.col("prev_sales"))), 0).otherwise(df.col("prev_sales").$minus( df.col("sales")))
                            .divide( 
                        ((df.col("sales").$plus( df.col("prev_sales"))))))
                                .divide(
                        (functions.when(functions.isnull(df.col("price").$minus( df.col("prev_price"))), 0).otherwise(df.col("price").$minus( df.col("prev_price")))
                            .divide(
                        ((df.col("price").$plus( df.col("prev_price"))))))
                    ))
                );

        df = df.withColumn("SalesRevenue", (df.col("sales").multiply( df.col("price") ) ) );

        df.coalesce(1)
        .repartition(1)
        .write()
        .mode("overwrite") 
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .csv("C:/Users/admin/Desktop/GfK/Spark/final-result.csv");

        spark.stop();
    }
}
