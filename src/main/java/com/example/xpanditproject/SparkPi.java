package com.example.xpanditproject;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public final class SparkPi {

    //In this exercise I chose to change the "nan" value of the collum "Sentiment_Polarity" because if not so I would get a lot of NaN values in DF_1.
    //If you prefer to have those nan values but not have them if the df_1 rewrite the line 22 to the line 30 and make refactor the code.
    public static Dataset<Row> getDataFrame_Exercise1(SparkSession spark) {
        Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/googleplaystore_user_reviews.csv");
        df = df.withColumn("Sentiment_Polarity", functions.when(
                functions.col("Sentiment_Polarity").equalTo("nan"),
                0
        ).otherwise(functions.col("Sentiment_Polarity")));

        df.show();

        Dataset<Row> df_1 = df.groupBy("App").agg(functions.avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"));
        df_1.show();

        return df_1;
    }

    //In this exercise I think I cannot write the .csv because of not having HADOOP_HOME set, instead its creating me a file
    public static void getDataFrame_Exercise2(SparkSession spark) {
        Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/googleplaystore.csv");

        df = df.filter(df.col("Rating").gt(3.9));
        df = df.orderBy(df.col("Rating").desc());
        df.show();

        df.coalesce(1).write().mode("overwrite").option("header", "true").option("delimiter", "§").csv("src/main/resources/best_apps.csv");

    }


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Xpand-it Challenge")
                .master("local[*]")
                .getOrCreate();


        //Dataset<Row> df_1 = getDataFrame_Exercise1(spark);
        //df_1.show();

        getDataFrame_Exercise2(spark);

        spark.stop();
    }



}