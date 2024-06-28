package com.example.xpanditproject;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public final class SparkPi {

    /* Notes
        - In this exercise I chose to change the "nan" value of the column "Sentiment_Polarity" because if not so I would get a lot of NaN values in df_1.
        - If you prefer to have those nan values but not have them if the df_1 rewrite the line 22 to the line 30 and make refactor the code.
    */
    public static Dataset<Row> getDataFrame_Exercise1(SparkSession spark) {

        Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/googleplaystore_user_reviews.csv");

        df = df.withColumn("Sentiment_Polarity", functions.when(
                functions.col("Sentiment_Polarity").equalTo("nan"),
                0).otherwise(functions.col("Sentiment_Polarity")));

        Dataset<Row> df_1 = df.groupBy("App").agg(functions.avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"));
        df_1 = df_1.orderBy(df.col("App").asc());

        return df_1;
    }


    /* Notes
        - In this exercise I think I cannot write the .csv because of hadoop I think. Instead its creating me a file
    */
    public static void getDataFrame_Exercise2(SparkSession spark) {
        Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/googleplaystore.csv");

        df = df.filter(df.col("Rating").gt(3.9));
        df = df.orderBy(df.col("Rating").desc());

        df.coalesce(1).write().mode("overwrite").option("header", "true").option("delimiter", "§").csv("src/main/resources/best_apps.csv");
    }


    /* Notes
        - I know there are some errors, but i couldn't solve them.
        - The date format I maintain the same because I couldn't format it, I comment the code I was trying to use to do this step, so you can analyse.
        - I saw that in the version there are some errors in the input, like there's an "Android version" = "Mature +17", I think that doesn't belong there.
     */
    public static Dataset<Row> getDataFrame_Exercise3(SparkSession spark) {
        Dataset<Row> df = spark.read().option("header", "true").csv("src/main/resources/googleplaystore.csv");

        UDF1<String, Double> changeSize = (String size) -> {
            if (size == null) return null;
            size = size.trim();
            try {
                if (size.endsWith("M")) {
                    return Double.parseDouble(size.substring(0, size.length() - 1));
                } else if (size.endsWith("K")) {
                    return Double.parseDouble(size.substring(0, size.length() - 1)) / 1024;
                } else {
                    return null;
                }
            } catch (NumberFormatException e) {
                return null;
            }
        };
        spark.udf().register("changeSize", changeSize, DataTypes.DoubleType);


        UDF1<String, Double> changePrice = (String price) -> {
            if (price == null) return null;
            try{
                price = price.trim();
                if(price.endsWith("$")) {
                    return Double.parseDouble(price.substring(1)) * 0.9;
                } else {
                    return 0.0;
                }
            }catch (NumberFormatException e) {
                return null;
            }
        };
        spark.udf().register("changePrice", changePrice, DataTypes.DoubleType);


//        SimpleDateFormat inputFormat = new SimpleDateFormat("MMMM d, yyyy");
//        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        UDF1<String, java.sql.Date> changeFormatDate = (String date) -> {
//            if (date == null) return null;
//            try {
//                Date parsedDate = inputFormat.parse(date);
//                String formattedDate = outputFormat.format(parsedDate);
//                return java.sql.Date.valueOf(formattedDate.split(" ")[0]); // Convert to java.sql.Date
//            } catch (ParseException e) {
//                return null; // Handle parsing errors gracefully
//            }
//        };
//        spark.udf().register("changeFormatDate", changeFormatDate, DataTypes.DateType);


        df = df.withColumn("Size", functions.call_udf("changeSize", df.col("Size")))
                .withColumn("Price", functions.call_udf("changePrice", df.col("Price")))
                //.withColumn("Last Updated", functions.call_udf("changeFormatDate", df.col("Last Updated")))
                .withColumn("Genres", functions.split(df.col("Genres"), ";"))
                .withColumnRenamed("Content Rating", "Content_Rating")
                .withColumnRenamed("Last Updated", "Last_Updated")
                .withColumnRenamed("Current Ver", "Current_Version")
                .withColumnRenamed("Android Ver", "Minimum_Android_Version");

        Dataset<Row> df_final = df.dropDuplicates("App");
        Dataset<Row> df_categories = df.groupBy("App").agg(functions.collect_list(df.col("Category")).alias("Categories"));
        df_categories = df_categories.withColumn("Categories", functions.array_distinct(df_categories.col("Categories")));
        Dataset<Row> df_max_reviews = df.groupBy("App", "Rating").agg(functions.max("Reviews").alias("Reviews")).dropDuplicates();

        df_final = df_final.drop("Rating", "Reviews");
        df_final = df_final.join(df_categories, "App").join(df_max_reviews, "App");
        df_final = df_final.select("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version").orderBy(df.col("App").asc());

        return df_final;
    }

    /*Note
        - In my pc i couldn't save in the right way because of a problem with hadoop file, but i think this is the right answer
    */
    public static void getDataFrame_Exercise4(Dataset<Row> df_1, Dataset<Row> df_3) {
        df_1 = df_1.dropDuplicates("App");
        Dataset<Row> df = df_3.join(df_1, "App");

        df.write().format("parquet").mode("overwrite").option("compression", "gzip").save("src/main/resources/googleplaystore_cleaned.csv");
    }


    public static void getDataFrame_Exercise5(Dataset<Row> df_1, Dataset<Row> df_3) {
        df_1 = df_1.dropDuplicates("App");
        df_3 = df_3.join(df_1, "App");

        Dataset<Row> df_Genre_count = df_3.groupBy("Genres").agg(functions.count("Genres").alias("Count")).dropDuplicates();
        Dataset<Row> df_Average_Rating = df_3.groupBy("Genres").agg(functions.avg("Rating").alias("Average_Rating"));
        Dataset<Row> df_Average_Sentiment_Polarity = df_3.groupBy("Genres").agg(functions.avg("Average_Sentiment_Polarity"));

        Dataset<Row> df_4 = df_3.select("Genres");
        df_4 = df_4.join(df_Genre_count, "Genres").dropDuplicates().join(df_Average_Rating, "Genres").join(df_Average_Sentiment_Polarity, "Genres");
        df_4 = df_4.withColumnRenamed("Genres","Genre");

        df_4.write().format("parquet").mode("overwrite").option("compression", "gzip").save("src/main/resources/googleplaystore_metrics.csv");
    }


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Xpand-it Challenge").master("local[*]").getOrCreate();

        Dataset<Row> df_1 = getDataFrame_Exercise1(spark);
        getDataFrame_Exercise2(spark);
        Dataset<Row> df_3 = getDataFrame_Exercise3(spark);
        getDataFrame_Exercise4(df_1, df_3);
        getDataFrame_Exercise5(df_1, df_3);
        spark.stop();
    }

}