import org.apache.spark.sql.*;


public class Sample {



    public static void main(String[]  args){

        SparkSession spark = SparkSession
                .builder()
        .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://ls-c2f499b8c557ed25ac6fab268e5eeefdcca1b1dc.cxaxvunbmjdm.ap-south-1.rds.amazonaws.com:3306/nursing")
                . option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "nursing.profiles")
                .option("user", "dbmasteruser")
                .option("password", "k%QJL&-jA3oqiaynpiZx<ZX5TWL{R(K#")
                .load();

        jdbcDF.printSchema();
        jdbcDF.select("first_name").show();

        jdbcDF.show();



    }



}
