

import org.apache.spark.sql.SparkSession
import pretraitement._
import analyse._


object SimpleApp extends App {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    //val file_to_use = "./README.md"
    //val file_to_use = "./bookcorpus/books_large_p1.txt"
    //val file_to_use = "./bookcorpus/books_large_p2.txt"
    val file_to_use = "./bookcorpus/test.txt"
    val df_book =Pretraitement.clean_file(spark, file_to_use)
    df_book.collect.foreach(println)
    //df_book.printSchema()
    Analyse.check_content(df_book)
    spark.stop()
}
