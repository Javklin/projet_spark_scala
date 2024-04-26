

import org.apache.spark.sql.SparkSession
import pretraitement._
import analyse._


object SimpleApp extends App {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    //val file_test = "./bookcorpus/test.txt"
    //val df_test =Pretraitement.clean_file(spark, file_test)
    val file_book_1 = "./bookcorpus/books_large_p1.txt"
    val file_book_2 = "./bookcorpus/books_large_p2.txt"
    val df_book_1 = Pretraitement.clean_file(spark, file_book_1)
    val df_book_2 = Pretraitement.clean_file(spark, file_book_2)
    val df_book_fuse =df_book_1.unionByName(df_book_2)
    //df_book_fuse.show(5,true)
    Analyse.check_content(df_book_fuse)
    spark.stop()
}
