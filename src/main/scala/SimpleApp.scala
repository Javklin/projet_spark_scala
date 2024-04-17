

import org.apache.spark.sql.SparkSession
import pretraitement._
import analyse._


object SimpleApp extends App {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    //val fichier_a_traiter = "./README.md"
    //val fichier_a_traiter = "./bookcorpus/books_large_p1.txt"
    //val fichier_a_traiter = "./bookcorpus/books_large_p2.txt"
    val fichier_a_traiter = "./bookcorpus/test.txt"
    Pretraitement.normaliser_fichier(spark, fichier_a_traiter)
    spark.stop()
}
