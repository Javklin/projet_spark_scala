import org.apache.spark.sql.SparkSession
import pretraitement._
import analyse._
import Visualisation_breeze._
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._


object SimpleApp extends App {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val file_book_1 = "./bookcorpus/books_large_p1.txt"
    val file_book_2 = "./bookcorpus/books_large_p2.txt"
    val df_book_1 = Pretraitement.clean_file(spark, file_book_1)
    val df_book_2 = Pretraitement.clean_file(spark, file_book_2)
    val df_book_fuse =df_book_1.unionByName(df_book_2)

    //On récupère les DF à visualiser
    val (df_with_word_count, df_with_sentence_count, different_word_number_df, df_average_word_by_sentence)
    = Analyse.check_content(df_book_fuse)
    /*
    //On visualise les info en sauvegardant les graphes dans des fichiers
    Visualisation_breeze.display_word_count(df_with_word_count)
    Visualisation_breeze.display_sentence_count(df_with_sentence_count)
    Visualisation_breeze.display_word_and_sentence_count(df_with_word_count, df_with_sentence_count)
    Visualisation_ploty.display_occurence_word(different_word_number_df)
   
    //On analyse la répartition de sujet dans chaque livre
    val topic_distribution_df= Analyse.get_topic(df_book_fuse,15)

    //On analyse le sujet de chaque livre
    val topic_guess_df= Analyse.guessTopic(df_book_fuse,"book")
    val topic_counts_df = topic_guess_df
    .groupBy("topic")
    .count()
    .orderBy(desc("count"))

    //On visualise le nombre de livre par catégorie
    Visualisation_ploty.display_occurence_category(topic_counts_df)

    //On visualise la répartition de sujet dans chaque livre
    Visualisation_breeze.displayTopicDistribution(topic_distribution_df)


    */
    spark.stop()
}
