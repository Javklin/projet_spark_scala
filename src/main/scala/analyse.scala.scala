package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object  Analyse {

// affiche le nb de mot,nb de phrase, nb de livre, nb de mot différents,moyenne mot par phrase,médiane mot par phrase
//retourne les DF avec
// - le nb de mot
// - nb de phrase, 
// - nb de mot différents,
// - le nb de mot par phrase
def check_content(df: org.apache.spark.sql.DataFrame ) :(org.apache.spark.sql.DataFrame,
org.apache.spark.sql.DataFrame,
org.apache.spark.sql.DataFrame,
org.apache.spark.sql.DataFrame)
  = {
    //nb de mot
    val df_with_word_count = df.withColumn("word_count", size(array_remove(split(col("book"), "\\s+"), "")))
    df_with_word_count.show()
    val total_word_count = df_with_word_count.agg(sum("word_count")).collect()(0)(0)
   
    //nb de phrase 
    val count_sentence_udf = udf((text: String) => {
    text.split("[.!?]").count(_.trim.nonEmpty)
    })
    val df_with_sentence_count = df.withColumn("sentence_count", count_sentence_udf(col("book")))
    df_with_sentence_count.show()
    val totalSentenceCount = df_with_sentence_count.filter(trim(col("book")) =!= "").agg(sum("sentence_count")).collect()(0)(0)
   
    //nb de livre
    val book_number =df.count()

    // nb de mot different
    val words_df = df.withColumn("word", explode(split(col("book"), "\\s+")))
    .withColumn("word", regexp_replace(col("word"), "[\\.\\!\\?]", ""))
    val distinct_word_count_df = words_df.filter(trim(col("word")) =!= "").groupBy("word").count()
    distinct_word_count_df.show()
    val different_word_number_df = distinct_word_count_df.agg(sum("count").alias("total_count"))
    val different_word_number = distinct_word_count_df.count()

    // la moyenne de mot par phrase
    val (average_word_by_sentence,df_average_word_by_sentence)=calculate_average_word_count_per_sentence(df, "book")

    // la médiane de mot par phrase
    val median_word_by_sentence = calculate_median_word_count_per_sentence(df, "book")

    println(s"Total number of words: $total_word_count *******************************************************************************")
    println(s"Total number of sentences: $totalSentenceCount *******************************************************************************")
    println(s"Total number of book: $book_number *******************************************************************************")
    println(s"Distinct number of word: $different_word_number *******************************************************************************")
    println(s"average of words by sentences: $average_word_by_sentence  *******************************************************************************")
    println(s"median of words by sentences: $median_word_by_sentence *******************************************************************************")
    return (
    df_with_word_count,
    df_with_sentence_count,
    distinct_word_count_df,
    df_average_word_by_sentence)
}

val sentence_tokenizerUDF = udf((text: String) => text.split("[.!?]").map(_.trim))
val count_wordsUDF = udf((sentence: String) => sentence.split("\\s+").length)

//pour obtenir nombre de mot moyen par phrase et le nb de mot par phrase
def calculate_average_word_count_per_sentence(df: DataFrame, columnName: String):( Double, org.apache.spark.sql.DataFrame) = {
    val df_sentence = df.withColumn("sentences", explode(sentence_tokenizerUDF(col(columnName))))
    val df_word = df_sentence.withColumn("word_count", count_wordsUDF(col("sentences")))
    val df_word_by_sentence = df_word.agg(avg(col("word_count"))).head().getDouble(0)
   // df_word_by_sentence
   return (df_word_by_sentence,df_word)
}
//pour obtenir la médiane de mot par phrase
def calculate_median_word_count_per_sentence(df: DataFrame, columnName: String): Integer = {
    val df_sentence = df.withColumn("sentences", explode(sentence_tokenizerUDF(col(columnName))))
    val df_word = df_sentence.withColumn("word_count", count_wordsUDF(col("sentences")))
    val median_word_by_sentence = df_word
      .selectExpr("percentile_approx(word_count, 0.5) as median")
      .head().getAs[Integer]("median")
    median_word_by_sentence
}

//TODO pour afficher les sujets principaux de chaque livre
def get_topic(df: DataFrame) = {

}

}