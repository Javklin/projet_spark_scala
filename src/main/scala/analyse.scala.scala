package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

object  Analyse {

// retourne true si le df contient bien les bonnes informations false sinon
def check_content(df: org.apache.spark.sql.DataFrame ) :Boolean = {
    val complete_data =false

    //nb de mot
    val df_with_word_count = df.withColumn("word_count", size(array_remove(split(col("book"), "\\s+"), "")))
    df_with_word_count.show()
    val total_word_count = df_with_word_count.agg(sum("word_count")).collect()(0)(0)
    //println(s"Total number of words: $total_word_count *******************************************************************************")

    //nb de phrase 
    val count_sentence_udf = udf((text: String) => {
    text.split("[.!?]").count(_.trim.nonEmpty)
    })
    val df_with_sentence_count = df.withColumn("sentence_count", count_sentence_udf(col("book")))
    df_with_sentence_count.show()
    val totalSentenceCount = df_with_sentence_count.filter(trim(col("book")) =!= "").agg(sum("sentence_count")).collect()(0)(0)
    //println(s"Total number of sentences: $totalSentenceCount *******************************************************************************")

    //nb de livre
    val book_number =df.count()
    //println(s"Total number of book: $book_number *******************************************************************************")
    
    // nb de mot different
    // val wordsDF = df.withColumn("word", explode(split(col("book"), "\\s+")))
    val words_df = df.withColumn("word", explode(split(col("book"), "\\s+")))
    .withColumn("word", regexp_replace(col("word"), "[\\.\\!\\?]", ""))
    val distinct_word_count_df = words_df.filter(trim(col("word")) =!= "").groupBy("word").count()
    distinct_word_count_df.show()
    val different_word_number_df = distinct_word_count_df.agg(sum("count").alias("total_count"))
    val different_word_number = distinct_word_count_df.count()

    // la moyenne de mot par phrase
    val average_word_by_sentence=calculate_average_word_count_per_sentence(df, "book")

    // la médiane de mot par phrase
    val median_word_by_sentence = calculate_median_word_count_per_sentence(df, "book")

    println(s"Total number of words: $total_word_count *******************************************************************************")
    println(s"Total number of sentences: $totalSentenceCount *******************************************************************************")
    println(s"Total number of book: $book_number *******************************************************************************")
    println(s"Distinct number of word: $different_word_number *******************************************************************************")
    println(s"average of words by sentences: $average_word_by_sentence  *******************************************************************************")
    println(s"median of words by sentences: $median_word_by_sentence *******************************************************************************")
    return complete_data
}


//pour le nombre de mot moyen par phrase
val sentence_tokenizerUDF = udf((text: String) => text.split("[.!?]").map(_.trim))
val count_wordsUDF = udf((sentence: String) => sentence.split("\\s+").length)
def calculate_average_word_count_per_sentence(df: DataFrame, columnName: String): Double = {
    val df_sentence = df.withColumn("sentences", explode(sentence_tokenizerUDF(col(columnName))))
    val df_word = df_sentence.withColumn("word_count", count_wordsUDF(col("sentences")))
    val df_word_by_sentence = df_word.agg(avg(col("word_count"))).head().getDouble(0)
    df_word_by_sentence
}
//pour la médiane de mot moyen par phrase
def calculate_median_word_count_per_sentence(df: DataFrame, columnName: String): Integer = {
    val df_sentence = df.withColumn("sentences", explode(sentence_tokenizerUDF(col(columnName))))
    val df_word = df_sentence.withColumn("word_count", count_wordsUDF(col("sentences")))
    val median_word_by_sentence = df_word
      .selectExpr("percentile_approx(word_count, 0.5) as median")
      .head().getAs[Integer]("median")
    median_word_by_sentence
}


}