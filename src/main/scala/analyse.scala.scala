package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}

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
    //df_with_word_count.show()
    val total_word_count = df_with_word_count.agg(sum("word_count")).collect()(0)(0)
   
    //nb de phrase 
    val count_sentence_udf = udf((text: String) => {
    text.split("[.!?]").count(_.trim.nonEmpty)
    })
    val df_with_sentence_count = df.withColumn("sentence_count", count_sentence_udf(col("book")))
    //df_with_sentence_count.show()
    val totalSentenceCount = df_with_sentence_count.filter(trim(col("book")) =!= "").agg(sum("sentence_count")).collect()(0)(0)
   
    //nb de livre
    val book_number =df.count()

    // nb de mot different
    val words_df = df.withColumn("word", explode(split(col("book"), "\\s+")))
    .withColumn("word", regexp_replace(col("word"), "[\\.\\!\\?]", ""))
    val distinct_word_count_df = words_df.filter(trim(col("word")) =!= "").groupBy("word").count()
    //distinct_word_count_df.show()
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

//pour la répartition des sujets de chaque livre
// exemple https://spark.apache.org/docs/latest/ml-clustering.html
def get_topic(df: DataFrame, numTopics: Int): DataFrame = {
  val tokenizer = new Tokenizer().setInputCol("book").setOutputCol("words")
  val countVectorizer = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("features")
    .setVocabSize(5000) 
    .setMinDF(2) 
  val lda = new LDA()
    .setK(numTopics)
    .setMaxIter(10)
    .setFeaturesCol("features")
  val pipeline = new Pipeline().setStages(Array(tokenizer, countVectorizer, lda))
  val model = pipeline.fit(df)
  val topicsData = model.transform(df)
  topicsData.select("book", "topicDistribution")
}

def guessTopic(df: DataFrame, bookColumn: String): DataFrame = {
    val genres_keywords = Map(
        "Fantasy" -> Seq("magic", "wizard", "fantasy", "dragon"),
        "Mystery" -> Seq("detective", "crime", "murder", "mystery"),
        "Romance" -> Seq("love", "romance", "heartbreak"),
        "Science Fiction" -> Seq("space", "alien", "robot", "future"),
        "Thriller" -> Seq("suspense", "thriller", "danger", "tension"),
        "Horror" -> Seq("horror", "scary", "fear", "haunted"),
        "Historical Fiction" -> Seq("historical", "era", "past", "period"),
        "Adventure" -> Seq("adventure", "journey", "quest", "explore"),
        "Poetry" -> Seq("poem", "verse", "rhyme", "stanza"),
        "Drama" -> Seq("drama", "theater", "stage", "act"),
        "Biography" -> Seq("biography", "life", "autobiography", "memoir"),
        "Autobiography" -> Seq("autobiography", "life story", "memoir", "journey"),
        "Self-Help" -> Seq("self-help", "improve", "personal growth", "success"),
        "Philosophy" -> Seq("philosophy", "thought", "theory", "belief"),
        "Business" -> Seq("business", "management", "entrepreneurship", "leadership"),
        "Travel" -> Seq("travel", "journey", "explore", "destination"),
        "Cooking" -> Seq("cooking", "recipe", "chef", "cuisine"),
        "Art" -> Seq("art", "painting", "sculpture", "visual art"),
        "Music" -> Seq("music", "musical", "instrument", "melody"),
        "Spirituality" -> Seq("spirituality", "religion", "faith", "belief")
    )

    def guess_book_topic(bookText: String): String = {
        val guessed_topic = genres_keywords.find { case (topic, keywords) =>
            keywords.exists(keyword => bookText.toLowerCase.contains(keyword))
        }
        guessed_topic.map(_._1).getOrElse("Unknown") 
    }
    val guess_book_topicUDF = udf((bookText: String) => guess_book_topic(bookText))
    return df.withColumn("topic", guess_book_topicUDF(col(bookColumn)))
}


}