package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

object  Analyse {

// retourne true si le df contient bien les bonnes informations false sinon
def check_content(df: org.apache.spark.sql.DataFrame ) :Boolean = {
    val complete_data =false
    //nb de mot

    //nb de phrase 
    val countSentencesUDF = udf((text: String) => {
    text.split("[.!?]").count(_.trim.nonEmpty)
    })    
    val dfWithSentenceCount = df.withColumn("sentence_count", countSentencesUDF(col("book")))
    //dfWithSentenceCount.show()
    val totalSentenceCount = dfWithSentenceCount.agg(sum("sentence_count")).collect()(0)(0)
    //println(s"Total number of sentences: $totalSentenceCount")

    //nb de livre
    val book_number =df.count()
    println(s"Total number of book: $book_number")
    val different_word_number=0
    val average_word_by_sentence = 0
    val median_word_by_sentence = 0
    return complete_data
}




}