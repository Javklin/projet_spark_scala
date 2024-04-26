package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._
//import breeze.linalg._
//import breeze.numerics._
import breeze.plot._
import breeze.linalg.{DenseMatrix, DenseVector}
// import plotly._, plotly.element._, plotly.layout._, plotly.Bar 
// import org.plotly_scala._
// import org.plotly_scala._
// import plotly._
// import plotly.element._
// import plotly.layout._



object  Visualisation {
    def display_word_count(df: org.apache.spark.sql.DataFrame)  = {
     val aggregated_data = df.groupBy("book").agg(Map("word_count" -> "sum"))
     val word_count = aggregated_data.select(col("sum(word_count)")).collect().map(_.getLong(0))
     val books = aggregated_data.select(col("book").cast("string")).rdd.map(_.getString(0)).collect()
     val count_matrix= new DenseMatrix(rows = word_count.length, cols = 1, data = word_count.map(_.toDouble))
     val f = Figure()
     val p = f.subplot(0)
     p += plot(DenseVector(books.indices.map(_.toDouble).toArray), count_matrix(::, 0))
     p.xlabel = "Books"
     p.ylabel = "Word Counts"
     p.title = "Evolution du nombre de mot dans les livres"
     f.saveas("word_count_graph.png")
    }

}