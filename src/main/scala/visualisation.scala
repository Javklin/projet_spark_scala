package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

object  Visualisation_breeze {
  import org.apache.spark.ml.linalg.Vector
import breeze.plot._
import breeze.linalg.{DenseMatrix, DenseVector}
    //pour voir l'évolution du nombre de mot dans chaque livre
    def display_word_count(df: org.apache.spark.sql.DataFrame)  = {
     val aggregated_data = df.groupBy("book").agg(Map("word_count" -> "sum"))
     val word_count = aggregated_data.select(col("sum(word_count)")).collect().map(_.getLong(0))
     val books = aggregated_data.select(col("book").cast("string")).rdd.map(_.getString(0)).collect()
     val count_matrix= new DenseMatrix(rows = word_count.length, cols = 1, data = word_count.map(_.toDouble))
     val f = Figure()
     val p = f.subplot(0)
     p += plot(DenseVector(books.indices.map(_.toDouble).toArray), count_matrix(::, 0))
     p.xlabel = "Livres"
     p.ylabel = "Nombre de mot"
     p.title = "Evolution du nombre de mot dans les livres"
     f.saveas("./visualisation/word_count_graph.png")
    }

    //pour voir l'évolution du nombre de phrase dans chaque livre
    def display_sentence_count(df: org.apache.spark.sql.DataFrame)  = {
     val aggregated_data = df.groupBy("book").agg(Map("sentence_count" -> "sum"))
     val sentence_count = aggregated_data.select(col("sum(sentence_count)")).collect().map(_.getLong(0))
     val books = aggregated_data.select(col("book").cast("string")).rdd.map(_.getString(0)).collect()
     val count_matrix= new DenseMatrix(rows = sentence_count.length, cols = 1, data = sentence_count.map(_.toDouble))
     val f = Figure()
     val p = f.subplot(0)
     p += plot(DenseVector(books.indices.map(_.toDouble).toArray), count_matrix(::, 0))
     p.xlabel = "Livres"
     p.ylabel = "Nombre de phrase"
     p.title = "Evolution du nombre de phrase dans les livres"
     f.saveas("./visualisation/sentence_count_graph.png")
    }
    
    //pour voir l'évolution du nombre de phrase et de mot dans chaque livre
    def display_word_and_sentence_count(wordDF: DataFrame, sentenceDF: DataFrame): Unit = {
      val word_aggregated_data = wordDF.groupBy("book").agg(Map("word_count" -> "sum"))
      val sentence_aggregated_data = sentenceDF.groupBy("book").agg(Map("sentence_count" -> "sum"))
      val word_counts = word_aggregated_data.select(col("sum(word_count)").alias("word_count"))
        .collect().map(_.getLong(0))
      val sentence_counts = sentence_aggregated_data.select(col("sum(sentence_count)").alias("sentence_count"))
        .collect().map(_.getLong(0))
      val books = word_aggregated_data.select(col("book").cast("string")).rdd.map(_.getString(0)).collect()
      val f = Figure()
      val p = f.subplot(0)
      p += plot(DenseVector(books.indices.map(_.toDouble).toArray), DenseVector(word_counts.map(_.toDouble)), name = "Nombre de mot")
      p += plot(DenseVector(books.indices.map(_.toDouble).toArray), DenseVector(sentence_counts.map(_.toDouble)), name = "Nombre de phrase")
      p.xlabel = "Livres"
      p.ylabel = "Nombre"
      p.title = "Evolution du nombre de mot et de phrase dans les livres"
      p.legend = true
      f.saveas("./visualisation/word_and_sentence_count_graph.png")
    }

    def displayTopicDistribution(df: org.apache.spark.sql.DataFrame) = {
      val books = df.select("book").rdd.map(_.getString(0)).collect()
      val topic_distribution = df.select("topicDistribution").rdd.map(_.getAs[org.apache.spark.ml.linalg.Vector](0)).collect()
      val topic_matrix = new DenseMatrix(rows = topic_distribution.length, cols = topic_distribution.head.size, data = topic_distribution.flatMap(_.toArray))
      val f = Figure()
      val p = f.subplot(0)
      for (i <- 0 until topic_matrix.cols) {
        p += plot(DenseVector(books.indices.map(_.toDouble).toArray), topic_matrix(::, i))
      }
      p.xlabel = "Livres"
      p.ylabel = "Distribution de sujets"
      p.title = "Distribution des sujets par livre"
      f.saveas("./visualisation/topic_distribution_graph.png")
}
}

object  Visualisation_ploty {
//on utilise ploty car la version de breeze utiliséee ne permet pas l'affichage de chaine de caractère
//code fait à partir de l'exmemple sur https://zwild.github.io/posts/plotly-examples-for-scala/
import plotly._, element._, layout._, Plotly._
import org.apache.spark.ml.linalg.Vector
    //pour voir le nombre de chaque mot dans les livres
    def display_occurence_word(df: org.apache.spark.sql.DataFrame)  = {
      val books = df.select("word").rdd.map(_.getString(0)).collect().toList
      val counts1 = df.select("count").rdd.map(_.getLong(0)).collect().toList
      val trace1 = Bar(
        books,
        counts1,
        name = "Book Counts",
        text = books.map(_ + "!"),
        marker = Marker(
          color = Color.RGB(49, 130, 189),
          opacity = 0.7)
      )
      val layout = Layout(
        title = "Book Counts",
        xaxis = Axis(title = "Books"),
        yaxis = Axis(title = "Counts")
      )
      Seq(trace1).plot(
        "./visualisation/word_count.html",
        Layout(
          title = "Nombre d occurrence de chaque mot",
          xaxis = Axis(tickangle = -45),
          yaxis = Axis(
            title = "",
            titlefont = Font(size = 20, color = Color.RGB(107, 107, 107))),
          barmode = BarMode.Group,
          bargroupgap = 0.1),
        false,
        true,
        true
      )
    }    

    //pour voir le nombre de livre pour chaque categorie
     def display_occurence_category(df: org.apache.spark.sql.DataFrame)  = {
      val books = df.select("topic").rdd.map(_.getString(0)).collect().toList
      val counts1 = df.select("count").rdd.map(_.getLong(0)).collect().toList
      val trace1 = Bar(
        books,
        counts1,
        name = "Book Counts",
        text = books.map(_ + "!"),
        marker = Marker(
          color = Color.RGB(49, 130, 189),
          opacity = 0.7)
      )
      Seq(trace1).plot(
        "./visualisation/category_count.html",
        Layout(
          title = "Nombre de livres par categorie",
          xaxis = Axis(tickangle = -45),
          yaxis = Axis(
            title = "",
            titlefont = Font(size = 20, color = Color.RGB(107, 107, 107))),
          barmode = BarMode.Group,
          bargroupgap = 0.1),
        false,
        true,
        true
      )
    }    

    



}

