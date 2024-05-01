package pretraitement
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

object  Pretraitement {
    // normalise un fichier et créer un dataframe qui contient sur chaque ligne le contenu d'un livre 
    def clean_file(spark: org.apache.spark.sql.SparkSession, file_path:  String ) :org.apache.spark.sql.DataFrame   = {
    val file = spark.sparkContext.textFile(file_path)
    //On lit que les X lignes premières lignes du fichier 
    val text_in_single_row = file
    .take(80000)
    .reduce(_ + " " + _)
    // on formatte le contenu du fichier
    val text_cleaned = text_in_single_row
      // on convertit tout en minuscule 
      .toLowerCase
        // on supprime la ponctuation sauf les points
        .replaceAll("[^a-zA-Z0-9\\.\\!\\?\\s]", "")
        // on remplace les caractères accentues 
        .replaceAll("[àáâãäå]", "a")
        .replaceAll("[èéêë]", "e")
        .replaceAll("[ìíîï]", "i")
        .replaceAll("[òóôõö]", "o")
        .replaceAll("[ùúûü]", "u")
        .replaceAll("[ýÿ]", "y")
    // les sépareateurs des livres isbn et copyright XXXX
    val separators = "(isbn|copyright \\d{4})"
    val text_split = text_cleaned.split(separators)
    //on supprime les valeurs vides du dataframe 
    val df_book = spark.createDataFrame(text_split.map(Tuple1.apply)).toDF("book").na.drop()
    return df_book.filter(trim(col("book")) =!= "")
  }

}

