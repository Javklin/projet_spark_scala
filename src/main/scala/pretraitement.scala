package pretraitement
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._

object  Pretraitement {

    // normalise un fichier et créer un dataframe qui contient sur chaque ligne le contenu d'un livre 
    def normaliser_fichier(spark: org.apache.spark.sql.SparkSession, chemin_fichier:  String ) :org.apache.spark.sql.DataFrame   = {
    val mon_fichier = spark.sparkContext.textFile(chemin_fichier)
    val texte_combine = mon_fichier.reduce(_ + " " + _)
    // on formatte le contenu du fichier
    val texte_nettoye = texte_combine
      // on convertit tous en minuscule 
      .toLowerCase
        // on supprime la ponctuation
        .replaceAll("[^a-zA-Z0-9\\s]", "")
        // on remplace les caractères accentues 
        .replaceAll("[àáâãäå]", "a")
        .replaceAll("[èéêë]", "e")
        .replaceAll("[ìíîï]", "i")
        .replaceAll("[òóôõö]", "o")
        .replaceAll("[ùúûü]", "u")
        .replaceAll("[ýÿ]", "y")
    // les sépareateurs des livres isbn et copyright XXXX
    val separateurs = "(isbn|copyright \\d{4})"
    val texte_separe = texte_nettoye.split(separateurs)
    val df_livre = spark.createDataFrame(texte_separe.map(Tuple1.apply)).toDF("text")
    //df_livre.collect.foreach(println) // pour afficher chaque ligne dataframe 
    return df_livre
  }

}

