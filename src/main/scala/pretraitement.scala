package pretraitement
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 

object  Pretraitement {

  def pre_traiter(spark: org.apache.spark.sql.SparkSession, chemin_fichier:  String ) = {
    val mon_fichier = spark.sparkContext.textFile(chemin_fichier)
    println("le fichier a "+ mon_fichier.count() + " lignes") 
    // on formatte le contenu du fichier
    val contenu_nettoye = mon_fichier.map(ligne =>
      // on convertit tous en minuscule 
      ligne.toLowerCase
        // on supprime la ponctuation
        .replaceAll("[^a-zA-Z0-9\\s]", "")
        // on remplace les caractères accentues 
        .replaceAll("[àáâãäå]", "a")
        .replaceAll("[èéêë]", "e")
        .replaceAll("[ìíîï]", "i")
        .replaceAll("[òóôõö]", "o")
        .replaceAll("[ùúûü]", "u")
        .replaceAll("[ýÿ]", "y")
    )
    //on convertit le contenu en dataframe
    val df_livre1 = spark.createDataFrame(contenu_nettoye.map(Tuple1.apply)).toDF("text")
    df_livre1.show()
  }


}

