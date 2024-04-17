package pretraitement
import org.apache.spark.sql.SparkSession
object  Pretraitement {
  def pre_traiter(spark: org.apache.spark.sql.SparkSession, chemin_fichier:  String ) = {
    val mon_fichier = spark.sparkContext.textFile(chemin_fichier)
    println("le fichier a "+ mon_fichier.count() + " lignes") 
  }


}

