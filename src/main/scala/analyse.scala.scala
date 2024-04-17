package analyse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 


object  Analyse {
// retourne 1 si le df contient bien les bonnes informations 0 sinon
def verifier_contenu(df: org.apache.spark.sql.DataFrame ) :Boolean = {
    val donnee_conforme =false
    val nb_livre =0
    val nb_phrase=0
    val nb_mot = 0
    val nb_mot_different=0
    val moyenne_nb_mot_par_phrase = 0
    val nb_mot_par_phrase= 0
    return donnee_conforme
}



}