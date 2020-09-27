package io.keepcoding.spark.ml.exercise3

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object Tunning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    // Cargamos los datos y dividimos en dataset de train y test (70/30)

    // Cargamos el pipeline almacenado

    // Obtenemos los parametros que queremos configurar desde el stage del pipeline (RandomForestClassifier)

    // Construimos el grid de parametros

    // Instanciamos el evaluator

    // Configuramos el modelo de CrossValidator y lo entramos

    // Probamos el modelo entrenado y calculamos el % de acierto.
    
    spark.close()
  }
}
