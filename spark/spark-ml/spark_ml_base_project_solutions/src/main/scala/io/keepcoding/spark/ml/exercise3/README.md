# Exercise 3: Tunning

En este ejercicio vamos a tunear el pipeline construido en el ejercicio 1.
Vamos a utilizar un modelo de CrossValidator, con la siguiente configuración:

* ParamGrid
    ```scala
        val paramGrid = new ParamGridBuilder()
          .addGrid(numTrees, Array(20, 30, 40))
          .addGrid(maxDepth, Array(5, 10, 15))
          .addGrid(maxBins, Array(32, 64))
          .addGrid(bootstrap, Array(true, false))
          .build()
    ```
* Evaluador: `MulticlassClassificationEvaluator`