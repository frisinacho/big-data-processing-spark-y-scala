# Exercise 1: Pokemon Classifier

En este ejercicio vamos a intentar predecir el tipo de un pokemon.

1. Cargamos el dataset y seleccionamos las columnas que vamos a utilizar y dividimos el dataset (train:70% test:30%)
2. Convertimos las columnas de texto en indices.
3. Creamos un vector de features con las columnas de categoria.
4. Creamos un vector de las features continuas y las normalizamos.
5. Combinamos todas las features.
6. Creamos un modelo de clasificación: RandomForestClassifier.
7. Construimos un pipeline con todos los stages anteriores.
8. Entrenemos el pipeline, para conseguir nuestro modelo (train: 70%)
9. Probamos nuestro modelo (test: 30%)
10. Calculamos el % de aciertos.
11. Almacenamos el modelo entrenado.
