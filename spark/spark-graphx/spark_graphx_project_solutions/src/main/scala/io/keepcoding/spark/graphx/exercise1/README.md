# Exercise 1: School Graph

En este ejercicio vamos a crear un grafo distribuido, con los siguientes requisitos:

* En nuestro grafo existiran Profesores y Alumnos:
    * Profesores: Nombre y Curso
    * Alumnos: Nombre y Edad
    
* Existirán 3 tipos de relaciones: "amigos", "profesor", "compañero de trabajo".

* En nuestro centro existen los distintos usuarios:
    * Profesores: Pepe(matematicas), Rocio(Español)
    * Alumnos: Luis(21), Sara(20), Maria(22), Fede(18)
    
* Las relaciones de los usuarios son:

```
pepe -> profesor -> luis
pepe -> profesor -> sara
pepe -> profesor -> maria
pepe -> profesor -> fede

rocio -> profesor -> sara
rocio -> profesor -> fede

luis -> amigo -> fede
sara -> amigo -> maria
sara -> amigo -> fede

pepe -> compañero -> rocio
```

1. Crea un grafo con estos usuarios y sus relaciones.
2. Imprime todos los elementos del grafo, con sus relaciones.
3. Cuenta el numero de profesor de nuestro sistema.
4. Cuantos alumnos tiene actualmente el profesor Pepe.
5. Cual es el alumno más mayor de Pepe.
6. Elimina las relaciones de compañeros, creando un nuevo grafo
7. Si consideramos que un persona tiene que estar más valorada en base al numero de relaciones directas o indirectas
cual sería la clasificación, podemos utilizar un algoritmo de PageRank.
8. Calcula el numero de relaciones que forman un triangualo en nuestro colegio.