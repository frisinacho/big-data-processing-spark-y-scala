# SSS Exercise 1: Basic Example

## Read - Print

En este ejercicio vamos a crear un job de streaming que va a ir cargando todos los ficheros que aparezcan
en el directorio `resources/exercise1/input`. Spark procesará estos ficheros en streaming y los mostrará en pantalla. 
Irá moviendo los ficheros procesados al directorio `resources/exercise1/archive`.

**NOTA:** La copia de los ficheros es best-effort, spark moverá los ficheros cuando el consideré no es algo inmediato. 

Podemos usar los ficheros del directorio `resources/exercise1`, para ir copiandolos a `resources/exercise1/input`.
    
1. ¿Podemos seleccionar las columnas `sensor_id` & `timestamp` como si fuera un job de sparkSQL.?
2. ¿Podemos contar el número de mensajes de cada sensor? --> OUTPUT MODES

## Output Modes

Como hemos visto en el ejercicio anterior, los modos de salida nos limitan nuestro funcionamiento.

En este ejercicio vamos a utilizar 2 modos de salida: `Complete`, `Update` y veremos la diferencia en la salida de los 
datos. El modo `Append` lo usarémos en un ejercicio posterior.