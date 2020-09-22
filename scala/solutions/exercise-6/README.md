# Exercise 6: Futures

Tenemos un User Service usando un repositorio con llamadas asincronas. Necesitamos implementar un método de borrado, siguiente los siguientes pasos:

1. Comprobar si el usuario existe.
    Si el usuario no existe, lanzar una excepción con el mensaje "User not found"
2. Borrar las lineas de telefono asociadas.
3. Borrar el usuario asociado.
4. Devuelve el usuario borrado.

Comprueba las respuestas con los test unitarios.