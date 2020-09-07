# Ejercicio 1: POO (Traits, case classes and Objects)

Necesitamos 3 tipos de mensajes, todos ellos tendran asociado un `id`:

- `Text`: contiene un `String`
- `Attachment`: contiene una URI, que indentifica el fichero adjunto.
- `Sound`: contiene una URI, que indentifica el fichero de sonido.

Los mensajes también necesitan soporte para:

- Ser `Printable`: Acción de poder imprimirse.
- Ser `Runnable`: Acciones de `start`/`stop`
- Ser `Coloreable`: `background_color`

El modelado se debe hacer teniendo en cuenta las siguientes restricciones:

- `Text` y `Attachment` pueden ser `Printable`
- Solamente los mensajes de tipo `Sound` pueden ser `Runnable`
- Solamente los mensajes de tipo `Text` pueden ser `Coloreable`

Extra:

- Define un método `apply` para que los mensajes de tipo `Text`, puedan sear creados unicamente con su contenido.
- Define un método `apply` para los mensajes de tipo `Attachment` y `Sound`, para poder crear los mensajes usando únicamente el URI.
