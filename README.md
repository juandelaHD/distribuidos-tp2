# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Resolución (Golang)

### Supuestos generales

Los siguientes supuestos aplican a toda la resolución, independientemente del escenario, y conviene tenerlos a la vista antes de entrar a los detalles de cada etapa.

Se asume que ningún nodo se cae durante una corrida y que el broker no pierde mensajes. El sistema no implementa tolerancia a fallos, no hay reintentos a nivel aplicación, reconciliación de estado, ni recuperación ante caídas.

El `client_id` que circula por el protocolo interno es un entero efímero que el gateway asigna en orden de aceptación de conexiones TCP. Vive únicamente durante la corrida y no se persiste ni se expone a los clientes: existe solo para separar los flujos dentro del sistema.

Las colas de RabbitMQ se levantan vacías en cada `make up` porque no hay volúmenes persistentes configurados. No quedan mensajes residuales entre corridas, por lo que cada ejecución parte de un estado limpio.

### Parte 1 — Multi-cliente con una sola réplica de cada control (escenario 2)

En esta etapa tenemos tres clientes concurrentes que consultan al sistema contra una única instancia de Sum, Aggregator y Joiner. El objetivo concreto es que cada cliente reciba su propio top de frutas sin que los datos de un cliente contaminen el resultado de otro, manteniendo intactos todos los componentes que el enunciado declara inmodificables.

El cambio central está en el protocolo de mensajería interno. Antes, los nodos intercambiaban listas de pares `(fruta, cantidad)` y la marca de fin de stream se codificaba como una lista vacía. Esa representación funcionaba para un único cliente porque el sistema entero podía suponer que todos los datos pertenecían a la misma consulta. Cuando aparecen varios clientes en paralelo, esa suposición deja de ser válida y hace falta que cada mensaje viaje con un dato extra sobre el cliente que lo originó. Por eso el formato lo pasé a `{"c": <client_id>, "d": [{"f": <fruta>, "a": <cantidad>}]}`, donde el `client_id` siempre acompaña al mensaje, sea de datos o de fin. La marca de EOF se sigue representando con `d` vacío. En esta etapa cada mensaje publicado contiene exactamente una fila, porque el protocolo externo entre el cliente y el gateway vi que envía un record esperando su ACK y el gateway propaga cada fila al broker uno a uno. Creo que sería ventajoso batchear esos mensajes para reducir el overhead de serialización y publicación pero la lógica de "leer-serializar-publicar" está fuera del `message_handler` así que esa optimización queda fuera del alcance de esta etapa.

Cabe mencionar que el gateway es el responsable de asignar y sostener las identidades. Cada vez que acepta una conexión TCP construye un `MessageHandler` y le asigna un identificador entero único, tomado de un contador que vive en el paquete del handler. Ese identificador viaja sellado en cada mensaje que el handler serializa al ingresar a `input_queue`. Para el camino de respuesta, el handler hace el filtro inverso asi que cuando el gateway le ofrece un mensaje proveniente de `results_queue`, el handler deserializa, compara el `client_id` con el suyo y devuelve `nil` si no le pertenece.

Sum y Aggregator dejan de tener un único diccionario global de acumulación. En su lugar, cada uno mantiene una estructura `map[ClientID -> map[string -> FruitItem]]`, que aísla los datos de cada cliente. Cuando llega un mensaje de datos para un cliente nuevo, el diccionario correspondiente se crea y si llega el EOF de un cliente, el nodo envía los datos acumulados al siguiente paso, reenvía el EOF con el mismo client_id y borra la entrada del diccionario para liberar memoria. Cada cliente tiene su propia entrada en el diccionario, por lo que los datos  de distintas consultas nunca se mezclan. Esto funciona porque el middleware procesa los mensajes de a uno por vez, sin paralelismo dentro del nodo, así que no hace falta ningún mecanismo de sincronización.

El Joiner solo termina enviando los tops parciales preservando el `client_id` y descarta las marcas de EOF, que no aportan información útil (al menos para este escenario) al gateway donde aún no hay coordinación de varios Aggregators por cliente. Cuando se introduzcan múltiples instancias de Aggregator, creo que sería útil que pasará a contar EOFs por cliente, de modo que el Joiner sepa cuándo recibió todos los tops parciales de un cliente y pueda enviar el resultado final al gateway, pero por ahora esa información no es necesaria.

