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

### Parte 2 — Múltiples réplicas de Sum (escenario 3)

El salto de esta etapa es poner N instancias de Sum con una única instancia de Aggregator y de Joiner, sosteniendo todos los clientes concurrentes que ya venían funcionando. Con round-robin sobre `input_queue`, cada mensaje de datos llega a un solo Sum y eso en sí mismo no es problema porque la suma es asociativa y el Aggregator termina consolidando los parciales. El problema real es coordinar el cierre: cuando el gateway publica el EOF de un cliente, RabbitMQ se lo entrega a un único Sum, y los demás Sums no se enteran por sí solos de que no van a venir más datos. Si cada Sum actuara de manera independiente al recibir un EOF, varias consultas quedarían incompletas.

Antes de llegar a la solución definitiva evalué tres enfoques. Los tres coinciden en que el Aggregator necesita contar `N` EOFs por cliente antes de finalizar (una barrera embebida parametrizada por `SUM_AMOUNT`), pero difieren en cómo garantizar que esos `N` EOFs lleven datos completos. Dejo documentadas las opciones descartadas porque creo que aporta valor y cada una revela una dimensión distinta del problema.

#### Idea 1 — Fanout del EOF con Qos global en un canal compartido

La primera idea fue crear un exchange fanout dedicado al que el Sum que recibe el EOF lo reenvíe, y que cada Sum tenga una cola privada bindeada a ese fanout. Así todos los Sums reciben la señal de fin. El problema sutil es que, en el momento en que el EOF se entrega al broadcaster, los datos previos al EOF en `input_queue` ya fueron despachados por FIFO, pero pueden estar todavía en el buffer local de otros workers esperando ser procesados. Si el EOF ya replicado llega a esos workers antes de que terminen con sus datos pendientes, van a flushear un estado incompleto y el resultado final a nivel de Aggregator va a faltarle filas.

La idea para cerrar esa grieta era consumir `input_queue` y la cola privada de EOF sobre el mismo canal AMQP con `Qos(1, global=true)`. Con global=true el broker serializa la entrega a nivel de canal: mientras haya un mensaje sin "ackear", no despacha el siguiente sin importar de qué cola venga. Eso garantiza que el EOF de la cola privada no pueda colarse mientras el worker todavía tiene un dato de `input_queue` sin procesar, porque no se entrega hasta que se ackeó el anterior.

El enfoque lo veía técnicamente correcto pero lo descarté por dos motivos. El primero y más fuerte es de responsabilidades: conseguir que dos consumidores compartan el mismo canal AMQP requiere que Sum tome el canal y la pase a las instancias de middleware, o que el middleware exponga setters para eso. En cualquiera de las dos variantes, Sum pasa a conocer detalles de conexión que justamente la abstracción del middleware está pensada para ocultar; la gestión del canal deja de ser responsabilidad del middleware y se filtra hacia la aplicación. El segundo motivo es el costo de performance: `Qos(1)` fuerza que cada worker procese un mensaje por vez sin pipelining. Mientras un dato está en proceso, el broker no entrega el siguiente, así que los workers se vuelven estrictamente secuenciales y se pierde el paralelismo interno que RabbitMQ te da con prefetch alto. El trade-off real del enfoque es romper la abstracción y achatar el throughput a cambio de una única entrega de fanout por cliente, y la verdad que no me convenció el balance.

#### Idea 2 — Fanout del EOF con fanout de conteos entre workers

La segunda idea extiende la anterior para evitar la dependencia del Qos global. El EOF se sigue fanouteando pero ahora viaja llevando el total de filas del archivo del cliente (trackeado por el `message_handler` en el gateway). Un Sum, al recibir ese EOF, broadcastea por otro fanout su conteo acumulado de ese cliente a todos los demás Sums. Con esos `N` conteos disponibles, cada worker puede verificar si la suma matchea el total y decidir si todavía falta procesamiento pendiente.

El enfoque resuelve el race condition sin necesidad de Qos global pero el costo en mensajes es bastante mayor. Con `N` workers tendría un EOF recibido que deriva en  propagar `N × N` mensajes para que todos se enteren de los conteos de todos, o sea `N²` entregas por cliente antes siquiera de empezar a mandar datos. Para `N=3` son 12; para `N=10` son 110 solamente para verificar las lineas que se encuentran entre Sums y hacer los conteos. Y la mayoría de esos mensajes son redundantes: si la verificación la hace un solo nodo, los otros `N-1` conteos que ve cada worker son información que no va a usar.

#### Idea final — Topología de anillo con verificación por conteo

Lo que terminó quedando es una topología de anillo sobre colas dedicadas. Cada worker Sum `i` tiene una cola privada `sum_ring_i` de la que consume, y publica a `sum_ring_{(i+1) mod N}` del siguiente. El EOF que publica el gateway viaja por `input_queue` junto con los datos (el `message_handler` fue extendido para que trackee un contador de filas que incrementa en cada `SerializeDataMessage` y lo incluya en `SerializeEOFMessage` vía un nuevo campo `t` del protocolo interno). De esa manera, el EOF llega al Sum que round-robin haya elegido acompañado del total de filas que el archivo del cliente contiene.

El protocolo tiene dos rondas que viajan por el anillo. En la **ronda de colecta**, el worker que recibió el EOF se declara iniciador, guarda el total esperado, y arranca un mensaje con su propio conteo de filas procesadas para ese cliente. Ese mensaje va dando la vuelta, y cada worker intermedio simplemente suma su conteo acumulado al valor que viene y reenvía al siguiente. Los workers intermedios no necesitan recordar el mensaje después de forwardearlo: su único aporte es agregar su parte a la suma parcial. Cuando el mensaje vuelve al iniciador, éste compara el total acumulado contra el total esperado. Si los números matchean, arranca la **ronda de envío**. Si no, republica el mismo EOF tal cual lo recibió a `input_queue` para que *eventualmente* vuelva a salir y algún worker (puede ser otro) reinicie la colecta; la suposición es que si la cuenta no dio, algún worker todavía tenía filas bufferadas sin procesar, y darles una vuelta más les da tiempo para procesarlas y *eventualmente* converger a la cuenta correcta.

En la **ronda de envío**, el iniciador flushea sus datos acumulados al exchange del Aggregator junto con un EOF y manda un segundo mensaje por el anillo. Cada worker intermedio, al recibirlo, hace exactamente lo mismo: flushea sus datos acumulados del cliente al Aggregator con su propio EOF, borra el estado local del cliente y reenvía. Cuando el mensaje vuelve al iniciador, éste termina la ronda. El Aggregator, que espera `N` EOFs por cliente, los recibe todos entre la combinación de los flushes, construye el top y lo reenvía al Joiner.

La garantía de la entrega confiable descansa sobre el hecho de que el EOF llega con el total de filas que el cliente envió. Mientras ese total no coincida con la suma de lo que reportan los workers, el sistema se niega a cerrar el cliente y republica el EOF. La convergencia está asegurada bajo los supuestos del TP (nada se pierde, nada se cae). Así, cada reintento le da a los workers "lentos" más tiempo para drenar sus buffers, y eventualmente la suma de conteos va a matchear exactamente el total publicado, en cuyo momento sabemos con certeza que no hay más filas en vuelo para ese cliente y la ronda de envío puede disparar sin miedo a dejar datos afuera.

**Trade-offs respecto a las ideas descartadas**:
- *Mensajes del protocolo*: el anillo usa `2N` mensajes por cliente en el happy path, mientras que la idea del fanout usa `Nx(N+1)`, del orden de `N²`. Desglosando un poco más:
  - **Anillo (`2N`)**:
    - Ronda de colecta: el mensaje arranca en el iniciador y da una vuelta completa. Cada paso entre Sums es 1 mensaje, y con `N` workers son `N` saltos para volver al iniciador.
    - Ronda de envío: misma estructura, otros `N` saltos.
  - **Fanout de conteos (`Nx(N+1)`)**:
    - EOF fanouteado: el gateway publica una vez y el exchange fanout lo entrega a los `N` Sums. Son `N` entregas.
    - Conteos broadcasteados: cada uno de los `N` Sums publica su propio conteo al fanout de conteos, que lo entrega a los `N` Sums. Son `N` publicaciones y cada una genera `N` entregas, totalizando como cota superior `N × N = N²` entregas.
  En base a esto, el anillo escala linealmente mientras que el fanout de conteos escala cuadráticamente, y esa diferencia es justamente lo que me llevó a descartar la idea del fanout aunque resolviera la race condition.
- *Sin Qos global*: al ser secuencial por diseño, no hay condición de carrera entre datos y señal de fin que haya que resolver con un prefetch serializado. Eso me permite mantener la abstracción del middleware intacta y no tengo que compartir canales AMQP entre instancias.
- *Costo del reintento*: el "precio" es que si hay filas bufferadas cuando arranca la primera colecta, el conteo no va a cerrar y hay que hacer una vuelta de `N` mensajes adicionales por reintento. Si los archivos son muy grandes y el sistema no da abasto, puede haber varias vueltas de reintento, lo que agrega latencia. Sin embargo, esa latencia es el costo de garantizar la entrega confiable sin perder datos, y me pareció un trade-off razonable para evitar la explosión de mensajes de la idea 2 y la dependencia del Qos global de la idea 1.

### Parte 3 — Múltiples réplicas de Aggregator (escenario 4)

En esta etapa sumamos `K` instancias de Aggregator manteniendo los `N` Sums y los clientes concurrentes. Acá tenemos 2 problemas. Por un lado, el esqueleto que teníamos hacía un *broadcast* de Sum a todos los Aggregators y, por ende, cada Aggregator terminaba reprocesando los mismos datos. Por otro lado, al repartir los datos hay que garantizar que un mismo par `(cliente, fruta)` llegue siempre al mismo Aggregator, porque si los conteos parciales de una fruta se dispersan entre varios, el top global deja de ser correcto.

La idea que terminó quedando es particionar el espacio de frutas por un hash determinístico: cada Sum, al flushear, rutea cada fruta a un único Aggregator usando `aggregator = fnv1a(fruta) mod K`. FNV-1a 32-bit alcanza de sobra para hashear — como mencionaron en el foro de consultas, no necesitamos una función criptográfica. La función vive dentro del paquete `sum`, así que todos los Sums corren exactamente el mismo binario y el hash es consistente entre réplicas sin coordinación. El EOF, en cambio, mantenemos su broadcast a los `K` Aggregators, porque cada Aggregator necesita contar `N` EOFs por cliente para disparar su "barrera" aunque no le haya tocado ningún dato de ese cliente para su *shard*.

Cabe mencionar por qué el merge de tops parciales es correcto. Dicho simple: **ninguna fruta que merezca estar en el top global puede quedarse afuera del top parcial de su shard**. Como el hash es determinístico, cada fruta cae siempre en el mismo Aggregator y compite únicamente contra las otras frutas de ese mismo shard; nunca contra las de otros. Entonces, si una fruta tiene volumen suficiente para ser top global, con más razón lo es dentro de su propio shard, donde compite contra un subconjunto más chico.

Denotemos el ejemplo con `K=3` y `TOP_SIZE=3`. Supongamos que las frutas caen así:
- shard 0: `manzana=100, banana=80, pera=50, uva=10`
- shard 1: `frutilla=90, frambuesa=60`
- shard 2: `naranja=70, mandarina=40`

El top global es `[manzana=100, frutilla=90, banana=80]`. Los tops parciales que los Aggregators mandan al Joiner son `[manzana, banana, pera]`, `[frutilla, frambuesa]` y `[naranja, mandarina]`. Las tres frutas del top global aparecen en esos parciales — no podía ser de otra forma, porque cada una es, como mínimo, top 3 de su propio shard. Al Joiner le alcanza con unir los parciales, ordenar y cortar en `TOP_SIZE` para reconstruir el top global. Deja de ser un pasante y pasa a ser una barrera que cuenta `K` EOFs por cliente antes de emitir el resultado final.

Para el canal entre el Sum y el Aggregator  decidí usar directamente `K` colas nombradas `aggregation_{i}`, una por Aggregator. Cada Sum arma una instancia por cola destino y publica a la que corresponde por hash; el Aggregator `i` simplemente consume de la suya. `QueueDeclare` es idempotente, así que no importa si el Sum o el Aggregator llegan primero a declararla. El Joiner, por su parte, se mantuvo consumiendo de una cola común donde los `K` Aggregators publican sus tops parciales.

En cuanto a costos, la distribución es equitativa en expectativa cuando la cantidad de frutas distintas es bastante mayor que `K`, el volumen hacia cada Aggregator está acotado por la cantidad de *frutas distintas en su shard*, no por la cantidad de filas ya que el Sum consolidó por fruta antes de flushear. El peor caso de balanceo aparece cuando la cantidad de frutas distintas es menor o igual que `K`: si hay menos frutas que Aggregators, algunos quedan ociosos y sólo reciben los EOFs, y aún con la misma cantidad podría pasar que varias frutas caigan en el mismo shard y dejen otros vacíos por colisiones del hash. Es un límite estructural del sharding por clave, no del hash elegido porque con pocas frutas no existe función que pueda repartir en más baldes de los que alcanza para cubrir, sin romper la invariante "la misma fruta siempre cae en el mismo Aggregator" de la que depende la correctitud del join final. A nivel de mensajes de coordinación, cada cliente genera `N × K` mensajes del Sum al Aggregator (los EOFs broadcast) más un cantidad `X` de datos acotado por la cantidad de frutas distintas del cliente, y `K` tops parciales hacia el Joiner; lineal en ambas dimensiones.
