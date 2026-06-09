Trabajo Práctico
Introducción
Nos encontramos desarrollando un sistema para el alquiler y devolución automatizado de bicicletas en una ciudad.

Contaremos con diferentes estaciones distribuidas por ditintos puntos de interés. Las mismas tienen capacidad para albergar entre 10 y 20 bicicletas y se encuentran conectadas a nuestra red.

Cada bicicleta contará con un identificador único de forma tal que las estaciones la puedan reconocer.

Los usuarios contarán con una aplicación móvil que les permitirá consultar bicicletas disponibles cercanas, estaciones posibles de devolución y realizar los pagos.

Objetivo
Implementar un conjunto de aplicaciones que modelen el sistema descripto. Se decidió que dicha implementación se realice íntegramente usando Rust por su seguridad y eficiencia en el manejo de memoria.

Requerimientos
Cada estación cuenta con entre 10 y 20 slots que pueden albergar una bicicleta. Cuando se encuentran vacíos pueden detectar una bicicleta que se acerca (se simula con un input), tomarla y asegurarla, liberando al usuario y cobrando un monto proporcional al tiempo que fue utilizada.

Al acercarse un usuario con la app abierta a un slot ocupado con una bicicleta, la misma se comunica para asignarle la bicicleta y liberarla, pre-autorizando un monto de seguridad en su tarjeta de crédito. (Simular mediante una conexión socket)

El usuario puede visualizar en la app estaciones cercanas y la cantidad de bicicletas y lugares disponibles para estacionar en ese momento.

Todo el sistema debe poder funcionar de forma desconectada. Si un usuario no tiene señal celular debe poder igualmente alquilar y devolver bicicletas. De igual forma si una estación pierde conectividad. Por ejemplo un usuario puede ver un lugar vacio y devolver su bicicleta en una estación, por más que la misma se encuentre sin conectividad.

El sistema debe intentar minimizar la cantidad de mensajes que viajan por toda la red; por ejemplo enviando mensajes sólo a los nodos que se encuentren cercanos por ubicación.

Requerimientos no funcionales
Los siguientes son los requerimientos no funcionales para la resolución de los ejercicios:

El proyecto deberá ser desarrollado en lenguaje Rust, usando las herramientas de la biblioteca estándar.
Por lo menos alguna, si no todas las aplicaciones implementadas deben funcionar utilizando el modelo de actores.
Cada instancia de cada aplicación debe ejecutarse en un proceso independiente.
En el modelado de la solución se deberán utilizar al menos dos de las herramientas de concurrencia distribuida mostradas en la cátedra. Por ejemplo: exclusiones mutuas distribuidas, elección de líder, algoritmos de anillo, commits de dos fases, etc.
Todas las entradas y salidas de las aplicaciones deberán ser sobre consola, o en su defecto archivos de texto (csv, json, etc.). En ningún caso se utilizarán GUIs, TUIs o cualquier otro tipo de interfáz.
No se permite utilizar crates externos, salvo los explícitamente mencionados en este enunciado, los utilizados en las clases, o los autorizados expresamente por los profesores.
El código fuente debe compilarse en la última versión stable del compilador y no se permite utilizar bloques unsafe.
El código deberá funcionar en ambiente Unix / Linux.
El programa deberá ejecutarse en la línea de comandos.
La compilación no debe arrojar warnings del compilador, ni del linter clippy.
Las funciones y los tipos de datos (struct) deben estar documentadas siguiendo el estándar de cargo doc.
El código debe formatearse utilizando cargo fmt.
Cada tipo de dato implementado debe ser colocado en una unidad de compilación (archivo fuente) independiente.
Entregas
La resolución del presente proyecto es en grupos de tres integrantes. No se aceptan grupos de otra cantidad de miembros.

Las entregas del proyecto se realizarán mediante Github Classroom. Cada grupo tendrá un repositorio disponible para hacer diferentes commits con el objetivo de resolver el problema propuesto.

Primera entrega: Diseño
Deberán entregar un informe en formato Markdown en el README.md del repositorio que contenga una explicación del diseño y de las decisiones tomadas para la implementación de la solución, asi como diagramas de threads y procesos, y la comunicación entre los mismos; y diagramas de las entidades principales.

De cada entidad se debe describir:

Finalidad general
Su estado interno, como uno o varios structs en pseudocodigo de Rust.
Mensajes que recibe, struct del payload de los mismos y como reaccionará cuando los recibe.
Mensajes que envía, struct del payload de los mismos y hacia quienes son enviados.
Protolos de transporte (TCP/UDP) y de aplicación que utiliza para comunicarse.
Casos de interés (felices, caídas)
Se recomienda fuertemente que las implementaciones de las ideas de diseño se encuentren realizadas mínimamente, para validar el mismo.

Además, validar periódicamente las mismas con su ayudante antes de la presentación final.

Fecha máxima de Entrega: 27 de Mayo de 2026

Presentar como un pull-request del README.md.

El diseño será evaluado por la cátedra y de no presentarse a término el trabajo quedará automaticamente desaprobado.

La cátedra podrá solicitar correciones que deberán realizarse en el mismo diseño, y puntos de mejora que deberán tenerse en cuenta durante la implementación.

Se podrán hacer commits hasta el día de la entrega a las 19 hs Arg, luego el sistema automáticamente quitará el acceso de escritura.

Segunda entrega: Código final - 16 de Junio de 2026
Deberá incluirse el código de la solución completa.
Deberá actualizarse el readme, con una sección dedicada a cambios que se hayan realizado desde la primera entrega. Además debe incluir cualquier explicación y/o set de comandos necesarios para la ejecución de los programas.
Presentación final: 16, 17, 23 y 24 de Junio de 2026.
Cada grupo presentará presencialmente a un profesor de la cátedra su trabajo. La fecha y horario para cada grupo serán definidos oportunamente.

La presentación deberá incluir un resumen del diseño de la solución y la muestra en vivo de las aplicaciones funcionando, demostrando casos de interes.

Todos los integrantes del grupo deberán exponer algo. La evaluación es individual.

La cátedra podrá solicitar luego correcciones donde se encuentren errores severos, sobre todo en el uso de herramientas de concurrencia; o bien desviaciones respecto al diseño pactado inicialmente.

De tener correciones para realizar, las mismas deben realizarse y aprobarse con anterioridad a la presentación a examen final.

Evaluación
Calificación
La calificación del trabajo individual y se evaluará la participación de cada miembro del grupo tanto en el desarrollo como en la presentación. La calificación final del mismo será nomimal y podrá ser INSUFICIENTE, REENTREGADO, APROBADO o DESTACADO; modulando la nota del alumno en el parcial para el cierre de la nota de cursada. INSUFICIENTE implica la desaprobación y necesidad de recursar la materia.

Principios teóricos y corrección de bugs
Los estudiantes presentarán el diseño, código y comportamiento en vivo de su solución, con foco en el uso de las diferentes herramientas de concurrencia.

Deberán poder explicar, desde los conceptos teóricos vistos en clase, cómo se comportará potencialmente su solución ante problemas de concurrencia (por ejemplo ausencia de deadlocks).

En caso de que la solución no se comportara de forma esperada, deberán poder explicar las causas y sus posibles rectificaciones.

Casos de prueba
Se someterá a la aplicación a diferentes casos de prueba que validen la correcta aplicación de las herramientas de concurrencia y la resiliencia de las distintas entidades.

Informe
El README de su repositorio funcionará como informe del trabajo y deberá contener toda la información requerida tanto para la primera como para la segunda entrega.

Organización del código
El código debe organizarse respetando los criterios de buen diseño y en particular aprovechando las herramientas recomendadas por Rust. Se prohibe el uso de bloques unsafe.

Tests automatizados
La entrega debe contar con tests automatizados que prueben diferentes casos. Se considerará en especial aquellos que pongan a prueba el uso de las herramientas de concurrencia.

Presentación en término
El trabajo deberá entregarse para la fecha estipulada. La presentación fuera de término sin coordinación con antelación con el profesor influirá negativamente en la nota final.