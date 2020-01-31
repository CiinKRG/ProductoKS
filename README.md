# Kafka Stream (Fork)

Las clases que se crearon fueron:

- Consumer

- GetPropertiesKafka

- GetValidations

- InputStream

- KSValidacion

- KSValidacion2

- Producer

- ReadFile

- ReadFileH

### Consumer

Consumer sencillo a un tópico de Kafka e imprime el value del mensaje sin la key.

Toma ciertos parámetros del archivo *kstream.properties* como el tópico, group id, IP y puerto, entre otros.

### GetPropertiesKafka

Clase que asigna las propiedades del archivo de configuración, se manda llamar desde la clase Consumer, Producer, KSValidacion y KSValidacion2

### GetValidations

Se implementaron los métodos usados en la clase InputStream, ReadFile y ReadFileH.

El método *FindDelimH* recibe un String que es el header; encuentra el delimitador que tenga esté, se asignaron jerarquías a los tipos de delimitadores como *"," "|" "_"* entre otros, para que sin importar el número de campos o la manera en que estén escritos se encuentre el delimitador adecuado asignando correctamente el número de campos y devuelva el delimitador.

El método *FindDelimD* recibe un String del dato a validar y el número de campos del header; este método encuentra el delimitador del dato, al igual que en el método anterior se asignaron jerarquías a los tipos de delimitadores; y al encontrar los delimitadores los compara con el número de campos del header para ver si coincide o no con el número de campos, en caso de coincidir devolver el delimitador que contiene.

El método *isJson* recibe un string y valida si es JSON.

### InputStream

Clase donde se inserta un dato y header, en la cual si no coincide indica si es por tener más o menos campos que el header y en caso de coincidir se convierte a JSON; hace uso de los métodos *FindDelimH* y *FindDelimD* de **GetValidations**

### KSValidacion

Se crea un Consumer a un tópico de Kafka, se procesan con los datos aplicando lo que se creo en la clase **InputStreams** y se manda mediante un Producer a dos tópicos diferentes; dependiendo si cumple o no con el número de campos del header asignado se convierte a JSON y se envía a un tópico, de lo contrarios se agrega al final del valor del mensaje la razón (si tenía menos o más campos) y se envía a un tópico de error.

### KSValidacion2

A partir de la clase anterior se crea un Kafka Stream en el que mediante un fork se envía a dos diferentes tópicos.

### Producer

Producer sencillo a un tópico de Kafka, toma ciertos parámetros del archivo *kstream.properties* como el tópico, group id, IP y puerto, entre otros.

### ReadFile

Clase en la que se lee un archivo sin header, detecta el tipo de archivo, compara el número de campos en el header contra el número de campos tomando la primera línea del archivo, se hace uso de los métodos *FindDelimH* y *FindDelimD* de la clase **InputStreams**; en caso de coincidir asigna el header y se convierten los datos a JSON.

### ReadFileH

Clase en la que se lee un archivo que contiene header, detecta el tipo de archivo, en caso de ser CSV o TXT busca el delimitador usando el método *FindDelimH* lo reemplaza por *","* y convierte a JSON; si el archivo es JSON lo valida usando el método *isJson* de la clase **InputStreams**
