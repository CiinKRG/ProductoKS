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

### KSValidacion

### KSValidacion2

### Producer

### ReadFile

### ReadFileH
