## Домашняя работа 7
#### Настроите применение предобученной модели в Spark Structured Streaming

Набор данных в формате CSV: https://www.kaggle.com/arshid/iris-flower-dataset

Набор данных в формате LIBSVM: https://github.com/apache/spark/blob/master/data/mllib/iris_libsvm.txt

Описание набора данных: https://ru.wikipedia.org/wiki/%D0%98%D1%80%D0%B8%D1%81%D1%8B_%D0%A4%D0%B8%D1%88%D0%B5%D1%80%D0%B0


#### Задание 1:
1) Построить модель классификации Ирисов Фишера и сохранить её

#### Задание 2:
2) Разработать приложение, которое читает из одной темы Kafka (например, "input") CSV-записи с четырми признаками ирисов, и возвращает в другую тему (например, "predictition") CSV-записи с теми же признаками и классои ириса
   Должен быть предоставлен код программы

### Запуск приложения из Idea:
Укажем в опцию при запуске "include dependencies with "Provided" scope", потом run src/main/scala/Main.scala

Предварительно
```
docker-compose up -d
```

Создать топики 
```
docker exec hw7_spark_streaming_broker_1 kafka-topics \
  --create \
  --zookeeper zookeeper:2181 \
  --replication-factor 1 \
  --partitions 1 \
  --topic input

docker exec hw7_spark_streaming_broker_1 kafka-topics \
  --create \
  --zookeeper zookeeper:2181 \
  --replication-factor 1 \
  --partitions 1 \
  --topic prediction
```
Отправить записи для топика input

```
docker exec hw7_spark_streaming_broker_1 bash -c "chmod +x /scripts/output.sh"
 
docker exec hw7_spark_streaming_broker_1 bash -c "/scripts/output.sh | kafka-console-producer --bootstrap-server localhost:9092 --topic input"
```

Посмотрим на результат работы стрима с моделью
```
docker exec hw7_spark_streaming_broker_1 kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic input

docker exec hw7_spark_streaming_broker_1 kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic prediction
```
