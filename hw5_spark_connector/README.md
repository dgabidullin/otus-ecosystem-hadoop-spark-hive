## Домашняя работа 5
Разработаете собственный коннектор на Spark
### Доработать data source для Postgres для партиционированного чтения.
1. Склонируйте репозиторий https://github.com/Gorini4/spark_datasource_example
2. Доработайте код в файле src/main/scala/org/example/datasource/postgres/PostgresDatasource.scala так, чтобы тест в файле src/test/scala/org/example/PostgresqlSpec.scala при выполнении читал таблицу users не в одну партицию, а в несколько (размер одной партиции должен задаваться через метод .option("partitionSize", "10")).
