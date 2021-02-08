## Домашняя работа 1 

1) Загрузите файл с географическими данными различных стран
(https://raw.githubusercontent.com/mledoze/countries/master/countries.json)
2) Среди стран выберите 10 стран Африки с наибольшей площадью
3) Запишите данные о выбранных странах в виде JSON-массива объектов
следующей структуры:
``
[{
“name”: <Официальное название страны
на английском языке, строка>,
“capital”: <Название столицы,
строка>(если столиц перечисленно
несколько, выберите первую),
“area”: <Площадь страны в квадратных
километрах, число>,
}]
``

4) Обеспечьте проект инструкциями для сборки JAR-файла, принимающего на
вход имя выходного файла и осуществляющего запись в него

### Сборка и запуск приложения:
Предварительно установить scala 2.12, sbt 1.\*.\*, java1.8
1) Собрать executable jar в папке с проектом `sbt assembly`
2) После окончания сборки будет указан путь, где расположен файл 
```
[info] Packaging target/africa-assembly-1.0.jar
```
3) Запуск джобы осуществляется через scala интерпретатор. Нужно передать путь до jar файла, который собрали на шаге 2, и передать аргумент в виде: полный путь + имя выходного файла.

Пример запуска:
```
scala /path/to/assembly-jar /opt/otus-homework/1/africa.json
```