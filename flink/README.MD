### Итоговая работа
#### Схема и описание работы: https://docs.google.com/presentation/d/1-Pi8UwlyFVyhVUpm-slEHMNN9wem6hfyEH5MqLkzono/edit?usp=sharing

#### За основу генерации данных был взят следующий датасет https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

#### Запуск
```
Из папки ./docker:
docker-compose build
docker-compose up -d
```

#### Текущее состояние компонентов:
1. Реализована запись в hdfs для batch layer
2. Реализована запись в эластик для speed layer
3. Подготовлен базовый дашборд kibana для serving layer
