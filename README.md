Система рекомендаций по оптимизации Data Lakehouse
REST-сервис на основе LLM для автоматической оптимизации структуры данных и SQL-запросов в архитектуре Trino + Iceberg + S3.
Сервис анализирует DDL, частоту выполнения запросов и предлагает:
- Денормализацию часто джойнящихся таблиц,
- Партиционирование,
- Создание плоских таблиц для аналитики,
- Оптимизированные SQL-запросы с сохранением `queryid`.

Входные данные
Сервис принимает POST-запрос на `/new` в формате:
```json
{
  "url": "jdbc:trino://host:8080/catalog?user=...&password=...",
  "ddl": [
    { "statement": "CREATE TABLE catalog.schema.table1 (...)" },
    { "statement": "CREATE TABLE catalog.schema.table2 (...)" }
  ],
  "queries": [
    {
      "queryid": "0197a0b2-...",
      "query": "SELECT ... FROM catalog.schema.table1 JOIN ...",
      "runquantity": 1250
    }
  ]
}

{
  "ddl": [
    { "statement": "CREATE SCHEMA catalog.optimized" },
    { "statement": "CREATE TABLE catalog.optimized.flat_table (...)" }
  ],
  "migrations": [
    { "statement": "INSERT INTO catalog.optimized.flat_table SELECT ..." }
  ],
  "queries": [
    {
      "queryid": "0197a0b2-...",
      "query": "SELECT ... FROM catalog.optimized.flat_table ..."
    }
  ]
}

```
 Требования к формату:

Все таблицы — в виде catalog.schema.table.
Первая DDL — всегда CREATE SCHEMA.
queryid сохраняются без изменений.

Запуск локально:
Установите библиотеки:
pip install fastapi uvicorn requests pydantic
Получите токен Hugging Face:
Зарегистрируйтесь на huggingface.co
Settings → Access Tokens → New token (роль: Read)
Скопируйте токен вида hf_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Запустите сервис:
HF_TOKEN="ваш_токен" uvicorn main:app --reload
Перейдите на http://127.0.0.1:8000/docs

Метод: POST
Путь: /new
Описание: Запуск задачи → возвращает { "taskid": "..." }

Метод: GET
Путь: /status?task_id=...
Описание: Запрос статуса задачи. Возможные значения:

RUNNING — задача выполняется,
DONE — задача завершена,
FAILED — задача завершилась с ошибкой.
Метод: GET
Путь: /getresult?task_id=...
Описание: Получение результата задачи. Доступно только при статусе DONE.
