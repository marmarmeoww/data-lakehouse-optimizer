# data-lakehouse-optimizer
Сервис на FastAPI + LLM для автоматической оптимизации Data Lakehouse (Trino + Iceberg + S3). Анализирует DDL и SQL-запросы, предлагает денормализацию, партиционирование и миграции. Возвращает валидный JSON с сохранением queryid.
