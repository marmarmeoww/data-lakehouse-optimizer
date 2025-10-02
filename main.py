# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import uuid
import threading
import logging
import os

# Для работы с Hugging Face API
import requests
import json

app = FastAPI()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Хранилище задач (в реальном проекте — база данных или Redis)
tasks = {}

# === Модели Pydantic ===
class DDLStatement(BaseModel):
    statement: str

class QueryItem(BaseModel):
    queryid: str
    query: str
    runquantity: int

class NewTaskRequest(BaseModel):
    url: str
    ddl: List[DDLStatement]
    queries: List[QueryItem]
    
class TaskIdResponse(BaseModel):
    taskid: str

class StatusResponse(BaseModel):
    status: str

class OptimizationResult(BaseModel):
    ddl: List[DDLStatement]
    migrations: List[DDLStatement]
    queries: List[QueryItem]

# === Эндпоинты API ===
@app.post("/new", response_model=TaskIdResponse)
def create_task(request: NewTaskRequest):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "RUNNING", "result": None}

    thread = threading.Thread(target=hf_optimize, args=(task_id, request))
    thread.start()

    return {"taskid": task_id}

@app.get("/status", response_model=StatusResponse)
def get_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": tasks[task_id]["status"]}

    
@app.get("/getresult", response_model=OptimizationResult)
def get_result(task_id: str):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] == "FAILED":
        error_detail = task.get("result", {}).get("error", "Unknown error")
        raise HTTPException(status_code=500, detail=error_detail)
    if task["status"] != "DONE":
        raise HTTPException(status_code=400, detail="Task not ready")
    
    return task["result"]   


# === Логика оптимизации через Hugging Face ===
def hf_optimize(task_id: str, request: NewTaskRequest):
    """Анализ через Hugging Face Inference API с обработкой ошибок"""
    try:
        logger.info(f"🚀 Начинаем анализ задачи {task_id}")

        # Подготовим промпт
        ddl_statements = "\n".join([d.statement for d in request.ddl])
        query_examples = "\n".join([f"{q.queryid}: {q.query} (выполняется {q.runquantity} раз)" for q in request.queries[:3]])

        prompt = f"""
Ты — эксперт по оптимизации Data Lakehouse на базе Trino + Iceberg + S3.
Проанализируй DDL и часто выполняемые SQL-запросы.
Предложи оптимизацию производительности через:
- Денормализацию таблиц, которые часто джойнятся
- Партиционирование по дате или другому полю
- Изменение типов данных
- Создание плоских таблиц для ускорения аналитики

ВАЖНО:
1. Первая команда DDL должна быть: CREATE SCHEMA <catalog>.<new_schema>
2. Все таблицы должны использовать полный путь: catalog.schema.table
3. Верни строго JSON в формате:
{{
  "ddl": [{{"statement": "..."}}, ...],
  "migrations": [{{"statement": "..."}}, ...],
  "queries": [{{"queryid": "...", "query": "..."}}, ...]
}}

DDL:
{ddl_statements}

Примеры запросов:
{query_examples}

Верни только JSON, без пояснений.
"""

        # Используем рабочую модель
        API_URL = "https://api-inference.huggingface.co/models/google/gemma-7b-it"

        # Получаем токен
        HF_TOKEN = os.getenv("HF_TOKEN")
        if not HF_TOKEN:
            raise Exception("HF_TOKEN не установлен")

        headers = {
            "Authorization": f"Bearer {HF_TOKEN}",
            "Content-Type": "application/json"
        }

        payload = {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 2048,
                "temperature": 0.3,
                "return_full_text": False
            }
        }

        logger.info("📤 Отправляем запрос к Hugging Face...")
        response = requests.post(API_URL, headers=headers, json=payload, timeout=60)

        if response.status_code != 200:
            error_msg = f"❌ HF API вернул {response.status_code}: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)

        result_text = response.json()[0]["generated_text"].strip()
        logger.info("✅ Получен ответ от модели:")
        logger.info(result_text[:500] + "...")

        # Убираем Markdown-обёртки
        if result_text.startswith("```json"):
            result_text = result_text[7:].split("```")[0].strip()

        # Пытаемся распарсить JSON
        try:
            result = json.loads(result_text)
            logger.info("✅ JSON успешно распарсен")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON: {e}")
            logger.error(f"Текст, который пытались распарсить:\n{result_text}")
            raise Exception(f"Невалидный JSON: {e}")

        # Проверяем структуру
        if not isinstance(result.get("ddl"), list):
            raise ValueError("Ответ не содержит списка ddl")
        if not isinstance(result.get("migrations"), list):
            raise ValueError("Ответ не содержит списка migrations")
        if not isinstance(result.get("queries"), list):
            raise ValueError("Ответ не содержит списка queries")

        tasks[task_id]["result"] = result
        tasks[task_id]["status"] = "DONE"
        logger.info(f"🎉 Задача {task_id} успешно завершена")

    except Exception as e:
        logger.error(f"💥 Ошибка в задаче {task_id}: {str(e)}")
        tasks[task_id]["status"] = "FAILED"
        tasks[task_id]["result"] = {
            "error": str(e),
            "message": "Не удалось выполнить оптимизацию. Проверьте входные данные и токен."
        }
