# Лабораторная работа №13: Мультиагентные системы. Разработка распределённых интеллектуальных агентов

## Информация о студенте

| Параметр | Значение |
|----------|----------|
| **ФИО** | Евстигнеев Фёдор Алексеевич |
| **Группа** | 220032-11 |
| **Лабораторная работа** | №13 |
| **Вариант** | №5 - Мониторинг IT-инфраструктуры |
| **Сложность** | Повышенная |

## Описание системы

Разработана мультиагентная система для мониторинга IT-инфраструктуры.

### Агенты и их роли

| Агент | Роль | Вход | Выход |
|-------|------|------|-------|
| **Collector** | Сбор метрик со служб | Список сервисов | Метрики CPU |
| **Analyzer** | Анализ метрик, детекция аномалий | Метрика + пороги | Severity (normal/warning/critical) |
| **Alerter** | Оповещение при аномалиях | Alert | Уведомление |
| **Recovery** | Автоматическое восстановление | Service + issue | Действие (restart/escalate) |

### Бизнес-правила

1. Collector собирает метрики каждые 5 секунд, сохраняет счётчик задач в Redis
2. Analyzer: если value >= critical → critical, если value >= warning → warning
3. Alerter: при critical отправляет срочное уведомление
4. Recovery: 1 попытка → restart, 3+ попытки → эскалация on-call

## Выполненные задания

| № | Задание | Статус |
|---|---------|--------|
| 1 | Разработка 4 агентов на Go | ✅ |
| 2 | Цепочки задач (pipeline) | ✅ |
| 3 | Распределённая трассировка (Jaeger) | ✅ |
| 4 | Агент с состоянием (Redis) | ✅ |
| 5 | Динамическое масштабирование | ✅ |
| 6 | Аукционное распределение | ✅ |
| 7 | Интеграция LLM-агента (Ollama) | ✅ |
| 8 | Веб-интерфейс мониторинга | ✅ |

## Архитектура
Collector (Go) ──► Analyzer (Go) ──► Alerter (Go)
│ │
▼ ▼
Redis Jaeger
│ │
▼ ▼
NATS ◄────── Orchestrator (Python) ──────► FastAPI
│
▼
Recovery (Go) ──► LLM Agent (Python + Ollama)


## Технологический стек

| Компонент | Технология |
|-----------|------------|
| Агенты | Go 1.21 |
| Оркестратор | Python 3.10 + asyncio |
| Message Broker | NATS |
| State Storage | Redis |
| Tracing | Jaeger + OpenTelemetry |
| LLM | Ollama (llama2) |
| API | FastAPI |
| Контейнеризация | Docker Compose |

## Быстрый старт

### 1. Запуск инфраструктуры

docker-compose -f docker/docker-compose.yml up -d

### 2. Установка зависимостей

cd agent_go && go mod download && cd ..
cd orchestrator && pip install -r requirements.txt && cd ..

### 3. Запуск Go-агентов

cd agent_go/collector && go run main.go &
cd agent_go/analyzer && go run main.go &
cd agent_go/alerter && go run main.go &
cd agent_go/recovery && go run main.go &

### 4. Запуск дополнительных компонентов

# Динамическое масштабирование
cd orchestrator/scaler && python dynamic_scaler.py &

# Аукцион
cd orchestrator && python auction_manager.py &

# LLM агент (скачать модель первый раз)
docker exec -it ollama ollama pull llama2
cd orchestrator/llm_agent && python llm_analyzer.py &

### 5. Запуск оркестратора и API

cd orchestrator
python orchestrator.py &
uvicorn api:app --reload --port 8000

## Использование API

### Запрос на мониторинг

curl -X POST http://localhost:8000/api/v1/monitor/run \
  -H "Content-Type: application/json" \
  -d '{
    "services": ["api-gateway", "auth-service", "payment-service"],
    "thresholds": {
      "api-gateway": {"warning": 75, "critical": 92},
      "auth-service": {"warning": 70, "critical": 88}
    }
  }
  '
### Ответ

{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "metrics_collected": 3,
  "alerts": [...],
  "recoveries": [...],
  "status": "success"
}

### Health check

curl http://localhost:8000/api/v1/health

## Мониторинг

Сервис	URL
Jaeger UI (трассировки)	http://localhost:16686
NATS мониторинг	http://localhost:8222
Swagger API	http://localhost:8000/docs
Ollama API	http://localhost:11434

## Тестирование

cd orchestrator
pytest test_orchestrator.py -v

Ожидаемый результат:
collected 3 items
test_orchestrator.py::test_send_task_timeout PASSED
test_orchestrator.py::test_on_result_sets_future PASSED
test_orchestrator.py::test_run_pipeline_success PASSED

## Структура репозитория

lab13_variant5/
├── .gitignore
├── README.md
├── PROMPT_LOG.md
├── Makefile
├── docker/
│   └── docker-compose.yml
├── agent_go/
│   ├── models/task.go
│   ├── collector/main.go
│   ├── analyzer/main.go
│   ├── alerter/main.go
│   ├── recovery/main.go
│   └── auction/auction_agent.go
└── orchestrator/
    ├── orchestrator.py
    ├── api.py
    ├── test_orchestrator.py
    ├── auction_manager.py
    ├── scaler/dynamic_scaler.py
    └── llm_agent/llm_analyzer.py