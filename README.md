# GeoStreamer

Утилита для потоковой обработки CSV файлов с именованными сущностями, обогащения их геоданными из Manticore Search.

## Назначение

Обрабатывает CSV файлы вида:
```
doc_id|entity_type|entity_text|confidence|start_pos|end_pos
6056452479959192749|LOC|Египет|1.0|769|775
6056452479959192749|LOC|Гелиополя|0.9952|812|821
6056452479959192749|PER|Плутарх|0.6273|839|846
```

Для каждой сущности типа LOC (настраивается) выполняет поиск в Manticore и сохраняет уникальные геохеши по doc_id.

## Режимы работы

### Базовый запуск
```bash
./geostreamer -csv data.csv
```

### С фильтрацией по типам сущностей
```bash
# Только LOC
./geostreamer -csv data.csv -filter-types LOC

# Несколько типов
./geostreamer -csv data.csv -filter-types "LOC,PER,ORG"
```

### С сохранением отладочной информации
```bash
./geostreamer \
  -csv data.csv \
  -filter-types LOC \
  -output-failures failures.ndjson \
  -output-skipped skipped.ndjson
```

### Версионирование

Проект использует семантическое версионирование. Текущая версия: **1.0.0**

Для просмотра версии:
```bash
./geostreamer -version
# GeoStreamer v1.0.0-1-gd716e88-dirty (commit: d716e88, built: 2026-03-12_08:20:53)
```
## Параметры запуска

### Параметры CSV
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-csv` | путь к входному CSV файлу | `data.csv` |
| `-delim` | разделитель полей | `\|` |
| `-csv-batch` | максимальный размер батча (в строках) | `500` |
| `-csv-min-batch` | минимальный размер батча для отправки | `100` |
| `-checkpoint` | путь к файлу чекпоинта | `geostreamer.checkpoint` |
| `-strict` | останов при ошибках | `false` |

### Параметры Manticore
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-manticore-url` | URL Manticore Search | `http://localhost:9308` |
| `-manticore-index` | название индекса | `geoname_dict` |
| `-manticore-timeout` | таймаут запроса | `60s` |
| `-manticore-retries` | количество повторов | `3` |
| `-manticore-retry-delay` | задержка между повторами | `2s` |
| `-manticore-batch` | размер батча запросов | `500` |
| `-manticore-workers` | количество воркеров | `10` |
| `-manticore-parallel` | параллельных запросов на батч | `5` |
| `-manticore-cache-size` | размер кэша запросов | `10000` |
| `-manticore-cache-ttl` | время жизни кэша | `1h` |

### Параметры вывода
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-output` | путь к выходному NDJSON файлу | `results.ndjson` |
| `-output-failures` | путь к файлу с ошибками | `failures.ndjson` |
| `-output-skipped` | путь к файлу с пропущенными | `skipped.ndjson` |
| `-output-flush` | интервал сброса буфера | `5s` |
| `-output-buffer` | размер буфера записи | `1048576` |
| `-output-gzip` | сжатие выходного файла | `false` |

### Параметры логирования
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-log-level` | уровень логирования (debug/info/warn/error) | `info` |
| `-log-file` | путь к файлу лога | (stdout) |
| `-log-stats` | интервал вывода статистики | `10s` |
| `-debug` | режим отладки (включает debug логи) | `false` |

## Форматы выходных файлов

### results.ndjson (основной результат)
Каждая строка - JSON объект с уникальными геохешами для doc_id:
```json
{
  "doc_id": "6056452479959192749",
  "geohashes_string": ["1443f4", "38b1ac"],
  "geohashes_uint64": [1460278985035350016, -8519469090799091712]
}
```

### failures.ndjson (ошибочные запросы)
```json
{
  "timestamp": "2026-03-09T17:52:46.358139731+03:00",
  "csv_record": {
    "DocID": "6056452479959189958",
    "EntityType": "LOC",
    "EntityText": "Загородному проспекту",
    "Confidence": 0.9724,
    "StartPos": 1260,
    "EndPos": 1281,
    "LineNum": 303305
  },
  "query_info": {
    "text": "Загородному проспекту",
    "query": "SELECT * FROM geoname_dict WHERE match('\"^Загородному проспекту$\"')",
    "attempts": 1,
    "worker_id": 9,
    "duration_ms": 1112404,
    "timestamp": "2026-03-09T17:52:46.358139731+03:00",
    "hit_count": 0,
    "response": "{\"hits\":{\"hits\":[],\"total\":0}}",
    "error": "context deadline exceeded",
    "http_status": "504 Gateway Timeout"
  },
  "worker_id": 9
}
```

### skipped.ndjson (пропущенные записи)
Для выбранных типов сущностей (например, LOC), которые не найдены в Manticore:
```json
{
  "timestamp": "2026-03-09T17:52:52.364440871+03:00",
  "csv_record": {
    "DocID": "6056452479959189958",
    "EntityType": "LOC",
    "EntityText": "Париже",
    "Confidence": 0.9999,
    "StartPos": 1801,
    "EndPos": 1808,
    "LineNum": 303316
  },
  "reason": "not_found_in_manticore",
  "query_info": {
    "text": "Париже",
    "query": "SELECT * FROM geoname_dict WHERE match('\"^Париже$\"')",
    "attempts": 1,
    "worker_id": 4,
    "duration_ms": 156,
    "timestamp": "2026-03-09T17:52:46.358157574+03:00",
    "hit_count": 0,
    "response": "{\"hits\":{\"hits\":[],\"total\":0}}"
  },
  "worker_id": 4
}
```

## Статистика выполнения

### Периодическая статистика (каждые 10 секунд)
```
2026/03/09 23:55:48 [INFO] Orchestrator === STATISTICS ===
2026/03/09 23:55:48 [INFO] Orchestrator Entity types: [LOC]
2026/03/09 23:55:48 [INFO] Orchestrator Processed (with geohashes): 172095 records
2026/03/09 23:55:48 [INFO] Orchestrator Skipped (not found in Manticore): 69326 records
2026/03/09 23:55:48 [INFO] Orchestrator Filtered (other entity types): 584764 records
2026/03/09 23:55:48 [INFO] Orchestrator Written (unique doc_id): 38270 records
2026/03/09 23:55:48 [INFO] Orchestrator Bytes written: 64846342
2026/03/09 23:55:48 [INFO] Orchestrator Manticore queries: 82874 success, 0 failures
2026/03/09 23:55:48 [INFO] Orchestrator Rate: 9179.81 records/sec
2026/03/09 23:55:48 [INFO] Orchestrator =================
```

### Финальная статистика (по завершении)
```
2026/03/09 23:55:55 [INFO] Orchestrator === FINAL STATISTICS ===
2026/03/09 23:55:55 [INFO] Orchestrator Entity types processed: [LOC]
2026/03/09 23:55:55 [INFO] Orchestrator Total processing time: 1m36.812863154s
2026/03/09 23:55:55 [INFO] Orchestrator Records processed (with geohashes): 174540
2026/03/09 23:55:55 [INFO] Orchestrator Records skipped (not found in Manticore): 70012
2026/03/09 23:55:55 [INFO] Orchestrator Records filtered (other types): 589302
2026/03/09 23:55:55 [INFO] Orchestrator Records written (unique doc_id): 38874
2026/03/09 23:55:55 [INFO] Orchestrator Bytes written: 65658650
2026/03/09 23:55:55 [INFO] Orchestrator Manticore queries: 83568 success, 0 failures
2026/03/09 23:55:55 [INFO] Orchestrator Processing rate: 8613.05 records/sec
2026/03/09 23:55:55 [INFO] Orchestrator ========================
```

### Пояснение полей статистики
| Поле | Описание |
|------|----------|
| **Entity types** | Типы сущностей, выбранные для обработки |
| **Processed** | Количество записей выбранного типа, для которых найдены геохеши |
| **Skipped** | Записи выбранного типа, не найденные в Manticore |
| **Filtered** | Записи других типов (PER, ORG), исключенные фильтром |
| **Written** | Количество уникальных doc_id, записанных в выходной файл |
| **Bytes written** | Объем записанных данных |
| **Manticore** | Статистика запросов к Manticore (успешно/ошибки) |
| **Rate** | Скорость обработки записей в секунду |
| **Total processing time** | Общее время выполнения |

## Проверка целостности результатов

### Уникальность doc_id в выходном файле
```bash
# Количество записей в файле должно равняться количеству уникальных doc_id
wc -l results.ndjson
jq -r '.doc_id' results.ndjson | sort -u | wc -l

# Проверка на дубликаты (должно быть 0)
jq -r '.doc_id' results.ndjson | sort | uniq -d | wc -l
```

### Проверка соответствия статистики
```bash
# Общее количество строк в исходном файле
wc -l testdata/test.csv

# Распределение по типам сущностей (по строкам)
tail -n +2 testdata/test.csv | cut -d'|' -f2 | sort | uniq -c

# Распределение по типам сущностей (по уникальным текстам)
tail -n +2 testdata/test.csv | cut -d'|' -f2,3 | sort -u | cut -d'|' -f1 | sort | uniq -c
```

## Тестовые данные

### Характеристики тестового файла (100k параграфов)
```
$ wc -l testdata/test.csv
833855 testdata/test.csv

$ tail -n +2 testdata/test.csv | cut -d'|' -f2 | sort | uniq -c
 244552 LOC
  85626 ORG
 503676 PER

$ tail -n +2 testdata/test.csv | cut -d'|' -f2,3 | sort -u | cut -d'|' -f1 | sort | uniq -c
  54567 LOC
  35652 ORG
 156217 PER
```

### Статистика по документам
```
Всего doc_id: 68,828
Всего записей: 833,854
Среднее: 12.12 записей на doc_id
Минимум: 1
Максимум: 252

Распределение:
- 10% документов: 1 запись
- 25% документов: до 3 записей
- 50% документов: до 8 записей (медиана)
- 75% документов: до 17 записей
- 90% документов: до 29 записей
- 95% документов: до 38 записей
- 99% документов: до 58 записей
```

## Анализ производительности

### Характеристики тестового запуска
- **Объем входных данных**: 833 855 записей
- **Типы сущностей в файле**: LOC, PER, ORG
- **Режим обработки**: только LOC (фильтрация по типу)
- **Параметры**: 30 воркеров, батчи по 500 записей, 30 параллельных запросов
- **Время обработки**: 1 минута 40 секунд
- **Скорость обработки**: **8,359 записей/сек**

### Результаты обработки
Из 833 855 записей:
- **LOC сущности**: 244,552 записи (29% от общего объема)
  - Найдено в Manticore и обработано: **236,793**
  - Не найдено (skipped): **7,759**
- **PER/ORG сущности**: 589,302 записи (71% от общего объема)
  - Отфильтровано: **589,302**

### Эффективность
- **Успешных запросов к Manticore**: 83,631
- **Ошибок запросов**: 0
- **Уникальных doc_id с геохешами**: 38,875
- **Объем выходных данных**: ~65 MB

### Масштабирование
При увеличении объема входных данных в 380 раз (до ~300 млн записей):
```
Прогнозируемое время: ~10-12 часов
Рекомендуемые настройки: 
  - воркеров: 50-100
  - батчи: 500-800 записей
  - параллельных запросов: 20-30

Ожидаемый объем выходных данных: ~24 GB (в сжатом виде ~4-5 GB)
```

Система линейно масштабируется за счет:
- Потокового чтения без загрузки всего файла в память
- Настраиваемого параллелизма запросов к Manticore
- Эффективного кэширования повторяющихся entity_text
- Буферизированной записи результатов
- Интеллектуального формирования батчей с гарантией целостности doc_id

## Особенности работы

1. **Чекпоинты** - автоматическое сохранение прогресса позволяет прерывать и возобновлять обработку с места остановки
2. **Фильтрация** - обрабатываются только указанные типы сущностей (по умолчанию LOC)
3. **Параллелизм** - настраиваемое количество воркеров и параллельных запросов
4. **Кэширование** - результаты запросов кэшируются для повторяющихся entity_text
5. **Буферизация** - все операции чтения/записи буферизированы для производительности
6. **Graceful shutdown** - корректное завершение при получении SIGINT/SIGTERM
7. **Целостность данных** - гарантированная уникальность doc_id в выходном файле

## Примеры использования

### Обработка большого файла с оптимальными настройками
```bash
./geostreamer \
  -csv /data/entities.csv \
  -filter-types LOC \
  -manticore-workers 20 \
  -manticore-batch 500 \
  -manticore-parallel 10 \
  -manticore-cache-size 50000 \
  -output results.ndjson \
  -output-failures failures.ndjson \
  -output-skipped skipped.ndjson \
  -log-stats 30s
```

### Продолжение прерванной обработки
```bash
# Автоматически использует checkpoint файл
./geostreamer -csv /data/entities.csv -filter-types LOC
```

### Отладка проблемных запросов
```bash
./geostreamer \
  -csv test.csv \
  -filter-types LOC \
  -manticore-debug \
  -log-level debug \
  -output-failures failures.ndjson
```

### Проверка результатов после обработки
```bash
# Проверка уникальности
echo "Уникальных doc_id: $(jq -r '.doc_id' results.ndjson | sort -u | wc -l)"
echo "Всего записей: $(wc -l < results.ndjson)"
echo "Дубликатов: $(jq -r '.doc_id' results.ndjson | sort | uniq -d | wc -l)"

# Статистика по геохешам
jq -r '.geohashes_string | length' results.ndjson | sort -n | uniq -c | tail -5
```

## Итоговые метрики

| Показатель | Значение |
|------------|----------|
| **Входные данные** | 833,854 записи |
| **Время обработки** | 1м 36с |
| **Скорость** | 8,613 записей/сек |
| **LOC найдено** | 174,540 |
| **LOC пропущено** | 70,012 |
| **PER/ORG отфильтровано** | 589,302 |
| **Уникальных doc_id** | 38,874 |
| **Запросов к Manticore** | 83,568 |
| **Ошибок** | 0 |

## Требования к Manticore

Необходим индекс с полями:
- `id` (bigint)
- `name` (string)
- `geohashes_string` (string)
- `geohashes_uint64` (multi-value bigint)
- `occurrences` (uint)
- `first_geoname_id` (bigint)

Запросы выполняются в формате:
```sql
SELECT * FROM geoname_dict WHERE match('"^entity_text$"')
```

## Архитектура

Проект следует принципам гексагональной архитектуры с четким разделением:
- **domain** - бизнес-сущности и правила
- **ports** - интерфейсы для взаимодействия
- **adapters** - реализации интерфейсов (CSV, Manticore, JSON)

## Требования к окружению

- Go 1.25+
- Manticore Search (для работы с геоданными)
- Доступ к Manticore по HTTP (по умолчанию порт 9308)
