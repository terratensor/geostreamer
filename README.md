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

Для каждой сущности выполняет поиск в Manticore и сохраняет геоданные. Поддерживает три режима вывода.

## Режимы вывода

### Режим 1: Только геохеши (results.ndjson)
```bash
./geostreamer -csv data.csv -output results.ndjson
```
```json
{"doc_id":"6056452479959192749","geohashes_string":["1443f4","38b1ac"],"geohashes_uint64":[1460278985035350016,-8519469090799091712]}
```

### Режим 2: Только NER (ner.ndjson)
```bash
./geostreamer -csv data.csv -output-ner ner.ndjson
```
```json
{"doc_id":"6056452479959192749","ner_loc":[{"value":"Египет","start_pos":769,"end_pos":775,"geohash":["1443f4"],"confidence":1}],"ner_per":[{"value":"Плутарх","start_pos":839,"end_pos":846,"geohash":[],"confidence":0.6273}],"ner_org":[]}
```

### Режим 3: Полный (enriched.ndjson)
```bash
./geostreamer -csv data.csv -output-enriched enriched.ndjson
```
```json
{"doc_id":"6056452479959192749","geohashes_string":["1443f4","38b1ac"],"geohashes_uint64":[1460278985035350016,-8519469090799091712],"ner_loc":[...],"ner_per":[...],"ner_org":[...]}
```

### Комбинированный режим (все три сразу)
```bash
./geostreamer \
  -csv data.csv \
  -output results.ndjson \
  -output-ner ner.ndjson \
  -output-enriched enriched.ndjson
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
| `-output` | путь к файлу с геохешами (режим 1) | `results.ndjson` |
| `-output-ner` | путь к файлу с NER (режим 2) | (не задан) |
| `-output-enriched` | путь к полному файлу (режим 3) | (не задан) |
| `-output-failures` | путь к файлу с ошибками | `failures.ndjson` |
| `-output-skipped` | путь к файлу с пропущенными | `skipped.ndjson` |
| `-output-flush` | интервал сброса буфера | `5s` |
| `-output-buffer` | размер буфера записи | `1048576` |
| `-output-gzip` | сжатие выходного файла | `false` |

### Параметры фильтрации
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-filter-types` | типы сущностей для обработки | `LOC` |

### Параметры логирования
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-log-level` | уровень логирования (debug/info/warn/error) | `info` |
| `-log-file` | путь к файлу лога | (stdout) |
| `-log-stats` | интервал вывода статистики | `10s` |
| `-debug` | режим отладки (включает debug логи) | `false` |
| `-version` | вывод информации о версии | `false` |

## Форматы выходных файлов

### results.ndjson (режим 1 - только геохеши)
```json
{
  "doc_id": "6056452479959192749",
  "geohashes_string": ["1443f4", "38b1ac"],
  "geohashes_uint64": [1460278985035350016, -8519469090799091712]
}
```

### ner.ndjson (режим 2 - только NER)
```json
{
  "doc_id": "6056452479959192749",
  "ner_loc": [
    {"value": "Египет", "start_pos": 769, "end_pos": 775, "geohash": ["1443f4"], "confidence": 1},
    {"value": "Гелиополя", "start_pos": 812, "end_pos": 821, "geohash": [], "confidence": 0.9952}
  ],
  "ner_per": [
    {"value": "Плутарх", "start_pos": 839, "end_pos": 846, "geohash": [], "confidence": 0.6273}
  ],
  "ner_org": []
}
```

### enriched.ndjson (режим 3 - полный)
Объединяет оба предыдущих формата в одном файле.

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
| **Filtered** | Записи других типов, исключенные фильтром |
| **Written** | Количество уникальных doc_id, записанных в выходной файл |
| **Bytes written** | Объем записанных данных |
| **Manticore** | Статистика запросов к Manticore |
| **Rate** | Скорость обработки записей в секунду |

## Проверка целостности результатов

### Уникальность doc_id в выходном файле
```bash
# Количество записей должно равняться количеству уникальных doc_id
wc -l results.ndjson
jq -r '.doc_id' results.ndjson | sort -u | wc -l

# Проверка на дубликаты (должно быть 0)
jq -r '.doc_id' results.ndjson | sort | uniq -d | wc -l
```

### Проверка соответствия статистики
```bash
# Общее количество строк в исходном файле
wc -l testdata/test.csv

# Распределение по типам сущностей
tail -n +2 testdata/test.csv | cut -d'|' -f2 | sort | uniq -c
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
```

### Статистика по документам
```
Всего doc_id: 68,828
Всего записей: 833,854
Среднее: 12.12 записей на doc_id
Максимум: 252 записи
99% документов: до 58 записей
```

## Версионирование

Проект использует семантическое версионирование. Текущая версия: **1.2.0**

```bash
# Просмотр версии
./geostreamer -version

# Сборка с указанием версии
make build VERSION=v1.2.0
```

## Архитектура

Проект следует принципам гексагональной архитектуры:
- **domain** - бизнес-сущности и правила
- **ports** - интерфейсы для взаимодействия
- **adapters** - реализации интерфейсов

## Требования к окружению

- Go 1.25+
- Manticore Search (порт 9308)
- Доступ к Manticore по HTTP
