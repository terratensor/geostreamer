## GeoStreamer

Утилита для потоковой обработки CSV файлов с именованными сущностями, обогащения их геоданными из Manticore Search.

### Назначение

Обрабатывает CSV файлы вида:
```
doc_id|entity_type|entity_text|confidence|start_pos|end_pos
6056452479959192749|LOC|Египет|1.0|769|775
6056452479959192749|LOC|Гелиополя|0.9952|812|821
6056452479959192749|PER|Плутарх|0.6273|839|846
```

Для каждой сущности типа LOC (настраивается) выполняет поиск в Manticore и сохраняет уникальные геохеши по doc_id.

### Режимы работы

#### Базовый запуск
```bash
./geostreamer -csv data.csv
```

#### С фильтрацией по типам сущностей
```bash
# Только LOC
./geostreamer -csv data.csv -filter-types LOC

# Несколько типов
./geostreamer -csv data.csv -filter-types "LOC,PER,ORG"
```

#### С сохранением отладочной информации
```bash
./geostreamer \
  -csv data.csv \
  -filter-types LOC \
  -output-failures failures.ndjson \
  -output-skipped skipped.ndjson
```

### Параметры запуска

#### Параметры CSV
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-csv` | путь к входному CSV файлу | `data.csv` |
| `-delim` | разделитель полей | `\|` |
| `-csv-batch` | размер буфера чтения CSV | `10000` |
| `-checkpoint` | путь к файлу чекпоинта | `geostreamer.checkpoint` |
| `-strict` | останов при ошибках | `false` |

#### Параметры Manticore
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

#### Параметры вывода
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-output` | путь к выходному NDJSON файлу | `results.ndjson` |
| `-output-failures` | путь к файлу с ошибками | `failures.ndjson` |
| `-output-skipped` | путь к файлу с пропущенными | `skipped.ndjson` |
| `-output-flush` | интервал сброса буфера | `5s` |
| `-output-buffer` | размер буфера записи | `1048576` |
| `-output-gzip` | сжатие выходного файла | `false` |

#### Параметры логирования
| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `-log-level` | уровень логирования (debug/info/warn/error) | `info` |
| `-log-file` | путь к файлу лога | (stdout) |
| `-log-stats` | интервал вывода статистики | `10s` |
| `-debug` | режим отладки (включает debug логи) | `false` |

### Форматы выходных файлов

#### results.ndjson (основной результат)
Каждая строка - JSON объект с уникальными геохешами для doc_id:
```json
{
  "doc_id": "6056452479959192749",
  "geohashes_string": ["1443f4", "38b1ac"],
  "geohashes_uint64": [1460278985035350016, -8519469090799091712]
}
```

#### failures.ndjson (ошибочные запросы)
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

#### skipped.ndjson (пропущенные записи)
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

### Особенности работы

1. **Чекпоинты** - автоматическое сохранение прогресса позволяет прерывать и возобновлять обработку с места остановки
2. **Фильтрация** - обрабатываются только указанные типы сущностей (по умолчанию LOC)
3. **Параллелизм** - настраиваемое количество воркеров и параллельных запросов
4. **Кэширование** - результаты запросов кэшируются для повторяющихся entity_text
5. **Буферизация** - все операции чтения/записи буферизированы для производительности
6. **Graceful shutdown** - корректное завершение при получении SIGINT/SIGTERM

### Примеры использования

#### Обработка большого файла с оптимальными настройками
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

#### Продолжение прерванной обработки
```bash
# Автоматически использует checkpoint файл
./geostreamer -csv /data/entities.csv -filter-types LOC
```

#### Отладка проблемных запросов
```bash
./geostreamer \
  -csv test.csv \
  -filter-types LOC \
  -manticore-debug \
  -log-level debug \
  -output-failures failures.ndjson
```

### Требования к Manticore

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

### Статистика выполнения

В процессе работы и по завершении утилита выводит подробную статистику:

#### Периодическая статистика (каждые 10 секунд)
```
2026/03/09 19:13:52 [INFO] Orchestrator === STATISTICS ===
2026/03/09 19:13:52 [INFO] Orchestrator Entity types: [LOC]
2026/03/09 19:13:52 [INFO] Orchestrator Processed (with geohashes): 121561 records
2026/03/09 19:13:52 [INFO] Orchestrator Skipped (not found in Manticore): 62130 records
2026/03/09 19:13:52 [INFO] Orchestrator Filtered (other entity types): 409804 records
2026/03/09 19:13:52 [INFO] Orchestrator Written: 46860 records
2026/03/09 19:13:52 [INFO] Orchestrator Bytes written: 46950808
2026/03/09 19:13:52 [INFO] Orchestrator Manticore: 81312 success, 0 failures
2026/03/09 19:13:52 [INFO] Orchestrator Rate: 639.79 records/sec
2026/03/09 19:13:52 [INFO] Orchestrator =================
```

#### Финальная статистика (по завершении)
```
2026/03/09 19:13:58 [INFO] Orchestrator === FINAL STATISTICS ===
2026/03/09 19:13:58 [INFO] Orchestrator Entity types processed: [LOC]
2026/03/09 19:13:58 [INFO] Orchestrator Total processing time: 3m16.405343118s
2026/03/09 19:13:58 [INFO] Orchestrator Records processed (with geohashes): 122839
2026/03/09 19:13:58 [INFO] Orchestrator Records skipped (not found in Manticore): 62444
2026/03/09 19:13:58 [INFO] Orchestrator Records filtered (other types): 412013
2026/03/09 19:13:58 [INFO] Orchestrator Records written: 47455
2026/03/09 19:13:58 [INFO] Orchestrator Bytes written: 47412718
2026/03/09 19:13:58 [INFO] Orchestrator Manticore queries: 81611 success, 0 failures
2026/03/09 19:13:58 [INFO] Orchestrator Processing rate: 625.44 records/sec
2026/03/09 19:13:58 [INFO] Orchestrator ========================
```

#### Пояснение полей статистики

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

### Анализ производительности

Тестирование проводилось на файле, полученном из обработки 100 000 параграфов текстовой базы. Входной CSV файл содержит **833 855 записей** с именованными сущностями.

#### Характеристики тестового запуска
- **Объем входных данных**: 833 855 записей
- **Типы сущностей в файле**: LOC, PER, ORG
- **Режим обработки**: только LOC (фильтрация по типу)
- **Параметры**: 30 воркеров, батчи по 1000 запросов, 30 параллельных запросов
- **Время обработки**: 3 минуты 16 секундов
- **Скорость обработки**: **625 записей/сек**

#### Результаты обработки
Из 833 855 записей:
- **LOC сущности**: ~185 000 записей (22% от общего объема)
  - Найдено в Manticore и обработано: **122 839**
  - Не найдено (skipped): **62 444**
- **PER/ORG сущности**: ~648 000 записей (78% от общего объема)
  - Отфильтровано: **412 013**

#### Эффективность
- **Успешных запросов к Manticore**: 81 611
- **Ошибок запросов**: 0
- **Уникальных doc_id с геохешами**: 47 455
- **Объем выходных данных**: ~47 MB

#### Масштабирование
При увеличении объема входных данных в 380 раз (до ~300 млн записей) ожидаемое время обработки:
- **Прогнозируемое время**: ~20 часов
- **Рекомендуемые настройки**: 50-100 воркеров, батчи по 500-1000 запросов
- **Ожидаемый объем выходных данных**: ~18 GB (в сжатом виде ~3-4 GB)

Система линейно масштабируется за счет:
- Потокового чтения без загрузки всего файла в память
- Настраиваемого параллелизма запросов к Manticore
- Эффективного кэширования повторяющихся entity_text
- Буферизированной записи результатов

## Архитектура

Проект следует принципам гексагональной архитектуры с четким разделением на domain, ports и adapters.

## Требования

- Go 1.25+
- Manticore Search (для работы с геоданными)
