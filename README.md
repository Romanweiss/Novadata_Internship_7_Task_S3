# S3 Pipeline (Selectel S3 compatible)

## Описание проекта
Проект предназначен для автоматизированной обработки входных файлов и загрузки результатов в S3-совместимое хранилище (Selectel/MinIO/Yandex S3 API).

Состав проекта:
- S3-клиент на `boto3` с базовыми методами работы с объектами
- демо-скрипты для проверки S3-операций и versioning
- Docker-пайплайн, который отслеживает директорию с новыми файлами, обрабатывает их через pandas, асинхронно загружает результат в S3 и архивирует исходники

## Назначение
Проект решает типовой сценарий data ingestion:
1. входной файл появляется в `watch/`
2. пайплайн проверяет, что файл полностью записан
3. данные читаются в `DataFrame`
4. применяется фильтрация
5. обработанный файл сохраняется во временный каталог
6. результат загружается в S3
7. исходный файл переносится в `archive/`
8. лог пайплайна обновляется локально и загружается в S3

## Как работает пайплайн

### Watcher и очередь
- `src/pipeline/watcher.py` отслеживает `/app/watch`.
- События добавляются в очередь с debounce, чтобы не обрабатывать дубли.
- В `docker-compose.yml` включен polling (`WATCH_USE_POLLING=true`), что стабильно работает в Docker на Windows.

### Проверка готовности файла
В `src/pipeline/pipeline.py` перед чтением выполняется ожидание готовности файла:
- файл существует
- размер больше 0
- размер стабилен в двух последовательных проверках

Это защищает от ошибки чтения при копировании больших/медленно записываемых файлов.

### Обработка данных
`src/pipeline/processor.py` поддерживает форматы:
- `.csv`
- `.csv.gz`
- `.csv.zip`
- `.tsv`
- `.jsonl`
- `.json`
- `.parquet`

Логика фильтрации:
- если есть колонка `value`, выполняется нормализация (`"10,5" -> "10.5"`), затем `to_numeric(errors="coerce")`, затем фильтр `value > 0`
- если `value` нет, используется fallback: `head(100)`

При временно пустом CSV используется retry чтения (`EmptyDataError`), чтобы не падать на раннем событии watcher.

### Выгрузка и архив
- Асинхронная загрузка выполняется через `aioboto3` (`src/pipeline/uploader_async.py`).
- Обработанный файл загружается в S3 под префикс `S3_PREFIX`.
- Исходный файл переносится в `archive/` с timestamp в имени.
- Если файл не удалось прочитать после retry, он переносится в `archive/failed`.

### Логирование
- Лог пишется в `LOG_FILE` (по умолчанию `./logs/pipeline.log`).
- После обработки каждого файла текущий лог загружается в S3 по ключу `LOG_OBJECT_KEY`.
- При включенном versioning у бакета история логов сохраняется в версиях объекта.

## Требования
- Docker Desktop (или Docker Engine)
- Docker Compose v2
- доступ к бакету и S3-ключам

Локальный Python не требуется.

## Конфигурация (`.env`)
Создать файл из шаблона:

```powershell
copy .env.example .env
```

Минимальные переменные:

```env
S3_ENDPOINT=https://s3.ru-3.storage.selcloud.ru
S3_ACCESS_KEY=<your_access_key>
S3_SECRET_KEY=<your_secret_key>
S3_BUCKET=<your_bucket_name>
S3_REGION=ru-3
S3_PREFIX=processed/
S3_VERIFY_SSL=true
S3_CA_BUNDLE=
WATCH_DIR=./watch
ARCHIVE_DIR=./archive
LOG_FILE=./logs/pipeline.log
LOG_OBJECT_KEY=logs/pipeline.log
WATCH_USE_POLLING=true
```

Проверка конфигурации:

```powershell
docker compose config
```

## Запуск

### 1. Сборка и запуск пайплайна

```powershell
docker compose up --build
```

### 2. Просмотр логов

```powershell
docker compose logs -f
```

### 3. Остановка

```powershell
docker compose down
```

## Рабочие директории (volumes)
В контейнере используются пути:
- `/app/watch`
- `/app/archive`
- `/app/logs`
- `/app/downloads`
- `/app/tmp`

Они связаны с локальными папками:
- `./watch`
- `./archive`
- `./logs`
- `./downloads`
- `./tmp`

## Полезные команды

Запуск demo Task 1:

```powershell
docker compose run --rm app python scripts/demo_task1.py
```

Запуск demo Task 2 (versioning):

```powershell
docker compose run --rm app python scripts/demo_task2_verify.py
```

Генерация тестовых файлов для пайплайна:

```powershell
docker compose run --rm app python scripts/demo_task3.py
```

## Проверка корректности пайплайна

1. Запустить пайплайн:

```powershell
docker compose up --build
```

2. В другом терминале сгенерировать тестовый CSV:

```powershell
docker compose run --rm app python scripts/demo_task3.py
```

3. Убедиться по логам (`docker compose logs -f`), что для нового CSV есть последовательность этапов:
- `Waiting for file to be ready`
- `File ready`
- `Detected input format`
- `Processed file saved`
- `Uploaded ... to s3://<bucket>/<key>`
- `Source file moved to archive`
- `Uploaded log snapshot`

4. Проверить артефакты корректной обработки:
- исходный файл перемещен в `archive/`
- обработанный временный файл не остается в `tmp/` (после upload удаляется)
- в бакете появляется новый объект в `S3_PREFIX` (обычно `processed/`)
- объект `LOG_OBJECT_KEY` (обычно `logs/pipeline.log`) обновляется после каждого файла

5. Проверка устойчивости:
- неподдерживаемые расширения (например `.txt`) игнорируются с INFO-логом
- недописанные/пустые CSV не обрабатываются преждевременно (ждем стабилизации файла + retry чтения)
- при неуспешном чтении после retry исходник переносится в `archive/failed`

## Структура проекта

```text
.
  Dockerfile
  docker-compose.yml
  .env.example
  README.md
  src/
    config.py
    logging_setup.py
    s3_client.py
    pipeline/
      watcher.py
      processor.py
      uploader_async.py
      pipeline.py
  scripts/
    demo_task1.py
    demo_task2_verify.py
    demo_task3.py
    run_pipeline.py
  watch/
  archive/
  logs/
  downloads/
  tmp/
```

## Troubleshooting

### `SSL: CERTIFICATE_VERIFY_FAILED`
Причина: TLS-сертификат endpoint не доверен внутри контейнера.

Варианты:
- временно отключить проверку: `S3_VERIFY_SSL=false`
- задать CA bundle: `S3_CA_BUNDLE=/app/certs/ca.pem`

### `Could not connect to endpoint https://s3.example.com/...`
Причина: в `.env` остались шаблонные значения.

Проверь через:

```powershell
docker compose config
```

и убедись, что `S3_ENDPOINT`, `S3_BUCKET`, ключи подставлены реальными значениями.
