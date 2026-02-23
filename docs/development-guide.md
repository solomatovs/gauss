# quotes-server -- Руководство по разработке

## Обзор

quotes-server -- workspace из нескольких проектов для работы с рыночными котировками и OHLC свечами в реальном времени:

- **quotes-server** -- микросервис: принимает котировки через плагины, рассчитывает свечи, сохраняет в storage-плагины, транслирует клиентам через REST API и WebSocket
- **quotes-gen** -- генератор котировок: отправляет синтетические или файловые котировки через sink-плагины
- **grafana-datasource** -- Grafana datasource плагин: HTTP REST клиент к quotes-server

Все проекты используют общий API крейт (`libs/api`) с типами и трейтами.

## Принципы архитектуры

### 1. Никаких внешних runtime-зависимостей

Финальный Docker-образ НЕ ДОЛЖЕН зависеть от `apk add` или любых внешних пакетов во время работы. Все необходимые библиотеки копируются в образ из стадии сборки:

- Бинарник компилируется с `RUSTFLAGS="-C target-feature=-crt-static"` (динамическая линковка обязательна для `dlopen`)
- `libgcc_s.so.1` копируется из builder-стадии через `COPY --from=builder`
- Никаких других разделяемых библиотек не требуется

**Правило:** если новая зависимость требует `.so` во время работы, копируйте её из builder-стадии. Никогда не добавляйте `apk add` в runtime-стадию.

### 2. Трёхслойная плагинная архитектура

Каждый источник/приёмник данных собирается из трёх плагинов-кубиков:

**Inbound (quotes-server):**
```
Source (I/O)     →  Framing (границы сообщений)  →  Format (декодирование)
     TCP              lines / length-prefixed          JSON / CSV / Protobuf
  accept+read         read_frame(BufRead)              parse(&[u8]) → Quote
```

**Outbound (quotes-gen):**
```
Format (кодирование)  →  Framing (границы сообщений)  →  Sink (I/O)
    JSON / CSV             lines / length-prefixed         TCP
  serialize(Quote)         write_frame(Writer, &[u8])      send(&[u8])
```

Все плагины **двунаправленные**: Format имеет `parse()` + `serialize()`, Framing имеет `read_frame()` + `write_frame()`. Это позволяет использовать одни и те же плагины и для приёма, и для отправки.

Это разделение позволяет свободно комбинировать плагины:
- TCP + lines + JSON (текстовый протокол)
- TCP + lines + CSV
- TCP + length-prefixed + Protobuf (бинарный протокол)
- TCP + length-prefixed + Avro
- UDP + length-prefixed + MessagePack

### 3. Storage backend'ы -- это плагины (.so)

Storage backend'ы компилируются как **cdylib** крейты, создающие `.so` файлы. Основной бинарник загружает их во время работы через `libloading` (`dlopen`/`dlsym`). Это позволяет:

- Добавлять новые backend'ы без модификации основного бинарника
- Настраивать backend'ы через TOML конфиг файл
- Включать/отключать backend'ы комментированием секций конфига

**Правило:** логика хранения НЕ ДОЛЖНА компилироваться в основной бинарник. Все реализации хранилищ живут в отдельных крейтах с `crate-type = ["cdylib"]`.

### 4. Общий API крейт

Крейт `libs/api/` (`quotes-server-api`) определяет все общие типы и трейты: `OhlcStorage`, `QuoteSource`, `QuoteSink`, `QuoteFraming`, `QuoteFormat`. И бинарники, и все плагины зависят от него. Это контракт между хостом и плагинами.

### 5. Конфигурация через TOML файл

Все настройки в `config.toml`. Никаких переменных окружения для бизнес-конфигурации, никаких захардкоженных значений. CLI имеет только параметр `--config` (путь к файлу). По умолчанию: `config.toml`, переопределение через env: `CONFIG_PATH`.

### 6. Composite/fan-out паттерн хранения

Можно настроить несколько backend'ов одновременно. Один -- `primary` (чтение + запись), остальные -- `replicas` (только запись). Ошибки primary -- фатальные, ошибки реплик логируются, но не прерывают работу.

### 7. Rust edition 2024

Все крейты используют `edition = "2024"` (кроме grafana-datasource: `"2021"` из-за ограничений grafana-plugin-sdk). Это влияет на синтаксис FFI:
```rust
#[unsafe(no_mangle)]  // НЕ #[no_mangle]
pub unsafe extern "C" fn qs_create_storage(...) -> StorageCreateResult { ... }
```

## Структура проекта

```
quotes-server/
├── Cargo.toml                          # Workspace root (без [package])
├── Cargo.lock
├── config.toml                         # Конфигурация quotes-server по умолчанию
│
├── libs/
│   └── api/                            # quotes-server-api (общий lib крейт)
│       ├── Cargo.toml
│       └── src/lib.rs                  # Типы, трейты, FFI типы
│
├── plugins/
│   ├── source-tcp/                     # Source: TCP сервер (inbound)
│   ├── sink-tcp/                       # Sink: TCP клиент (outbound)
│   ├── framing-lines/                  # Framing: line-based (\n)
│   ├── framing-length-prefixed/        # Framing: length-prefixed
│   ├── format-json/                    # Format: JSON ↔ Quote
│   ├── format-csv/                     # Format: CSV ↔ Quote (RFC 4180)
│   ├── storage-file/                   # Storage: файловый backend
│   └── storage-clickhouse/             # Storage: ClickHouse backend
│       └── sql/create_ohlc.sql         # DDL таблицы
│
├── bins/
│   ├── server/                         # quotes-server (основной бинарник)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs                 # Точка входа
│   │       ├── config.rs               # CLI аргументы + TOML конфиг
│   │       ├── ohlc.rs                 # In-memory OHLC движок
│   │       ├── api.rs                  # HTTP REST + WebSocket (axum)
│   │       ├── plugin.rs               # Загрузчик плагинов
│   │       ├── pipeline.rs             # Pipeline: source → framing → format → engine
│   │       ├── raw_storage.rs          # Архивирование сырых котировок
│   │       ├── storage/
│   │       │   ├── mod.rs
│   │       │   └── composite.rs        # Fan-out: primary + replicas
│   │       └── cmd/
│   │           ├── mod.rs
│   │           └── serve.rs            # Оркестрация сервера
│   │
│   ├── quotes-gen/                     # quotes-gen (генератор котировок)
│   │   ├── Cargo.toml
│   │   ├── config.toml                 # Конфигурация генератора
│   │   └── src/
│   │       ├── main.rs
│   │       ├── plugin.rs               # Загрузчик плагинов (Format, Framing, Sink)
│   │       └── cmd/
│   │           ├── mod.rs
│   │           └── generate.rs         # Генерация + отправка котировок
│   │
│   └── grafana-datasource/             # Grafana datasource плагин
│       ├── Cargo.toml
│       ├── src/
│       │   ├── main.rs
│       │   └── plugin.rs               # DataService + DiagnosticsService
│       ├── frontend/                   # TypeScript фронтенд
│       │   ├── package.json
│       │   ├── module.ts
│       │   ├── datasource.ts
│       │   ├── QueryEditor.tsx
│       │   └── ConfigEditor.tsx
│       └── build/
│           ├── Dockerfile.build        # Docker сборка (Node.js + Rust)
│           ├── docker-compose.yml
│           └── out/                    # Артефакты сборки (gitignored)
│
├── build/
│   ├── Dockerfile.build                # Docker сборка (server + gen + plugins)
│   ├── docker-compose.yml
│   └── out/                            # Артефакты сборки (gitignored)
│
├── docker-compose.yml                  # Продакшн деплой
└── docs/
    └── development-guide.md            # Этот файл
```

## Компоненты

### libs/api/ крейт (quotes-server-api)

Общая библиотека, используемая всеми бинарниками и плагинами.

**Типы:**
- `Quote` -- котировка (тик): `{symbol, bid, ask, ts_ms?}`. `ts_ms` опционально: если отсутствует, сервер подставляет текущее время. При сериализации `None` пропускается (`skip_serializing_if`).
- `Candle` -- OHLC свеча: `{symbol, tf, ts_ms, open, high, low, close, volume}`. В JSON используются короткие имена полей: `ts`, `o`, `h`, `l`, `c`, `v`.
- `CandleQuery` -- параметры запроса свечей: `{symbol, tf, from_ms?, to_ms?, limit?}`
- `QuoteQuery` -- параметры запроса котировок: `{symbol, from_ms?, to_ms?, limit?}`

**Трейты:**

```rust
/// Storage backend (async, Send + Sync)
pub trait OhlcStorage: Send + Sync {
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
    fn save_candle(&self, candle: &Candle) -> ...;
    fn save_candles(&self, candles: &[Candle]) -> ...;
    fn query_candles(&self, query: &CandleQuery) -> ...;
    fn flush(&self) -> ...;
}

/// Source -- входящий источник данных (blocking I/O, inbound)
pub trait QuoteSource: Send {
    fn start(&mut self, framing: Arc<dyn QuoteFraming>) -> Result<(), String>;
    fn recv(&mut self) -> Result<Option<Vec<u8>>, String>;
    fn stop(&mut self) -> Result<(), String>;
}

/// Sink -- исходящий приёмник данных (blocking I/O, outbound)
pub trait QuoteSink: Send {
    fn start(&mut self) -> Result<(), String>;
    fn send(&mut self, data: &[u8]) -> Result<(), String>;
    fn stop(&mut self) -> Result<(), String>;
}

/// Framing -- границы сообщений в потоке байтов (stateless, Send + Sync)
pub trait QuoteFraming: Send + Sync {
    fn read_frame(&self, reader: &mut dyn BufRead) -> Result<Option<Vec<u8>>, String>;
    fn write_frame(&self, writer: &mut dyn Write, data: &[u8]) -> Result<(), String>;
}

/// Format -- кодирование/декодирование Quote (stateless, Send + Sync)
pub trait QuoteFormat: Send + Sync {
    fn parse(&self, data: &[u8]) -> Result<Quote, String>;
    fn serialize(&self, quote: &Quote) -> Result<Vec<u8>, String>;
}
```

**Паттерн double-boxing:** `Box<dyn Trait>` -- fat pointer (2 слова), который нельзя передать через `*mut ()`. Оборачиваем: `Box::into_raw(Box::new(storage))` создаёт thin pointer. На стороне хоста: `*Box::from_raw(ptr as *mut Box<dyn Trait>)` восстанавливает trait object. Это безопасно, т.к. хост и плагины компилируются вместе в одном workspace одним компилятором.

### Загрузчик плагинов

Каждый бинарник имеет свой `plugin.rs` с обёртками для нужных типов плагинов:

**quotes-server** (`bins/server/src/plugin.rs`): `PluginStorage`, `PluginSource`, `PluginFraming`, `PluginFormat`

**quotes-gen** (`bins/quotes-gen/src/plugin.rs`): `PluginSink`, `PluginFraming`, `PluginFormat`

Каждая обёртка:
- Загружает `.so` через `libloading::Library`
- Вызывает `qs_create_{type}(config_json_ptr, len)` для создания экземпляра
- Хранит `inner: Option<Box<dyn Trait>>` и `_lib: Library`
- Реализует соответствующий трейт делегированием в `inner`

**Порядок Drop критически важен:** `inner` использует vtable из `_lib`, поэтому `inner` должен быть уничтожен первым. Кастомный `Drop` impl вызывает `self.inner.take()` до автоматического уничтожения `_lib`.

### Pipeline (bins/server/src/pipeline.rs)

Связывает три плагина в цепочку обработки:

1. `source.start(framing)` -- запуск source с framing
2. Blocking thread: `source.recv()` → `mpsc::channel` → async task
3. Async task: `format.parse(&data)` → `Quote` → `OhlcEngine` → storage + broadcast

### SinkPipeline (bins/quotes-gen)

Связывает три плагина в обратную цепочку:

1. `format.serialize(&quote)` → `Vec<u8>`
2. `framing.write_frame(&mut buf, &data)` → добавляет framing
3. `sink.send(&buf)` → отправляет

При ошибке отправки выполняется автоматический реконнект: `sink.start()` + повторная отправка.

### OHLC движок (bins/server/src/ohlc.rs)

In-memory конечный автомат. Для каждой входящей `Quote` обновляет свечи по всем настроенным таймфреймам.

- Ключ: `(symbol, tf_name)` -> текущая `Candle`
- Начало окна: `ts_ms - (ts_ms % interval_ms)`
- Окно сменилось: сброс свечи (новый open/high/low/close из bid, volume=1)
- То же окно: обновление high/low/close, инкремент volume
- Для значений свечи используется цена `bid`

### HTTP/WebSocket API (bins/server/src/api.rs)

Axum сервер на `api_port` (по умолчанию 9200).

**REST эндпоинт:**
```
GET /api/candles?symbol=EURUSD&tf=1m&from=<ms>&to=<ms>&limit=100
```

**WebSocket эндпоинт:**
```
WS /ws
```
Сообщения от клиента:
```json
{"action": "subscribe", "symbol": "EURUSD", "tf": "1m", "history": 100}
{"action": "unsubscribe", "symbol": "EURUSD", "tf": "1m"}
```

### grafana-datasource (bins/grafana-datasource/)

Grafana backend-плагин. Использует `Candle` и `Quote` из `quotes-server-api` для десериализации REST-ответов quotes-server. Поддерживает два типа запросов:

- **candles** (по умолчанию): `GET /api/candles?symbol=...&tf=...&from=...&to=...`
- **quotes**: `GET /api/quotes?symbol=...&from=...&to=...`

## Plugin FFI протокол

Каждый тип плагина экспортирует два C ABI символа:

| Тип плагина | Создание | Уничтожение |
|-------------|----------|-------------|
| Storage | `qs_create_storage` | `qs_destroy_storage` |
| Source | `qs_create_source` | `qs_destroy_source` |
| Sink | `qs_create_sink` | `qs_destroy_sink` |
| Framing | `qs_create_framing` | `qs_destroy_framing` |
| Format | `qs_create_format` | `qs_destroy_format` |

Сигнатура создания одинакова для всех:
```rust
pub unsafe extern "C" fn qs_create_{type}(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> {Type}CreateResult;
```

`{Type}CreateResult` -- структура `#[repr(C)]` с полями `{type}_ptr` и `error_ptr`.

### Создание нового плагина

1. Создать крейт в `plugins/{type}-{name}/`
2. `Cargo.toml`:
   ```toml
   [package]
   name = "quotes-{type}-{name}"
   version = "0.1.0"
   edition = "2024"

   [lib]
   crate-type = ["cdylib"]

   [dependencies]
   quotes-server-api = { path = "../../libs/api" }
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   ```
3. Реализовать соответствующий трейт. Пример для framing:
   ```rust
   use quotes_server_api::{framing_ok, FramingCreateResult, QuoteFraming};

   pub struct MyFraming { /* config fields */ }

   impl QuoteFraming for MyFraming {
       fn read_frame(&self, reader: &mut dyn std::io::BufRead) -> Result<Option<Vec<u8>>, String> {
           // Прочитать один фрейм из reader. None = EOF.
       }
       fn write_frame(&self, writer: &mut dyn std::io::Write, data: &[u8]) -> Result<(), String> {
           // Записать данные + разделитель/заголовок длины.
       }
   }

   #[unsafe(no_mangle)]
   pub unsafe extern "C" fn qs_create_framing(
       config_json_ptr: *const u8, config_json_len: usize,
   ) -> FramingCreateResult {
       // Парсинг конфига, создание экземпляра
       framing_ok(Box::new(MyFraming { /* ... */ }))
   }

   #[unsafe(no_mangle)]
   pub unsafe extern "C" fn qs_destroy_framing(framing_ptr: *mut ()) {
       if !framing_ptr.is_null() {
           let _ = unsafe { Box::from_raw(framing_ptr as *mut Box<dyn QuoteFraming>) };
       }
   }
   ```
4. Добавить крейт в `[workspace] members` в корневом `Cargo.toml`
5. Добавить COPY и cp в `build/Dockerfile.build`

### Поток конфигурации плагина

Секция `[sources.framing_config]` в `config.toml` (TOML) → сериализация в JSON строку → передача как `(ptr, len)` в `qs_create_framing` → плагин десериализует JSON в свою структуру конфига.

## Справочник конфигурации

### quotes-server (config.toml)

```toml
# Порт HTTP + WebSocket API
api_port = 9200

# Таймфреймы OHLC для расчёта
timeframes = ["1s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1y"]

# Директория для архива сырых котировок (JSONL файлы по дням)
raw_data_dir = "./data/raw"

# --- Sources (source + framing + format) ---
[[sources]]
name = "tcp-json"
source = "/app/plugins/libquotes_source_tcp.so"
framing = "/app/plugins/libquotes_framing_lines.so"
format = "/app/plugins/libquotes_format_json.so"
[sources.source_config]
port = 9100

# --- Storage backends ---
[[backends]]
plugin = "/app/plugins/libquotes_storage_file.so"
primary = true
[backends.config]
data_dir = "./data"
```

### quotes-gen (bins/quotes-gen/config.toml)

```toml
rate = 10
# seed = 42
# symbol = "EURUSD"
# history = true

[[sinks]]
name = "tcp-json"
sink = "/app/plugins/libquotes_sink_tcp.so"
framing = "/app/plugins/libquotes_framing_lines.so"
format = "/app/plugins/libquotes_format_json.so"
[sinks.sink_config]
host = "quotes-server"
port = 9100
```

**Правила quotes-server:**
- Каждый source обязан указать `source`, `framing` и `format`
- Ровно один backend должен иметь `primary = true`
- Минимум один backend и один source обязательны

**Правила quotes-gen:**
- Каждый sink обязан указать `sink`, `framing` и `format`
- Минимум один sink обязателен

## Существующие плагины

| Плагин | Тип | Конфигурация |
|--------|-----|-------------|
| `libquotes_source_tcp.so` | Source | `port` -- TCP порт |
| `libquotes_sink_tcp.so` | Sink | `host`, `port` -- адрес TCP сервера |
| `libquotes_framing_lines.so` | Framing | `max_length` -- макс. длина строки (0 = без лимита) |
| `libquotes_framing_length_prefixed.so` | Framing | `length_bytes` (1/2/4), `byte_order` (big/little), `max_payload` |
| `libquotes_format_json.so` | Format | (без конфигурации) |
| `libquotes_format_csv.so` | Format | `delimiter`, `quoting` (bool), `header` (absent/present) |
| `libquotes_storage_file.so` | Storage | `data_dir` |
| `libquotes_storage_clickhouse.so` | Storage | `host`, `port`, `user`, `password`, `database`, `table` |

## Поток данных

```
quotes-gen                              quotes-server
==========                              =============

Symbol.tick() → Quote                   Source Plugin (blocking thread)
    │                                       │ accept connections, read bytes
    v                                       v
Format Plugin                           Framing Plugin (per-connection)
    │ serialize(Quote) → Vec<u8>            │ read_frame(BufReader) → Vec<u8>
    v                                       v
Framing Plugin                          mpsc::channel → async pipeline
    │ write_frame(buf, data)                │
    v                                       v
Sink Plugin                             Format Plugin
    │ send(buf) → TCP                       │ parse(&[u8]) → Quote
    │                                       v
    │              TCP                  RawStorage → ./data/raw/{date}.jsonl
    └──────────────────────►                │
                                            v
                                        OhlcEngine (in-memory)
                                            │ process_tick → Vec<Candle>
                                            │
                                            ├── CompositeStorage
                                            │     ├── Primary (FileStorage)
                                            │     └── Replica (ClickHouse)
                                            │
                                            └── broadcast::channel<Candle>
                                                    │
                                                    ├── WS /ws
                                                    └── REST /api/candles
                                                            │
                                                    grafana-datasource
                                                    ==================
                                                    HTTP GET → Candle/Quote
                                                    → Grafana DataFrame
```

## Docker сборка

### quotes-server + quotes-gen + plugins

```bash
docker compose -f build/docker-compose.yml build
```

Результат:
- `/app/quotes-server-linux-amd64` -- бинарник сервера
- `/app/quotes-gen-linux-amd64` -- бинарник генератора
- `/app/plugins/libquotes_*.so` -- все плагины
- `/app/config.toml` -- конфигурация по умолчанию

### grafana-datasource

```bash
docker compose -f bins/grafana-datasource/build/docker-compose.yml build
```

Отдельная сборка: фронтенд (Node.js + webpack) + бэкенд (Rust) → готовый Grafana плагин.

## Тестирование

```bash
# Отправка тестовой котировки (TCP + lines + JSON)
echo '{"symbol":"EURUSD","bid":1.08512,"ask":1.08532}' | nc localhost 9100

# Запрос свечей
curl 'http://localhost:9200/api/candles?symbol=EURUSD&tf=1s'

# WebSocket (через websocat или аналог)
echo '{"action":"subscribe","symbol":"EURUSD","tf":"1s","history":10}' | websocat ws://localhost:9200/ws

# Запуск генератора (5 котировок в секунду)
./quotes-gen --config bins/quotes-gen/config.toml --rate 5
```

## Основные зависимости

| Крейт | Бинарник | Назначение |
|-------|----------|-----------|
| `tokio` | server, gen | Асинхронный runtime (многопоточный) |
| `axum` | server | HTTP + WebSocket сервер |
| `clap` | server, gen | Парсинг CLI аргументов |
| `serde` / `serde_json` | все | Сериализация |
| `toml` | server, gen | Парсинг конфиг файла |
| `libloading` | server, gen | Динамическая загрузка .so плагинов |
| `reqwest` | clickhouse, grafana | HTTP клиент (с `rustls-tls`) |
| `grafana-plugin-sdk` | grafana | Grafana backend plugin SDK |
| `chrono` | grafana | Работа с timestamps |
