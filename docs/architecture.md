# Gauss — Архитектура

## Концепция

Gauss — система потоковой обработки данных, построенная вокруг одной абстракции: **Topic**.

Topic = имя + storage. Данные — это байты. Ни движок, ни topic не знают их формат.
Движок знает только `ts_ms` (время добавления записи). Format знают те, кому
он нужен: processor (для framing и десериализации) и storage (если ему нужна
десериализация — ClickHouse, table mode). Каждый — из своего конфига.

Десериализация происходит **только** когда пользователь явно этого потребовал
(например, для колоночного хранения в ClickHouse или для table-режима с upsert по ключу).

## Topic — центральная сущность

Topic — **именованный storage-бакет**. Он не знает формат данных.
Хранит опак байты (`TopicRecord { ts_ms, data }`), не парсит, не валидирует.

Format — ответственность тех, кто **производит** и **потребляет** данные:
- Source processor знает `config.input` (format + framing входящего потока)
- Transform processor знает `config.input` / `config.output` (format для десериализации/сериализации)
- Sink processor знает `config.output` (format + framing исходящего потока)
- Storage знает `storage_config.format` (если ему нужна десериализация — ClickHouse, table mode)
- Memory/File storage **не знает** format — ему не нужен

```
┌───────────────────────────────────────────┐
│                  Topic                    │
│                                           │
│  Метаданные:                              │
│    name:       "quotes.raw"               │
│                                           │
│  Storage (плагин .so):                    │
│    storage:        путь к .so             │
│    storage_config: { ... }               │
│      всё — на усмотрение плагина:        │
│      storage_size, write_full, mode,     │
│      format, key_field, host, ttl, ...   │
│                                           │
│  Запись:                                  │
│    TopicRecord { ts_ms, data }            │
│                                           │
│  Read modes — определяет storage:          │
│    Каждый storage декларирует какие        │
│    read modes он поддерживает.             │
│    Движок валидирует при старте.           │
│                                           │
│  memory (ring buffer):                    │
│    offset, latest, query                  │
│  memory (table/upsert):                   │
│    snapshot, subscribe, query             │
│  file (append):                           │
│    offset, latest, query                  │
│  clickhouse:                              │
│    query, snapshot                        │
└───────────────────────────────────────────┘
```

### TopicRecord

```rust
pub struct TopicRecord {
    pub ts_ms: i64,       // единственное что движок обязан знать
    pub data: Vec<u8>,    // опак байты — topic не знает их формат
}
```

- `ts_ms` — индекс для temporal query, сортировки, retention
- `data` — опак байты, ни движок, ни topic не интерпретируют их содержимое

Движок **не знает** ключ записи и не знает структуру данных.
Если storage нуждается в десериализации (upsert по ключу, колоночное хранение),
он получает `format`, `schema` и нужные параметры (например `key_field`)
через свой `storage_config` и `StorageContext` при инициализации.

### Framing

Framing — разбиение потока байтов на отдельные фреймы.
Один фрейм = один TopicRecord = одна единица данных.

Framing — **внутренняя механика processor-а**, а не отдельный слой в pipeline.
Processor, принимающий байтовый поток (source), сам применяет framing
для нарезки потока на записи.

**Framing всегда указывается явно.** Никаких неявных значений по умолчанию.
Source и sink processor-ы обязаны указать framing в своём `config.input` / `config.output`.
Transform processor-ы не нуждаются в framing — они работают с дискретными TopicRecord.

Типичные комбинации format + framing (справочная таблица, не правило):

| Format | Обычно используемый framing |
|--------|---------------------------|
| JSON | `framing = "newline", delimiter = "\n"` |
| CSV | `framing = "newline", delimiter = "\n"` |
| Protobuf | `framing = "length_prefixed", prefix_type = "u32be"` |
| Avro | `framing = "avro_container"` |
| Arrow (streaming) | `framing = "arrow_ipc_streaming"` |
| Arrow (file) | используется storage напрямую (file storage пишет .arrow файлы) |

### input / output — объекты формата в config processor-а

Processor описывает формат своих входных и выходных данных через объекты
`input` и `output` в `config`. Все свойства — явные, ничего не подразумевается.

**Для source processor-а** (byte stream → TopicRecord):

```toml
config = {
    port = 9100,
    input = {
        format  = "json",       # имя format-плагина из [[formats]]
        framing = "newline",    # стратегия разбиения потока на фреймы
        delimiter = "\n",       # параметр framing (для newline)
    }
}
```

**Для sink processor-а** (TopicRecord → byte stream):

```toml
config = {
    port = 9300,
    output = {
        format  = "json",       # имя format-плагина из [[formats]]
        framing = "newline",    # стратегия сборки фреймов в поток
        delimiter = "\n",       # параметр framing (для newline)
    }
}
```

**Для transform processor-а** (TopicRecord → process → TopicRecord):

```toml
config = {
    input  = { format = "json" },        # только format, framing не нужен
    output = { format = "avro-ohlc" },   # только format, framing не нужен
    intervals = ["1m"]
}
```

**Без format** (raw bytes, только framing):

```toml
config = {
    port = 9100,
    input = {
        framing    = "fixed_size",   # нет format — только framing
        frame_size = 100,            # параметр framing
    }
}
```

Полный перечень свойств `input` / `output`:

| Свойство | Тип | Когда нужно | Описание |
|----------|-----|-------------|----------|
| `format` | string | десериализация/сериализация | имя format-плагина из `[[formats]]` |
| `framing` | string | source/sink (byte stream) | стратегия framing |
| `delimiter` | string | `framing = "newline"` | байты-разделитель (`"\n"`, `"\r\n"`) |
| `prefix_type` | string | `framing = "length_prefixed"` | тип длины: `"u32be"`, `"u16be"`, `"varint"` |
| `frame_size` | int | `framing = "fixed_size"` | размер фрейма в байтах |

Типы framing:

| Framing | Описание | Параметры |
|---------|----------|-----------|
| `newline` | разделитель между фреймами | `delimiter` |
| `length_prefixed` | длина фрейма перед данными | `prefix_type` |
| `fixed_size` | фиксированный размер каждого фрейма | `frame_size` |
| `avro_container` | Avro container sync markers | — |
| `arrow_ipc_streaming` | Arrow IPC streaming messages (continuation + size) | — |

### Storage — плагин

Storage — это плагин (.so). Движку всё равно что внутри передаваемых данных. Представь что он выполняет роль посредника между двумя звеньями между котороми передаются байты, он как бы связывает их.
Движек передаем `storage_config` в Storage плагин. Storage плагин знает сам, нужно ли выполнять сериализацию/десериализацию данных, так как это его часть работы и он выполняет валидацию передаваемого `storage_config`. Движек передает `storage_config` в процессе инициализации. А в процессе работы передает StorageContext который позволяет получить runtime данные

**Примеры поведения storage-плагинов:**

| Storage | Что делает с данными | Нуждается в format/schema |
|---------|---------------------|--------------------------|
| memory (ring buffer) | хранит TopicRecord as-is | нет |
| memory (table/upsert) | десериализует → извлекает key → upsert | да |
| file (append) | пишет raw bytes в файл | нет |
| clickhouse (INSERT) | десериализует → раскладывает по колонкам | да |
| clickhouse (blob) | пишет data в `payload` колонку | нет |
| postgres (INSERT) | десериализует → INSERT по колонкам | да |

`append` vs `table`, `INSERT` vs `upsert` — это **не свойство Topic**,
а режим работы конкретного storage, задаваемый через `storage_config`:

```toml
# Memory: ring buffer (append), 4096 записей, drop при переполнении
storage_config = { storage_size = 4096, write_full = "drop" }

# Memory: upsert по ключу — format нужен для десериализации и извлечения key
storage_config = { mode = "table", format = "json", key_field = "symbol" }

# File: append в директорию, block при переполнении — format не нужен
storage_config = { data_dir = "./data/quotes", write_full = "block" }

# ClickHouse: INSERT — format + schema_map для schema mapping
storage_config = {
    format = "proto-quote",
    schema_map = "proto-quote-to-clickhouse",
    schema = { table = "quotes" },
    host = "localhost",
}

# ClickHouse: ReplacingMergeTree (upsert) — тот же schema_map
storage_config = {
    format = "proto-quote",
    schema_map = "proto-quote-to-clickhouse",
    schema = { table = "quotes", engine = "ReplacingMergeTree" },
    host = "localhost",
}
```

`storage_size`, `write_full`, `mode`, `key_field`, `host`, `ttl` — всё это
параметры конкретного storage-плагина. Движок их не интерпретирует,
а передаёт плагину as-is. Если storage хочет делать upsert — он **сам**
потребует `key_field` и `format`. Движок в это не вмешивается.

### Storage с десериализацией

Когда storage нуждается в десериализации (upsert, реляционка, колоночное хранение),
он обязан понимать структуру данных. Маппинг описывается через `schema_map`
(см. Schema Mapping). Цепочка:

```
TopicRecord { ts_ms, data: bytes }
        │
        ▼
   storage: serializer + schema (из StorageContext)
        │
   1. deserialize(data) → структурированные данные (все поля формата)
   2. extract нужные поля по schema.fields[].source
   3. map на колонки по schema.fields[].name с типом schema.fields[].field_type
   4. save (INSERT по колонкам / upsert по ключу)
```

`TargetSchema` — результат Schema Mapping (движок строит при старте).
Пользователь контролирует через `schema_map` — Rhai скрипт `map_schema(source, target)`,
который трансформирует `SourceSchema` в `TargetSchema`.

Пользователь описывает это в `storage_config`:

```toml
[[topics]]
name = "quotes.live"
storage = "memory"
storage_config = { mode = "table", format = "proto-quote", key_field = "symbol" }
```

`format` и `key_field` — параметры storage (в `storage_config`).
Сервер видит `format = "proto-quote"` → резолвит в плагин → передаёт
serializer в storage через `StorageContext`. Topic сам format не знает.

## Поток данных

### Базовый поток

```
Transport → Processor (source) → Topic storage
                                       │
                                       ├── Processor (transform) → Topic
                                       ├── Processor (sink) → Transport
                                       └── Consumer (query)
```

Три сущности: Transport (байтовый I/O), Topic (пассивное хранение), Processor (вся активная работа).
Processor внутри себя делает framing, трансформацию, запись — всё что нужно.

### Read modes — storage-специфичные

Read modes определяются **storage-ом**, а не движком. Каждый storage при
инициализации декларирует какие read modes он поддерживает. Движок
валидирует совместимость при старте: если processor просит `read = "offset"`
у topic-а с table storage — ошибка конфигурации, не runtime паника.

Не каждый storage может реализовать каждый read mode. Причины:

- **Table mode (upsert)** — нет упорядоченного лога записей. Когда `BTC`
  обновился, старое значение исчезло. `offset` семантически бессмыслен.
- **ClickHouse** — может симулировать offset (SELECT ... OFFSET N), но это
  антипаттерн. CH оптимизирован для batch-аналитики, не cursor-based streaming.

Read modes по типам storage:

```
memory (ring buffer):   offset, latest, query
memory (table/upsert):  snapshot, subscribe, query
file (append):          offset, latest, query
clickhouse:             query, snapshot
postgres:               query, snapshot
```

Описание read modes:

| Read mode | Семантика | Кто поддерживает |
|-----------|-----------|-----------------|
| `offset` | последовательно по курсору (Kafka-семантика) | ring buffer, file |
| `latest` | только последнее значение (пропущенные не нужны) | ring buffer, file |
| `query` | фильтр по ts_ms диапазону | все |
| `snapshot` | вся таблица / все данные целиком | table, clickhouse, postgres |
| `subscribe` | snapshot при каждом изменении | table |

`read_mode` — параметр подписки processor-а (через `source`), а не свойство topic-а.

```
Topic storage (ring buffer): [f0][f1][f2][f3][f4][f5]

Consumer A (offset):                Consumer B (latest):
читает последовательно              читает только последнее
    ▼                                    ▼
   [f2] → [f3] → [f4] →...             [f5]
   "дай всё по порядку"                "дай самое свежее"

Topic storage (table/upsert):
┌───────────────────────┐
│  "BTC" → f5 (новый)  │
│  "ETH" → f3          │
│  "SOL" → f1          │
└───────────────────────┘

Consumer C (snapshot):              Consumer D (subscribe):
вся таблица по запросу               snapshot при каждом изменении
    ▼                                    ▼
   { BTC:f5, ETH:f3, SOL:f1 }        { BTC:f5, ETH:f3, SOL:f1 }
   "дай всё что есть"                "уведомляй об изменениях"
```

Классические паттерны (pipe, buffer, latest) — **частные случаи**
комбинации storage_config + read_mode:

```
pipe    = storage_size: 1  + offset reader  + write_full: block
buffer  = storage_size: N  + offset reader  + write_full: drop | block | overwrite
latest  = storage_size: 1  + latest reader  + write_full: overwrite
```

`storage_size` и `write_full` — параметры storage (через `storage_config`).

```
┌──────────────────────────────────────────────┐
│           Topic                              │
│                                              │
│  storage_config:                             │
│    storage_size:  1 / N / ∞  (сколько)      │
│    write_full:    block / drop (переполн.)   │
│                                              │
│  Storage декларирует:                        │
│    supported_read_modes: [offset, latest,    │
│                           query]             │
│                                              │
│  Consumer X:                                 │
│    read_mode: offset  ✓ (storage поддерж.)  │
│                                              │
│  Consumer Y:                                 │
│    read_mode: latest  ✓ (storage поддерж.)  │
│                                              │
└──────────────────────────────────────────────┘
```

Пользователь комбинирует параметры под свою задачу:

```toml
# Ring buffer topic
[[topics]]
name = "quotes"
storage = "memory"
storage_config = { storage_size = 4096, write_full = "drop" }

# Два processor-а читают из одного topic-а с разными read mode
[[processors]]
name = "realtime-view"
plugin = "./plugins/processor/ws-sink.so"
source = { topic = "quotes", read = "latest" }
config = { port = 8080 }

[[processors]]
name = "archiver"
plugin = "./plugins/processor/file-writer.so"
source = { topic = "quotes", read = "offset" }
target = { topic = "quotes.archive" }
```

Движок при старте проверяет что storage `memory` (ring buffer)
поддерживает и `latest`, и `offset` — валидация проходит.
Он не знает, pipe там или buffer.

### Десериализация — только по требованию

Десериализация не глобальное решение движка, а требование конкретного режима:

| Сценарий | Десериализация | Почему |
|----------|---------------|--------|
| Passthrough | Нет | байты пролетают как есть |
| File storage (raw) | Нет | пишет bytes в файл |
| Memory storage | Нет | хранит TopicRecord as-is |
| ClickHouse (columnar) | Да | раскладывает по колонкам, извлекает key |
| Table mode (upsert) | Да | извлекает key для upsert |
| Processor (OHLC) | Да | нуждается в структуре данных |
| API (HTTP/WS display) | Да | показывает клиенту как JSON |

Когда storage или processor нуждается в десериализации, он использует
`FormatSerializer` — загруженный по имени format из своего конфига.

Topic **не хранит** format. Format указывается там, где он нужен, например:

| Кому нужен format | Где указывается | Зачем |
|---|---|---|
| Source processor | `config.input` (format + framing) | framing входящего потока + опциональная десериализация |
| Sink processor | `config.output` (format + framing) | framing исходящего потока + опциональная сериализация |
| Transform processor | `config.input` / `config.output` (format) | десериализация / сериализация (без framing) |
| Storage (ClickHouse) | `storage_config.format` + `schema_map` | Schema Mapping → раскладка по колонкам |
| Storage (table mode) | `storage_config.format` | извлечение key для upsert |
| Storage (memory, file) | — | не нужен |

## Storage

Storage — плагин. Движок не перечисляет и не знает конкретные реализации.
Для движка storage — это `TopicStorage` trait:

```rust
pub trait TopicStorage {
    fn init(&mut self, ctx: StorageContext) -> Result<()>;
    fn save(&self, record: TopicRecord) -> Result<()>;
    fn read(&self, mode: &ReadMode, params: &ReadParams) -> Result<ReadResult>;

    /// Какие read modes поддерживает этот storage.
    /// Движок вызывает при старте для валидации конфигурации.
    fn supported_read_modes(&self) -> &[ReadMode];
}
```

Движок при старте проверяет: для каждого processor-а, который ссылается
на topic через `source = { topic = "...", read = "..." }`, read mode
должен быть в списке `supported_read_modes()` storage-а этого topic-а.
Несовместимость — ошибка конфигурации при старте.

### StorageContext

При инициализации storage получает контекст:

```rust
pub struct StorageContext {
    pub serializer: Option<Arc<dyn FormatSerializer>>,
    pub schema: Option<TargetSchema>,
}
```

**Без десериализации** (format не указан в storage_config):
- `serializer = None`, `schema = None`
- Пишет `record.data` как есть (файл, ring buffer, blob-колонка)

**С десериализацией** (format + schema_map указаны в storage_config):
- `serializer` — резолвится движком из `storage_config.format`
- `schema` — результат Schema Mapping: `TargetSchema` с заполненными `fields` и `attrs`
- `schema.attrs` содержит DDL-свойства из `storage_config.schema` (table, engine, order_by...)
- Падает при init() если serializer или schema отсутствуют

Какой режим использовать — определяет `storage_config` пользователя, не движок.
Storage получает полный `TargetSchema` — единую структуру для генерации DDL.

## Schema Mapping

Schema Mapping — механизм трансформации schema из format-а в native типы storage.
`FieldType` — единая валюта типов во всей системе: source и target.

Вся логика трансформации — в одном Rhai скрипте: `map_schema(source, target)`.
Скрипт получает обе схемы и модифицирует `TargetSchema` — заполняет `fields`,
может корректировать `attrs`. TargetSchema зарождается из `storage_config.schema`
(attrs заполнены, fields пустой) ещё до вызова скрипта.

```
storage_config.schema    FormatPlugin       Rhai скрипт              StoragePlugin
(attrs: DDL props)       .schema()          map_schema(src, tgt)     .init(ctx)
       │                     │                    │                       │
       ▼                     ▼                    ▼                       ▼
  TargetSchema ─┐
  { fields: [], ├──→ [ map_schema(source, target) ] ──→ TargetSchema
    attrs: {…} }│                                       { fields: [...],
  SourceSchema ─┘                                         attrs: {…} }
                                                              │
                                                         render DDL
                                                              │
                                                         CREATE TABLE
```

Движок при старте:
1. Загружает format plugin → `schema()` → `SourceSchema`
2. Строит начальный `TargetSchema { fields: [], attrs }` из `storage_config.schema`
3. Загружает `[[schema_maps]]` → Rhai скрипт
4. Вызывает `map_schema(source, target)` → `TargetSchema` (fields заполнены)
5. `StorageContext { serializer, schema }` → `storage.init(ctx)`

### FieldType — структурированный тип

Единое представление типа данных. Используется везде: source schema, target schema,
вход и выход Rhai скрипта. Имя типа и набор атрибутов — произвольные.

```rust
pub struct FieldType {
    pub name: String,                       // "decimal", "Decimal64", "NUMERIC", ...
    pub attrs: HashMap<String, Value>,      // { "precision": 18, "scale": 8 }
}
```

Примеры source FieldType (от format plugin):

| Format поле | FieldType |
|------------|-----------|
| `double` | `{ name: "double", attrs: {} }` |
| `decimal(18,8)` | `{ name: "decimal", attrs: { precision: 18, scale: 8 } }` |
| `varchar(255)` | `{ name: "varchar", attrs: { length: 255 } }` |
| `varchar(max)` | `{ name: "varchar", attrs: { length: "max" } }` |
| `varchar` | `{ name: "varchar", attrs: {} }` |
| `timestamp(3)` | `{ name: "timestamp", attrs: { precision: 3 } }` |
| `array(int64)` | `{ name: "array", attrs: { element: "int64" } }` |

Примеры target FieldType (после Rhai, для storage):

| ClickHouse | Postgres |
|------------|----------|
| `{ name: "Decimal64", attrs: { scale: 8 } }` | `{ name: "NUMERIC", attrs: { precision: 18, scale: 8 } }` |
| `{ name: "String", attrs: {} }` | `{ name: "TEXT", attrs: {} }` |
| `{ name: "DateTime64", attrs: { precision: 3 } }` | `{ name: "TIMESTAMP", attrs: { precision: 3, timezone: true } }` |
| `{ name: "Array", attrs: { element: { name: "Int64" } } }` | `{ name: "ARRAY", attrs: { element: { name: "BIGINT" } } }` |

### SourceSchema — от format plugin

```rust
pub struct SourceSchema {
    pub fields: Vec<SourceField>,
    pub attrs: HashMap<String, Value>,  // source-specific метаданные
}

pub struct SourceField {
    pub name: String,           // "symbol" для плоских, "$.order.id" для иерархических
    pub field_type: FieldType,
}
```

`attrs` — метаданные source, format plugin заполняет из своей конфигурации:

| Source | attrs |
|--------|-------|
| Postgres | `{ database: "mydb", schema: "public", table: "quotes" }` |
| ClickHouse | `{ database: "default", table: "quotes", engine: "ReplacingMergeTree", order_by: ["ts_ms", "symbol"] }` |
| Protobuf | `{ package: "market.data", message: "Quote" }` |
| JSON | `{}` |

Rhai скрипт получает `attrs` через контекст и может использовать для принятия решений.

- Для плоских форматов (protobuf, Arrow) — `name` = имя поля (`"symbol"`, `"bid"`)
- Для иерархических (JSON, MessagePack) — `name` = путь внутри документа (`"$.order.id"`)
- Format plugin знает свой формат и интерпретирует `name` как нужно

### FormatSerializer — runtime сериализация

FormatSerializer — runtime объект, выполняет `bytes ↔ Row`:

```rust
pub trait FormatSerializer {
    fn deserialize<'a>(&self, bytes: &'a [u8]) -> Row<'a>;
    fn serialize(&self, row: &Row) -> Vec<u8>;
}
```

- `deserialize()` — парсит raw bytes в `Row`. Для строк/bytes использует `Cow::Borrowed`
  (zero-copy ссылки в исходный буфер). Знает schema формата (получил при создании).
- `serialize()` — собирает `Row` обратно в bytes формата.

Кто использует:
- **Storage** (Data Pipeline): `deserialize()` — читает TopicRecord, раскладывает по колонкам
- **Transform processor**: `deserialize()` на входе, `serialize()` на выходе (два разных FormatSerializer — input/output)
- **Source/Sink processor**: `serialize()` / `deserialize()` для framing потока

### FormatPlugin — фабрика

FormatPlugin — конфигурация формата. Создаёт serializer и предоставляет schema:

```rust
pub trait FormatPlugin {
    fn serializer(&self) -> Arc<dyn FormatSerializer>;
    fn schema(&self) -> Option<SourceSchema>;
}
```

- `serializer()` — создаёт FormatSerializer с внутренним знанием schema и конфига формата
- `schema()` — возвращает SourceSchema для Schema Mapping Pipeline

`schema()` возвращает `None` если формат не имеет фиксированной схемы и не сконфигурирован с `fields`.
Если `schema_map` указан, но `schema()` возвращает `None` — ошибка при старте.

### Иерархические форматы и JSONPath

Плоские форматы (protobuf, Arrow) — поля уже на верхнем уровне, `name` = имя поля.
Иерархические форматы (JSON, MessagePack) — документ содержит вложенные объекты
и массивы. `name` в `SourceField` — путь внутри документа (JSONPath).
Format plugin при runtime использует `name` как путь для извлечения значений.

Входной JSON:
```json
{
  "order": {
    "id": 123,
    "customer": {
      "name": "Alice",
      "tags": ["vip", "retail"]
    },
    "items": [
      { "sku": "A1", "qty": 2, "price": 9.99 }
    ]
  },
  "ts": 1700000000
}
```

Format-конфиг:
```toml
[[formats]]
name = "json-orders"
plugin = "./plugins/format/json.so"
config = {
    fields = [
        { name = "$.order.id",             type = { name = "int64" } },
        { name = "$.order.customer.name",  type = { name = "string" } },
        { name = "$.order.customer.tags",  type = { name = "array", attrs = { element = "string" } } },
        { name = "$.order.items[*].sku",   type = { name = "array", attrs = { element = "string" } } },
        { name = "$.order.items[*].price", type = { name = "array", attrs = { element = "double" } } },
        { name = "$.ts",                   type = { name = "int64" } },
    ]
}
```

Результат `schema()` — `SourceSchema` с path-именами:

| SourceField.name | SourceField.field_type |
|-----------------|----------------------|
| `$.order.id` | `{ name: "int64", attrs: {} }` |
| `$.order.customer.name` | `{ name: "string", attrs: {} }` |
| `$.order.customer.tags` | `{ name: "array", attrs: { element: "string" } }` |
| `$.order.items[*].sku` | `{ name: "array", attrs: { element: "string" } }` |
| `$.order.items[*].price` | `{ name: "array", attrs: { element: "double" } }` |
| `$.ts` | `{ name: "int64", attrs: {} }` |

В Rhai скрипте `source` ссылается на `name` напрямую — по полному пути:
```rhai
target.fields.push(#{ name: "order_id", field_type: #{ name: "BIGINT", attrs: #{} },
                       source: "$.order.id" });
```

Переименование из path в короткое имя колонки — внутри Rhai скрипта (`source` → `name`),
а не format config. Один уровень ответственности, нет скрытых алиасов.

### TargetSchema — для storage plugin

```rust
pub struct TargetSchema {
    pub fields: Vec<TargetField>,
    pub attrs: HashMap<String, Value>,  // DDL-level свойства (table, engine, order_by...)
}

pub struct TargetField {
    pub name: String,
    pub field_type: FieldType,
    pub source: Option<String>,
    pub props: HashMap<String, Value>,  // field-level свойства (default, materialized...)
}
```

Симметрия с SourceSchema:

| | SourceSchema | TargetSchema |
|--|-------------|-------------|
| `fields` | `Vec<SourceField>` — поля формата | `Vec<TargetField>` — колонки storage |
| `attrs` | source метаданные (package, message...) | DDL метаданные (table, engine, order_by...) |

- `fields` — generic, движок понимает
- `attrs` — движок заполняет из `storage_config.schema`, Rhai скрипт может модифицировать
- `props` на каждом field — storage интерпретирует сам

Примеры `attrs` (schema-level):

| Storage | attr | DDL |
|---------|------|-----|
| ClickHouse | `table = "quotes"` | `CREATE TABLE quotes` |
| ClickHouse | `engine = "MergeTree"` | `ENGINE = MergeTree()` |
| ClickHouse | `order_by = "(ts_ms)"` | `ORDER BY (ts_ms)` |
| ClickHouse | `ttl = "..."` | `TTL ...` |
| Postgres | `table = "quotes"` | `CREATE TABLE quotes` |

Примеры `props` (field-level):

| Storage | prop | DDL |
|---------|------|-----|
| ClickHouse | `default = "now64(3)"` | `DEFAULT now64(3)` |
| ClickHouse | `materialized = "ask - bid"` | `MATERIALIZED ask - bid` |
| ClickHouse | `codec = "Delta, ZSTD"` | `CODEC(Delta, ZSTD)` |
| Postgres | `default = "now()"` | `DEFAULT now()` |
| Postgres | `generated = "ask - bid"` | `GENERATED ALWAYS AS (ask - bid) STORED` |
| Postgres | `not_null = true` | `NOT NULL` |

Storage получает `TargetSchema` через `StorageContext.schema` — единая структура,
достаточная для генерации полного DDL (CREATE TABLE с engine, columns, constraints).

### render_type — storage рендерит FieldType в DDL

Каждый storage plugin содержит логику рендеринга `FieldType → String` для DDL.
Движок не знает native синтаксис — это ответственность storage.

ClickHouse render_type:

| FieldType | DDL |
|-----------|-----|
| `{ name: "Decimal64", attrs: { scale: 8 } }` | `Decimal64(8)` |
| `{ name: "String", attrs: {} }` | `String` |
| `{ name: "DateTime64", attrs: { precision: 3 } }` | `DateTime64(3)` |
| `{ name: "LowCardinality", attrs: { inner: { name: "String" } } }` | `LowCardinality(String)` |
| `{ name: "Array", attrs: { element: { name: "Int64" } } }` | `Array(Int64)` |

Postgres render_type:

| FieldType | DDL |
|-----------|-----|
| `{ name: "NUMERIC", attrs: { precision: 18, scale: 8 } }` | `NUMERIC(18,8)` |
| `{ name: "NUMERIC", attrs: {} }` | `NUMERIC` |
| `{ name: "VARCHAR", attrs: { length: 255 } }` | `VARCHAR(255)` |
| `{ name: "TIMESTAMP", attrs: { precision: 3, timezone: true } }` | `TIMESTAMP(3) WITH TIME ZONE` |
| `{ name: "ARRAY", attrs: { element: { name: "BIGINT" } } }` | `BIGINT[]` |

### Rhai скрипт — map_schema(source, target)

Трансформация schema выполняется **скриптом** (Rhai).
Rhai — встраиваемый скриптовый язык для Rust. Чистый Rust, без FFI,
компилируется в бинарник как обычный крейт. Sandbox по умолчанию —
нет доступа к файлам, сети, системе.

Один скрипт — единый механизм для всей трансформации: фильтрация, переименование,
маппинг типов, extra колонки. Никаких ограничений на сложность логики.

```
fn map_schema(source, target) → TargetSchema
  source.fields — массив SourceField: #{ name, field_type: #{ name, attrs } }
  source.attrs  — метаданные source: #{ package, message, ... }
  target.fields — пустой массив (скрипт заполняет)
  target.attrs  — DDL-свойства из storage_config.schema: #{ table, engine, order_by, ... }
  return        — модифицированный target
```

Каждый элемент `target.fields` — TargetField:
- `name` — имя колонки
- `field_type` — `#{ name, attrs }` — FieldType для target
- `source` — (опционально) имя поля в данных, из которого извлекается значение
- `props` — (опционально) `#{ default, materialized, codec, ... }` — свойства storage

```rhai
// scripts/schema-map/proto-to-clickhouse.rhai
//
// Вспомогательная функция: маппинг одного типа
fn map_field_type(ft) {
    if ft.name == "decimal" {
        let p = ft.attrs.precision ?? 38;
        let s = ft.attrs.scale ?? 0;
        let width = if p <= 9 { "Decimal32" }
                    else if p <= 18 { "Decimal64" }
                    else if p <= 38 { "Decimal128" }
                    else { "Decimal256" };
        return #{ name: width, attrs: #{ scale: s } };
    }
    if ft.name == "varchar" || ft.name == "string" {
        return #{ name: "String", attrs: #{} };
    }
    if ft.name == "timestamp" {
        let p = ft.attrs.precision ?? 3;
        return #{ name: "DateTime64", attrs: #{ precision: p } };
    }
    if ft.name == "array" {
        let inner = map_field_type(#{ name: ft.attrs.element, attrs: #{} });
        if inner != () { return #{ name: "Array", attrs: #{ element: inner } }; }
        return ();
    }
    switch ft.name {
        "double" => #{ name: "Float64",  attrs: #{} },
        "float"  => #{ name: "Float32",  attrs: #{} },
        "int64"  => #{ name: "Int64",    attrs: #{} },
        "int32"  => #{ name: "Int32",    attrs: #{} },
        "uint64" => #{ name: "UInt64",   attrs: #{} },
        "bool"   => #{ name: "UInt8",    attrs: #{} },
        "bytes"  => #{ name: "String",   attrs: #{} },
        _        => ()
    }
}

// Главная функция: модифицирует target schema
fn map_schema(source, target) {
    // target.attrs уже заполнен из storage_config.schema:
    //   { table: "quotes", engine: "MergeTree", order_by: "(ts_ms)", ... }
    // target.fields пустой — заполняем

    // ts_ms — поле TopicRecord, добавляется первым
    target.fields.push(#{ name: "ts_ms", field_type: #{ name: "Int64", attrs: #{} },
                          source: "ts_ms" });

    for field in source.fields {
        // фильтрация: пропускаем ненужные поля
        if field.name == "internal_id" || field.name == "debug_info" { continue; }
        if field.name == "exchange" { continue; }

        // переименование + переопределение типа
        if field.name == "symbol" {
            target.fields.push(#{
                name: "sym",
                field_type: #{ name: "LowCardinality", attrs: #{ inner: #{ name: "String" } } },
                source: field.name,
            });
            continue;
        }

        // автоматический маппинг типа
        let mapped = map_field_type(field.field_type);
        if mapped == () { throw `unmapped type: ${field.field_type.name}`; }
        target.fields.push(#{ name: field.name, field_type: mapped, source: field.name });
    }

    // extra колонки: default, materialized
    target.fields.push(#{ name: "wrt_ts", field_type: #{ name: "DateTime64", attrs: #{ precision: 3 } },
                          props: #{ default: "now64(3)" } });
    target.fields.push(#{ name: "spread", field_type: #{ name: "Float64", attrs: #{} },
                          props: #{ materialized: "ask - bid" } });

    target
}
```

Postgres — другая логика, тот же интерфейс:

```rhai
// scripts/schema-map/proto-to-postgres.rhai

fn map_field_type(ft) {
    if ft.name == "decimal" {
        let a = #{};
        let p = ft.attrs.precision ?? ();
        let s = ft.attrs.scale ?? ();
        if p != () { a.precision = p; }
        if s != () { a.scale = s; }
        return #{ name: "NUMERIC", attrs: a };
    }
    if ft.name == "varchar" {
        let n = ft.attrs.length ?? ();
        if n != () && n != "max" { return #{ name: "VARCHAR", attrs: #{ length: n } }; }
        return #{ name: "TEXT", attrs: #{} };
    }
    if ft.name == "string" { return #{ name: "TEXT", attrs: #{} }; }
    if ft.name == "timestamp" {
        let a = #{ timezone: true };
        let p = ft.attrs.precision ?? ();
        if p != () { a.precision = p; }
        return #{ name: "TIMESTAMP", attrs: a };
    }
    if ft.name == "array" {
        let inner = map_field_type(#{ name: ft.attrs.element, attrs: #{} });
        if inner != () { return #{ name: "ARRAY", attrs: #{ element: inner } }; }
        return ();
    }
    switch ft.name {
        "double" => #{ name: "DOUBLE PRECISION", attrs: #{} },
        "float"  => #{ name: "REAL",             attrs: #{} },
        "int64"  => #{ name: "BIGINT",           attrs: #{} },
        "int32"  => #{ name: "INTEGER",          attrs: #{} },
        "uint64" => #{ name: "BIGINT",           attrs: #{} },
        "bool"   => #{ name: "BOOLEAN",          attrs: #{} },
        "bytes"  => #{ name: "BYTEA",            attrs: #{} },
        _        => ()
    }
}

fn map_schema(source, target) {
    target.fields.push(#{ name: "ts_ms", field_type: #{ name: "BIGINT", attrs: #{} },
                          source: "ts_ms" });

    for field in source.fields {
        let mapped = map_field_type(field.field_type);
        if mapped == () { throw `unmapped type: ${field.field_type.name}`; }
        target.fields.push(#{ name: field.name, field_type: mapped, source: field.name });
    }

    target.fields.push(#{ name: "wrt_ts", field_type: #{ name: "TIMESTAMPTZ", attrs: #{} },
                          props: #{ default: "now()" } });

    target
}
```

### schema_maps — описание маппинга

`[[schema_maps]]` — полный рецепт трансформации schema из формата в storage.
Единственное поле — `script`: путь к Rhai скрипту с функцией `map_schema(source, target)`.
Вся логика (фильтрация, переименование, маппинг типов, extra колонки) — внутри скрипта.
Определяется на верхнем уровне конфига, переиспользуется между topic-ами.

```toml
[[schema_maps]]
name = "proto-quote-to-clickhouse"
script = "scripts/schema-map/proto-to-clickhouse.rhai"
```

Storage ссылается через `schema_map`. DDL-свойства выделены в `schema`:

```toml
storage_config = {
    # движок резолвит → FormatSerializer, Rhai скрипт
    format = "proto-quote",
    schema_map = "proto-quote-to-clickhouse",

    # DDL → TargetSchema.attrs (до вызова Rhai скрипта)
    schema = { table = "quotes", engine = "MergeTree", order_by = "(ts_ms)" },

    # storage plugin — connection + operational
    host = "clickhouse",
}
```

### Цепочка Schema Mapping

```
1. format-плагин → SourceSchema (каждое поле — FieldType):
     symbol:      { name: "string",  attrs: {} }
     bid:         { name: "decimal", attrs: { precision: 18, scale: 8 } }
     ask:         { name: "decimal", attrs: { precision: 18, scale: 8 } }
     volume:      { name: "int64",   attrs: {} }
     exchange:    { name: "string",  attrs: {} }
     internal_id: { name: "string",  attrs: {} }
     debug_info:  { name: "bytes",   attrs: {} }

2. начальный TargetSchema из storage_config.schema:
     fields: []
     attrs: { table: "quotes", engine: "MergeTree", order_by: "(ts_ms)" }

3. map_schema(source, target) — Rhai скрипт модифицирует target:
     — пропускает internal_id, debug_info, exchange (фильтрация)
     — ts_ms: добавляет как { name: "Int64" }, source: "ts_ms"
     — symbol → sym: переименование + override тип LowCardinality(String)
     — bid:    map_field_type(decimal(18,8)) → { name: "Decimal64", attrs: { scale: 8 } }
     — ask:    map_field_type(decimal(18,8)) → { name: "Decimal64", attrs: { scale: 8 } }
     — volume: map_field_type(int64)         → { name: "Int64", attrs: {} }
     — wrt_ts: extra, props: { default: "now64(3)" }
     — spread: extra, props: { materialized: "ask - bid" }
     → TargetSchema (fields заполнены, attrs сохранены)

4. storage render DDL из TargetSchema (fields + attrs) → CREATE TABLE:
     ts_ms    Int64,
     sym      LowCardinality(String),
     bid      Decimal64(8),
     ask      Decimal64(8),
     volume   Int64,
     wrt_ts   DateTime64(3) DEFAULT now64(3),
     spread   Float64 MATERIALIZED ask - bid
```

Если `map_field_type()` возвращает `()` и скрипт не обрабатывает это — `throw` при старте.

### Пример: JSON с вложенной структурой → Postgres

```toml
[[formats]]
name = "json-orders"
plugin = "./plugins/format/json.so"
config = {
    fields = [
        { name = "$.order.id",             type = { name = "int64" } },
        { name = "$.order.customer.name",  type = { name = "string" } },
        { name = "$.order.customer.tags",  type = { name = "array", attrs = { element = "string" } } },
        { name = "$.order.items[*].price", type = { name = "array", attrs = { element = "double" } } },
        { name = "$.ts",                   type = { name = "int64" } },
    ]
}

[[schema_maps]]
name = "json-orders-to-postgres"
script = "scripts/schema-map/json-orders-to-postgres.rhai"

[[topics]]
name = "orders.pg"
storage = "postgres"
storage_config = {
    format = "json-orders",
    schema_map = "json-orders-to-postgres",
    schema = { table = "orders" },
    host = "postgres",
}
```

Rhai скрипт для этого маппинга:

```rhai
// scripts/schema-map/json-orders-to-postgres.rhai

fn map_field_type(ft) {
    // ... стандартный маппинг типов для Postgres (как выше)
}

fn map_schema(source, target) {
    for field in source.fields {
        // пропускаем items[*].price — не нужен в target
        if field.name == "$.order.items[*].price" { continue; }

        // переименование JSONPath → короткие имена колонок
        if field.name == "$.order.id" {
            target.fields.push(#{ name: "order_id", field_type: #{ name: "BIGINT", attrs: #{} },
                                  source: field.name });
            continue;
        }
        if field.name == "$.order.customer.name" {
            target.fields.push(#{ name: "customer_name", field_type: #{ name: "TEXT", attrs: #{} },
                                  source: field.name });
            continue;
        }
        if field.name == "$.order.customer.tags" {
            target.fields.push(#{ name: "customer_tags",
                                  field_type: #{ name: "ARRAY", attrs: #{ element: #{ name: "TEXT", attrs: #{} } } },
                                  source: field.name });
            continue;
        }
        if field.name == "$.ts" {
            target.fields.push(#{ name: "ts", field_type: #{ name: "BIGINT", attrs: #{} },
                                  source: field.name });
            continue;
        }
    }

    target.fields.push(#{ name: "created_at", field_type: #{ name: "TIMESTAMPTZ", attrs: #{} },
                          props: #{ default: "now()" } });

    target
}
```

Цепочка:
```
1. JSON format plugin → schema() → SourceSchema (name = JSONPath):
     $.order.id:             { name: "int64",  attrs: {} }
     $.order.customer.name:  { name: "string", attrs: {} }
     $.order.customer.tags:  { name: "array",  attrs: { element: "string" } }
     $.order.items[*].price: { name: "array",  attrs: { element: "double" } }
     $.ts:                   { name: "int64",  attrs: {} }

2. начальный TargetSchema из storage_config.schema:
     fields: [], attrs: { table: "orders" }

3. map_schema(source, target) — Rhai скрипт модифицирует target:
     — пропускает $.order.items[*].price
     — $.order.id            → order_id,       BIGINT
     — $.order.customer.name → customer_name,  TEXT
     — $.order.customer.tags → customer_tags,   TEXT[]
     — $.ts                  → ts,             BIGINT
     — created_at: extra, props: { default: "now()" }
     → TargetSchema

4. storage render DDL → CREATE TABLE:
     order_id       BIGINT,
     customer_name  TEXT,
     customer_tags  TEXT[],
     ts             BIGINT,
     created_at     TIMESTAMPTZ DEFAULT now()
```

Schema map ссылается на source-поля по их полному JSONPath в поле `source`.
Переименование path → короткое имя колонки — в `name` возвращаемой структуры.

### Несколько schema_maps для одного формата

Один формат, разные storage — разные Rhai скрипты:

```toml
[[schema_maps]]
name = "proto-quote-to-clickhouse"
script = "scripts/schema-map/proto-to-clickhouse.rhai"

[[schema_maps]]
name = "proto-quote-to-postgres"
script = "scripts/schema-map/proto-to-postgres.rhai"

# Один формат → два storage, разные скрипты маппинга
[[topics]]
name = "quotes.ch"
storage = "clickhouse"
storage_config = { format = "proto-quote", schema_map = "proto-quote-to-clickhouse", ... }

[[topics]]
name = "quotes.pg"
storage = "postgres"
storage_config = { format = "proto-quote", schema_map = "proto-quote-to-postgres", ... }
```

### Свойства TargetField (результат map_schema)

Каждый элемент `target.fields`, заполняемого скриптом `map_schema(source, target)` — TargetField:

| Свойство | Обязательное | Описание |
|----------|-------------|----------|
| `name` | да | имя колонки в storage |
| `field_type` | да | `FieldType`-объект `#{ name, attrs }` — target тип |
| `source` | нет | имя поля в данных. Если указан — значение извлекается из десериализованных данных |
| `props` | нет | `#{ default, materialized, codec, ... }` — storage-specific свойства |

Варианты колонки:

| Вариант | source | props | Поведение |
|---------|--------|-------|-----------|
| Из данных | `source: "bid"` | — | значение извлекается из десериализованных данных |
| С default | — | `props: #{ default: "now64(3)" }` | storage создаёт DEFAULT, значение не извлекается |
| Вычисляемая | — | `props: #{ materialized: "ask - bid" }` | storage создаёт MATERIALIZED (CH), GENERATED (PG) |
| Из данных + fallback | `source: "region"` | `props: #{ default: "'unknown'" }` | из данных, если нет — DEFAULT |

`ts_ms` из TopicRecord — не поле формата. Если нужен как колонка, скрипт добавляет
его явно с `source: "ts_ms"`. Движок не добавляет его автоматически.

## Data Pipeline

Schema Mapping определяет **структуру** (DDL, типы). Data Pipeline определяет
как **данные** трансформируются из source формата в target формат при каждой записи.

Проблема: N source форматов × M target storage = N×M конвертеров.
Решение: каноническое промежуточное представление `Row` — каждый format реализует
`bytes → Row`, каждый storage реализует `Row → native write`. Итого N + M.

```
              StoragePlugin (весь flow внутри storage)
                        │
  TopicRecord ──→ serializer.deserialize() ──→ Row
                                                │
                            engine::apply_mapping(row, schema) ──→ Row
                                                                    │
                                                        storage.write_native()
                                                                    │
                                                              native INSERT
```

Storage получает `FormatSerializer` и `TargetSchema` через `StorageContext`.
Весь data pipeline происходит внутри storage:
1. `serializer.deserialize(record.data)` → `Row`
2. `engine::apply_mapping(row, schema)` → mapped `Row` (библиотечная функция движка)
3. `write_native(row)` → native INSERT

`apply_mapping` — utility-функция движка, не plugin. Механическая операция по TargetSchema:
убирает лишние поля, переименовывает, переупорядочивает. Storage вызывает её сам.

### Value — каноническое представление значения

```rust
pub enum Value<'a> {
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Decimal(i128, u8),          // value, scale — eager, layout несовместим между форматами
    Timestamp(i64, u8),         // micros, precision — eager

    String(Cow<'a, [u8]>),     // raw bytes, не UTF-8 — source может быть любой кодировки
    Bytes(Cow<'a, [u8]>),      // opaque binary data (UUID, IP, JSONB...)

    Array(Vec<Value<'a>>),      // рекурсивный — поэлементный парсинг
    Map(Vec<(Value<'a>, Value<'a>)>),
    Tuple(Vec<Value<'a>>),

    Null,
}
```

Стратегия по типам:

| Тип | Стратегия | Причина |
|-----|-----------|---------|
| Int64, Float64, Bool | eager parse | стоимость ~0 (чтение 1-8 байт) |
| Decimal, Timestamp | eager parse | binary layout несовместим между форматами |
| String, Bytes | `Cow` (zero-copy) | основная стоимость — аллокация + копирование, Cow избегает |
| Array, Map, Tuple | рекурсивный eager | layout несовместим, элементы парсятся по одному |

`Cow::Borrowed` — ссылка в исходный буфер TopicRecord, без копирования.
`Cow::Owned` — копия, когда исходный буфер недоступен или значение трансформировано.

`String` и `Bytes` оба хранят `Cow<[u8]>` (raw bytes, не `str`).
Разница семантическая: `String` — текстовые данные, `Bytes` — opaque binary (UUID, JSONB, IP...).
Кодировка source данных может быть любой — storage интерпретирует при записи.

### Row — каноническое представление записи

```rust
pub struct RowField<'a> {
    pub name: String,
    pub value: Value<'a>,
    pub source_type: FieldType,   // исходный тип — всегда сохранён
}

pub struct Row<'a> {
    pub fields: Vec<RowField<'a>>,
}
```

`source_type` сохраняет полную информацию об исходном типе. Это позволяет:
- Storage видит **три вещи** при записи: `value` (данные), `source_type` (откуда), `target_type` (куда, из TargetSchema)
- Специализированный конвертер может оптимизировать hot path для конкретной пары source→target
- Generic fallback всегда работает через `Value`

`FormatSerializer.deserialize()` создаёт `Row` из байт,
storage plugin потребляет `Row` для записи.
Они не знают друг о друге — `Row` мост между ними.

### Data Pipeline — шаги

```
1. storage читает TopicRecord(bytes) из Topic

2. serializer.deserialize(bytes) → Row (каждое поле = value + source_type):
     Row [
         { name: "symbol", value: String(Cow::Borrowed(...)),
           source_type: { name: "string" } },
         { name: "bid",    value: Decimal(12345678901, 8),
           source_type: { name: "decimal", attrs: { precision: 18, scale: 8 } } },
         { name: "ask",    value: Decimal(98765432100, 8),
           source_type: { name: "decimal", attrs: { precision: 18, scale: 8 } } },
         { name: "volume", value: Int64(1000),
           source_type: { name: "int64" } },
     ]

3. storage вызывает engine::apply_mapping(row, TargetSchema):
   - filter: убирает поля не в TargetSchema
   - rename: source → column (symbol → sym)
   - reorder: порядок как в TargetSchema
   - source_type сохраняется при переименовании
     Row [
         { name: "sym",    value: String(Cow::Borrowed(...)),
           source_type: { name: "string" } },
         { name: "bid",    value: Decimal(12345678901, 8),
           source_type: { name: "decimal", attrs: { precision: 18, scale: 8 } } },
         ...
     ]

4. storage пишет Row → native (видит value + source_type + target_type):
   - sym: source string → target LowCardinality(String) → generic: Cow.as_bytes()
   - bid: source decimal(18,8) → target Decimal64(8) → specialized: прямая конверсия
   - колонки с props (default, materialized) — не в Row, storage обрабатывает сам
```

### Пример: Postgres → ClickHouse через Row

```
Postgres source processor читает строку через pg driver.
Байты строки добавляются в TopicRecord.

TopicRecord(pg binary bytes)
       │
       ▼
serializer.deserialize(bytes):  // FormatSerializer для "pg-binary"
  — парсит pg wire format (big-endian, length-prefixed)
  — строки/bytes: Cow::Borrowed (ссылки в исходный буфер)
  — числа: eager parse (endian swap)
  — source_type берётся из SourceSchema (serializer знает схему с момента создания)
       │
       ▼
  Row [
    { name: "id",     value: Int64(123),
      source_type: { name: "int8" } },
    { name: "bid",    value: Decimal(12345678901, 8),
      source_type: { name: "numeric", attrs: { precision: 18, scale: 8 } } },
    { name: "symbol", value: String(Cow::Borrowed(&bytes[..])),
      source_type: { name: "text" } },
    { name: "tags",   value: Array([
        String(Cow::Borrowed(&bytes[..])),        // zero-copy per element
        String(Cow::Borrowed(&bytes[..])),
      ]),
      source_type: { name: "array", attrs: { element: "text" } } },
  ]
       │
       ▼
  engine::apply_mapping(row, TargetSchema) — filter/rename/reorder
  source_type сохраняется
       │
       ▼
  Row [
    { name: "sym",  value: String(Cow::Borrowed(...)),
      source_type: { name: "text" } },
    { name: "bid",  value: Decimal(12345678901, 8),
      source_type: { name: "numeric", attrs: { precision: 18, scale: 8 } } },
    { name: "tags", value: Array([...]),
      source_type: { name: "array", attrs: { element: "text" } } },
  ]
       │
       ▼
ClickHouse storage.write(row, TargetSchema):
  для каждого поля видит value + source_type + target_type (из TargetSchema):

  sym:  source text → target LowCardinality(String)
        → generic: Cow.as_bytes() → CH string encoding

  bid:  source numeric(18,8) → target Decimal64(8)
        → specialized: pg numeric → CH Decimal64 напрямую

  tags: source array(text) → target Array(String)
        → generic: поэлементно Cow.as_bytes() → CH offsets + packed strings

  wrt_ts: нет в Row → DEFAULT now64(3), CH обрабатывает при INSERT
       │
       ▼
  CH native INSERT
```

### Специализированные конвертеры

Storage plugin при записи каждого поля имеет полный контекст:
`value` (данные) + `source_type` (откуда) + `target_type` (куда, из TargetSchema).

Specialized converters живут **внутри storage plugin**. Storage использует `source_type`
как hint для оптимизации — это не зависимость от source format plugin.
Storage не импортирует PG plugin, а просто проверяет строковое имя типа:

```rust
fn convert(field: &RowField, target: &TargetField) -> Vec<u8> {
    // Specialized: storage знает свой target тип + использует source_type как hint
    if field.source_type.name == "numeric" && target.field_type.name == "Decimal64" {
        return pg_numeric_to_ch_decimal64(field, target);
    }
    if field.source_type.name == "uuid" && target.field_type.name == "UUID" {
        // UUID: 16 bytes, одинаковый layout → zero-copy
        return field.value.as_bytes().to_vec();
    }

    // Generic fallback: работает для любых типов через Value
    generic_convert(&field.value, &target.field_type)
}
```

| Стратегия | Когда | Пример |
|-----------|-------|--------|
| zero-copy | binary layout совпадает | UUID → UUID (16 bytes as-is) |
| specialized | layout разный, но пара известна | pg numeric → CH Decimal64 |
| generic | всё остальное | Value::Int64 → CH Int64 через generic |

Принцип: **generic всегда работает, specialized оптимизирует hot path**.
Source_type в Row — metadata, не зависимость. Storage может его игнорировать
(generic fallback) или использовать для оптимизации конкретных пар.

### Lifetime: Row и TopicRecord

`Row<'a>` содержит `Cow::Borrowed` ссылки на bytes из TopicRecord.
TopicRecord должен жить пока Row используется.

Для одиночной записи — lifetime естественно ограничен вызовом:

```rust
fn process_record(&self, record: &TopicRecord) {
    let row = self.serializer.deserialize(&record.data); // Row borrows record.data
    let mapped = self.apply_mapping(row);                 // всё ещё borrows
    self.write_native(mapped);                            // пишет в storage
    // row dropped, record.data свободен
}
```

Для batch — storage буферизирует TopicRecord-ы, затем обрабатывает пачкой:

```rust
// Фаза накопления
self.buffer.push(record);  // TopicRecord сохраняется

// Фаза flush (по размеру буфера или таймеру)
for record in &self.buffer {
    let row = self.serializer.deserialize(&record.data);
    let mapped = self.apply_mapping(row);
    // add() сразу извлекает данные из Row в native буфер (CH column buffers и т.д.)
    // Row живёт только внутри одной итерации — Cow::Borrowed валиден,
    // т.к. record в self.buffer жив
    self.batch_writer.add(&mapped);
}
self.batch_writer.flush();  // batch INSERT — одна сетевая операция
self.buffer.clear();        // TopicRecord-ы освобождаются
```

Важно: `batch_writer.add()` не сохраняет Row — он сразу конвертирует данные
в native формат storage (например, CH column buffers). Row и Cow::Borrowed
живут только внутри одной итерации цикла. TopicRecord-ы в буфере гарантируют
что Cow::Borrowed ссылки валидны на момент чтения.

### Batching

Storage может работать в двух режимах:

| Режим | Когда | Как |
|-------|-------|-----|
| row-at-a-time | low-latency, upsert | `process_record()` на каждый TopicRecord |
| batch | high-throughput (CH, PG) | буферизация → batch `deserialize` + `INSERT` |

Выбор режима — ответственность storage plugin, не движка.
Движок передаёт TopicRecord-ы по одному, storage решает буферизировать или писать сразу.

Для ClickHouse batch эффективен: одна сетевая операция на N строк.
Для table mode (upsert) — row-at-a-time, т.к. нужна немедленная видимость.

### Ключевые принципы Data Pipeline

- **N + M вместо N × M**: каждый format реализует `bytes → Row`, каждый storage реализует `Row → write`
- **source_type сохранён**: исходный тип не теряется, специализация source→target всегда возможна
- **Cow для строк**: zero-copy когда возможно, copy когда нужно
- **Eager для чисел**: стоимость парсинга ~0, binary layout между форматами несовместим
- **Рекурсия для составных**: Array/Map/Tuple парсятся поэлементно, строковые элементы — Cow
- **Lifetime**: Row<'a> живёт в scope обработки, TopicRecord буферизируется для batch
- **Batching**: storage plugin решает, row-at-a-time или batch
- **Schema Mapping = метаданные** (DDL, типы), **Data Pipeline = данные** (runtime, каждая запись)

## Table mode (пример storage-уровня)

Table mode — это **режим конкретного storage-плагина**, не свойство Topic.
Storage в table mode поддерживает upsert по ключу и snapshot-запросы.

```
TopicRecord { ts_ms, data: quote_bytes }
        │
        ▼
   storage (memory, mode = "table"):
        │
   1. deserialize(data) используя serializer из StorageContext
   2. extract key = value["symbol"] (key_field из storage_config)
   3. upsert в таблицу по ключу
        │
        ▼
┌─────────────────────────┐
│   Table storage         │
│                         │
│   "BTC"  → f5 (новый)  │  ← заменён
│   "ETH"  → f3          │
│   "SOL"  → f1          │
└─────────────────────────┘
        │
        ├── snapshot   → { BTC: f5, ETH: f3, SOL: f1 }
        │
        └── subscribe  → при каждом изменении:
                         { BTC: f5, ETH: f3, SOL: f1 }  (полный снепшот)
```

Key извлекается **внутри storage** — движок не знает о нём.
Storage берёт `key_field` и `format` из своего `storage_config`, а `serializer`
получает из `StorageContext` при init() (сервер резолвит `format` → плагин).

Table mode поддерживает read-режимы `snapshot`, `subscribe` и `query`.
Режимы `offset` и `latest` **не поддерживаются** — в upsert-таблице нет
упорядоченного лога записей.

```
memory (table/upsert) supported read modes:
  snapshot  → вся таблица целиком
  subscribe → snapshot при каждом изменении
  query     → фильтр по ts_ms
```

```toml
[[formats]]
name = "proto-quote"
plugin = "./plugins/format/protobuf.so"
config = { descriptor = "schemas/quote.bin", message = "Quote" }

[[topics]]
name = "quotes.table"
storage = "memory"
storage_config = { mode = "table", format = "proto-quote", key_field = "symbol" }
```

Format указан в `storage_config`, потому что **storage** нуждается в десериализации
для upsert. Topic сам format не знает.

Аналогично ClickHouse может делать и append (MergeTree), и upsert
(ReplacingMergeTree) — это его `storage_config`, не атрибут Topic.

## Processor — вся активная работа

Processor — единственная **активная** сущность в системе. Он делает всё:
framing, трансформацию, десериализацию, агрегацию, сжатие/распаковку.

Topic — пассивный (хранит, ждёт). Processor — активный (читает, обрабатывает, пишет).

### Три варианта processor-а

```
Source processor:     Transport → [framing → process] → Topic
Transform processor:  Topic → [process] → Topic
Sink processor:       Topic → [process → framing] → Transport
```

**Source** — читает байтовый поток из transport, применяет framing (нарезает
на фреймы), создаёт TopicRecord { ts_ms, data }, пишет в topic.

**Transform** — читает TopicRecord из topic-а, обрабатывает (десериализация,
конвертация, агрегация, фильтрация, декомпрессия...), пишет результат в topic.

**Sink** — читает TopicRecord из topic-а, применяет framing (собирает в поток),
отправляет через transport.

Все три — processor-ы. Различие: откуда читают и куда пишут.

### Почему одна абстракция

Processor внутри себя делает несколько этапов:

```
byte stream (от transport или topic)
    │
    ▼
framing (если нужно — нарезка потока на фреймы)
    │
    ▼
TopicRecord { ts_ms, data }
    │
    ▼
process(data) — в зависимости от задачи:
    ├── passthrough (просто сохранить как есть)
    ├── decompress (распаковать и сохранить)
    ├── deserialize (format+schema) → extract key → upsert
    ├── deserialize → aggregate → serialize
    ├── deserialize → filter → serialize
    └── deserialize(format_A) → serialize(format_B) — конвертация
```

Middleware и framing как отдельные слои **не нужны**:
- Декомпрессия потока — source processor (gzip stream → framing → topic)
- Декомпрессия сообщений — transform processor (topic → decompress → topic)
- Конвертация формата — transform processor (topic → JSON→Avro → topic)
- Всё это частные случаи одной абстракции.

Topic-и выступают как очереди между processor-ами. Это аналогично:
- Informatica PowerCenter: buffer blocks между Reader/Transformation/Writer threads
- Apache NiFi: connections (очереди с backpressure) между processors
- Kafka Streams: internal topics между процессорами в topology

### Active vs Passive (классификация трансформаций)

Каждый processor либо **passive** (сохраняет кардинальность), либо **active** (меняет):

```
Passive (1:1) — одна запись на входе, одна на выходе:
  ├── format-convert    JSON → Avro, Protobuf → JSON
  ├── expression        вычисление/добавление полей
  ├── enrich            lookup из другого topic-а
  └── mask              маскировка данных

Active (N:M) — кардинальность меняется:
  ├── filter            1:0-or-1  отбросить по условию
  ├── router            1:N       разделить на несколько output topic-ов
  ├── aggregator        N:1       OHLC, SUM, COUNT по окну/группе
  ├── join              N+M:1     соединить два потока по ключу
  ├── window            N:1       группировка по времени
  ├── flatten           1:N       развернуть массив в записи
  └── union             N:1       объединить несколько потоков
```

Это деление важно для валидации: если два active processor-а пишут в один
topic, поведение определено (append). Но если нужно соединить два потока —
используй join, а не просто запись в один topic.

### Stateless vs Stateful

Ортогональное деление — нужна ли процессору память о предыдущих записях:

```
Stateless — обрабатывает каждую запись независимо:
  ├── filter, router     решение по текущей записи
  ├── format-convert     перекодировка текущей записи
  ├── expression         вычисление по текущей записи
  └── mask               маскировка текущей записи

Stateful — накапливает состояние:
  ├── aggregator         буферизует записи до закрытия окна
  ├── join               буферизует обе стороны
  ├── window             буферизует записи в окне
  ├── dedup              помнит виденные ключи
  └── enrich (cached)    кеширует lookup-таблицу
```

Stateful processor нуждается в хранении состояния. Варианты:
- Внутренний state (HashMap, ring buffer) — processor сам управляет
- Внешний storage (topic в table mode) — processor пишет state в отдельный topic

### Фазы processor-а

Processor может иметь несколько фаз работы. Типичный паттерн:

```
Phase 1 (init/detect):        Phase 2 (steady-state):
  буферизация                    основная работа
  детекция параметров            framing / обработка / passthrough
  загрузка state
  handshake
       │                              │
       ▼                              ▼
  "я ещё не готов"               "работаю штатно"
```

Примеры фаз:

| Processor | Phase 1 | Phase 2 |
|-----------|---------|---------|
| Source (encoding detect) | буферизация, детекция кодировки → байты разделителя | delimiter framing (passthrough) |
| Source (protocol) | WebSocket upgrade / TLS handshake | framing потока |
| Join (stream-table) | загрузка snapshot из table topic | lookup + emit |
| Aggregator | загрузка state из state topic | агрегация + emit |
| Source (auto-format) | анализ первых байтов → определение формата | framing по определённому формату |

Фазы — **внутренняя механика** processor-а. Движок не знает о них.
Processor сам решает, когда перейти из одной фазы в другую.
Для движка processor — чёрный ящик: принимает данные, публикует результат.

Во время Phase 1 processor **не публикует** данные в target topic (или публикует
частично). Он копит, анализирует, готовится. Переход в Phase 2 может быть:
- По таймауту (не удалось определить — использовать default)
- По достижению порога (достаточно данных для детекции)
- По событию (snapshot загружен, handshake завершён)

### Конфигурация processor-а

`input` / `output` — объекты в `config` processor-а. Все свойства формата
и framing указываются явно. Processor сам знает, с каким форматом работает.

```toml
# Source processor: transport → framing → topic
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# Sink processor: topic → framing → transport
[[processors]]
name = "realtime-out"
plugin = "./plugins/processor/tcp-sink.so"
source = { topic = "quotes.raw", read = "offset" }
config = {
    port = 9300,
    output = { format = "json", framing = "newline", delimiter = "\n" }
}

# Transform: OHLC агрегатор (active, stateful)
[[processors]]
name = "ohlc-builder"
plugin = "./plugins/processor/ohlc.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "ohlc.1m" }
config = {
    input  = { format = "json" },
    output = { format = "avro-ohlc" },
    intervals = ["1m", "5m", "1h"]
}

# Transform: формат-конвертер (passive, stateless)
[[processors]]
name = "json-to-avro"
plugin = "./plugins/processor/format-convert.so"
source = { topic = "quotes.json", read = "offset" }
target = { topic = "quotes.avro" }
config = {
    input  = { format = "json" },
    output = { format = "avro-quote" }
}

# Transform: фильтр по символам (active, stateless)
[[processors]]
name = "btc-only"
plugin = "./plugins/processor/symbol-filter.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "quotes.btc" }
config = {
    input = { format = "json" },
    symbols = ["BTCUSD", "BTCEUR"]
}

# Transform: декомпрессия сообщений (passive, stateless)
[[processors]]
name = "decompress"
plugin = "./plugins/processor/decompress.so"
source = { topic = "quotes.compressed", read = "offset" }
target = { topic = "quotes.raw" }
config = { algo = "lz4" }
# decompress не знает format — работает с raw bytes, input/output не нужны
```

Transform processor знает формат из **своего конфига** (`config.input` / `config.output`).
Десериализация и сериализация — ответственность processor-а:

```
source topic                     target topic
TopicRecord { data: bytes }      TopicRecord { data: bytes }
        │                                ▲
        ▼                                │
 FormatSerializer(input.format)  FormatSerializer(output.format)
   .deserialize()                  .serialize()
        │                                ▲
        ▼                                │
   structured data  ──► processor ──► structured data
```

`input` и `output` — из конфига processor-а, не из topic-а.

### Цепочка processor-ов

Processor-ы соединяются через topic-и, образуя DAG:

```
Transport ──► [Source] ──► Topic A ──► [Transform 1] ──► Topic B ──► [Sink] ──► Transport
                                          │
                                          └──► [Transform 2] ──► Topic C
```

Каждый topic в цепочке — это буфер с backpressure (через storage_config:
write_full = block/drop). Это даёт:
- Изоляцию: медленный processor не блокирует быстрый
- Персистентность: если processor упал, данные в topic-е ждут
- Fan-out: один topic может питать несколько processor-ов
- Наблюдаемость: можно подключиться к любому промежуточному topic-у

## Плагинная модель

### Типы плагинов

```
plugins/
├── transport/          ── Сетевой ввод/вывод ──
│   ├── tcp-server/      bind + accept
│   └── tcp-client/      connect
│
├── format/             ── FormatSerializer (десериализация + framing) ──
│   ├── json/            bytes ↔ JSON       (framing: \n)
│   ├── csv/             bytes ↔ CSV        (framing: \n)
│   ├── protobuf/        bytes ↔ Protobuf   (framing: length-prefixed)
│   ├── avro/            bytes ↔ Avro       (framing: container sync)
│   └── arrow/           bytes ↔ Arrow IPC  (streaming / file)
│
├── storage/            ── Storage engines ──
│   ├── memory/          ring buffer / table
│   ├── file/            raw files / partitioned
│   └── clickhouse-rmt/  ReplacingMergeTree, columnar
│
└── processor/          ── Вся активная работа ──
    ├── tcp-source/      transport → framing → topic (source)
    ├── tcp-sink/        topic → framing → transport (sink)
    ├── ohlc/            Quote → OHLC Candle (transform, active, stateful)
    ├── symbol-filter/   фильтр по символам (transform, active, stateless)
    ├── format-convert/  конвертация формата (transform, passive, stateless)
    └── decompress/      распаковка сообщений (transform, passive, stateless)
```

Format-плагины предоставляют десериализацию/сериализацию.
Processor и storage ссылаются на format по имени (из `[[formats]]`).
Processor указывает format в `config.input` / `config.output`, framing — явно там же.
Storage указывает format в `storage_config.format` для десериализации (если ему это нужно).

### FFI модель

```
Host (server)                    Plugin (.so)
─────────────                    ────────────
dlopen(plugin.so)
dlsym("qs_abi_version")  ──→    fn qs_abi_version() → u32
    version check
dlsym("qs_create_*")     ──→    fn qs_create_*(ptr, len) → PluginCreateResult
    передаём config JSON          парсит config, создаёт Box<dyn Trait>
    получаем *mut ()       ←──    возвращает pointer
unsafe Box::from_raw
    → Box<dyn Trait>
                          ──→    fn qs_destroy_*(ptr)   Drop
```

---

## Примеры конфигураций

### Пример 1: Простой passthrough (zero-copy)

Данные проходят через систему без десериализации.
Source processor принимает TCP, topic хранит raw bytes, sink processor отдаёт клиентам.

`config.input` / `config.output` указаны в processor-ах с явным framing. Topic не знает format.

```toml
[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

# Topic: ring buffer, не знает format, хранит опак байты
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 50000 }

# Source processor: TCP → framing (\n) → topic
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# Sink processor: topic → framing (\n) → TCP
[[processors]]
name = "realtime-out"
plugin = "./plugins/processor/tcp-sink.so"
source = { topic = "quotes.raw", read = "offset" }
config = {
    port = 9300,
    output = { format = "json", framing = "newline", delimiter = "\n" }
}
```

```
quotes-gen  ──TCP:9100──►  [source: input.format=json, framing=newline]  ──►  Topic "quotes.raw"
                                                                               (memory, 50K records)
                                                                                    │
                                                                      [sink: output.format=json, framing=newline]
                                                                              │
                                                                              ▼
                                                                         TCP:9300  ──► клиенты
```

Данные: `{"symbol":"BTC","bid":50000}\n` — проходят как есть, никто не парсит.
Topic не знает, что это JSON. Processor-ы знают format и framing из `config.input`/`config.output`.

### Пример 2: Protobuf → ClickHouse (колоночное хранение)

Source processor принимает Protobuf (`config.input` с format + framing явно).
ClickHouse storage десериализует (`format` в `storage_config`) и раскладывает по колонкам.

**Откуда берутся колонки?** Из Schema Mapping — `schema_map`
определяет Rhai скрипт, движок строит `TargetSchema` при старте:

1. `storage_config.format = "proto-quote"` → движок резолвит format-плагин → получает `SourceSchema`
2. `storage_config.schema` → движок строит начальный `TargetSchema { fields: [], attrs }`
3. `storage_config.schema_map = "proto-quote-to-clickhouse"` → движок загружает Rhai скрипт
4. Вызывает `map_schema(source, target)` → `TargetSchema` (fields заполнены)
5. `StorageContext { serializer, schema }` → ClickHouse storage при `init()` берёт `schema` → `CREATE TABLE`
6. При `save()` — десериализует данные через serializer, извлекает поля по `schema.fields[].source`

Protobuf message (исходник, из которого скомпилирован `quote.bin`):

```protobuf
// schemas/quote.proto — в proto 5 полей
message Quote {
    string symbol = 1;
    double bid    = 2;
    double ask    = 3;
    int64  volume = 4;
    string exchange = 5;
}
```

Schema map определяет маппинг через Rhai скрипт (фильтрация, типы, extra — всё внутри):

```toml
[[formats]]
name = "proto-quote"
plugin = "./plugins/format/protobuf.so"
config = { descriptor = "schemas/quote.bin", message = "Quote" }

[[schema_maps]]
name = "proto-quote-to-clickhouse"
script = "scripts/schema-map/proto-to-clickhouse.rhai"

# Topic: не знает format. ClickHouse знает format и schema_map из storage_config.
[[topics]]
name = "quotes.structured"
storage = "clickhouse"
storage_config = {
    format = "proto-quote",
    schema_map = "proto-quote-to-clickhouse",
    schema = {
        table = "quotes",
        engine = "MergeTree",
        order_by = "(ts_ms)",
        ttl = "toDateTime(ts_ms / 1000) + INTERVAL 90 DAY",
    },
    host = "clickhouse",
}

# Source processor: input с format и явным framing
[[processors]]
name = "binary-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.structured" }
config = {
    port = 9100,
    input = { format = "proto-quote", framing = "length_prefixed", prefix_type = "u32be" }
}
```

```
TCP:9100  ──►  [source: input.format=proto-quote, framing=length_prefixed]
                        │
                        ▼
               Topic "quotes.structured"
                        │
                 Schema Mapping (при старте):
                   format "proto-quote" → schema() → SourceSchema:
                     symbol: { name: "string", attrs: {} }
                     bid:    { name: "double", attrs: {} }
                     ask:    { name: "double", attrs: {} }
                     volume: { name: "int64",  attrs: {} }
                     exchange: { name: "string", attrs: {} }
                   │
                   storage_config.schema → начальный TargetSchema:
                     { fields: [], attrs: { table, engine, order_by, ttl } }
                   │
                   schema_map "proto-quote-to-clickhouse":
                     map_schema(source, target) — Rhai скрипт:
                       фильтрует (exchange пропущен)
                       маппит типы (double → Float64, string → LowCardinality(String))
                       добавляет extra (ts_ms, wrt_ts, spread)
                   │
                   → TargetSchema (fields заполнены, attrs сохранены)
                   │
                 ClickHouse storage:
                   init(ctx):  ctx.schema → render_type() → CREATE TABLE:
                     │
                     → CREATE TABLE quotes (
                         ts_ms  Int64,                            ← source="ts_ms"
                         sym    LowCardinality(String),           ← source="symbol"
                         bid    Float64,                          ← source="bid"
                         ask    Float64,                          ← source="ask"
                         wrt_ts DateTime64(3) DEFAULT now64(3),   ← extra (нет source)
                         spread Float64 MATERIALIZED ask - bid    ← extra (нет source)
                       ) ENGINE = MergeTree()
                         ORDER BY (ts_ms)
                         TTL toDateTime(ts_ms / 1000) + INTERVAL 90 DAY
                   │
                   save(record):
                     serializer.deserialize(record.data) → Row
                     apply_mapping(row, schema) → mapped Row
                     │
                     extract только fields с source:
                       ts_ms  = record.ts_ms       (TopicRecord)
                       sym    = data["symbol"]      (переименован скриптом)
                       bid    = data["bid"]
                       ask    = data["ask"]
                       wrt_ts  — пропускается (DEFAULT заполнит)
                       spread  — пропускается (MATERIALIZED вычислит)
                     │
                     INSERT INTO quotes (ts_ms, sym, bid, ask)
                       VALUES (record.ts_ms, "BTC", 50000.0, 50001.0)
```

Движок не знает о полях `symbol`, `bid`, `ask` — это знает только ClickHouse
storage (через `TargetSchema` + `serializer` из StorageContext).
Source processor знает format и framing из `config.input`. Topic не знает ничего.

### Пример 3: Fan-out с разными режимами

Один topic, два потребителя: один надёжный (offset + block), другой быстрый (latest).

`storage_config.format` не нужен ни одному из трёх topic-ов — все storage хранят raw bytes.

```toml
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 10000 }

[[topics]]
name = "quotes.reliable"
storage = "file"
storage_config = { data_dir = "./data/reliable", write_full = "block" }

[[topics]]
name = "quotes.realtime"
storage = "memory"
storage_config = { storage_size = 1000 }

# Потребитель 1: offset reader, block — не потеряет ни фрейма
[[processors]]
name = "reliable-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "quotes.reliable" }

# Потребитель 2: latest reader — только актуальные данные
[[processors]]
name = "realtime-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.raw", read = "latest" }
target = { topic = "quotes.realtime" }
```

```
Source ──► Topic "quotes.raw" (memory, 10K)
                   │
         ┌─────────┴─────────┐
         │                   │
   [offset, block]     [latest]
   passthrough          passthrough
         │                   │
         ▼                   ▼
  Topic "quotes.reliable"   Topic "quotes.realtime"
  (file, всё сохранено)     (memory, только актуальное)
```

`reliable-writer` читает offset — каждый фрейм по порядку, ничего не теряет.
`realtime-writer` читает latest — если не успевает, пропущенные не нужны.

Ни один topic не знает format — все хранят опак байты. Format и framing знает только
source processor (через `config.input`), потребители указывают свои `config.input`/`config.output`.

### Пример 4: Table mode (актуальные котировки с upsert)

Topic хранит таблицу актуальных значений per symbol. Потребители запрашивают
snapshot или подписываются на изменения.

```toml
[[formats]]
name = "proto-quote"
plugin = "./plugins/format/protobuf.so"
config = { descriptor = "schemas/quote.bin", message = "Quote" }

# Topic: storage знает format (для десериализации и upsert по key)
[[topics]]
name = "quotes.live"
storage = "memory"
storage_config = { mode = "table", format = "proto-quote", key_field = "symbol" }

# Source processor: input с format и явным framing
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.live" }
config = {
    port = 9100,
    input = { format = "proto-quote", framing = "length_prefixed", prefix_type = "u32be" }
}
```

```
[Source: input.format=proto-quote, framing=length_prefixed]
        │
        ▼
Topic "quotes.live" (memory, table mode)

Storage при save() (format = "proto-quote" из storage_config):
  1. deserialize(data) используя proto-quote serializer (из StorageContext)
  2. extract key = value["symbol"]  (key_field из storage_config)
  3. upsert в таблицу по ключу

  ┌──────────────────────────┐
  │  "BTCUSD"  → latest quote│
  │  "ETHUSD"  → latest quote│
  │  "EURUSD"  → latest quote│
  └──────────────────────────┘

Consumer A (snapshot):
  GET /api/topics/quotes.live/snapshot
  → { "BTCUSD": {...}, "ETHUSD": {...}, "EURUSD": {...} }

Consumer B (subscribe):
  WS subscribe("quotes.live", mode="snapshot")
  → при каждом изменении получает полную таблицу
```

Topic не знает format. Storage знает (из `storage_config.format`).
Source processor знает (из `config.input`). Каждый — из своего конфига.

### Пример 5: Цепочка обработки (Source → Processor → Storage)

Сырые котировки проходят через OHLC-процессор, результат сохраняется в ClickHouse.

```toml
[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

[[formats]]
name = "avro-ohlc"
plugin = "./plugins/format/avro.so"
config = { schema_file = "schemas/ohlc.avsc" }

# Topic 1: сырые котировки — storage не знает format
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 50000 }

# Topic 2: OHLC свечи — ClickHouse знает format и schema_map из storage_config
[[topics]]
name = "ohlc.1m"
storage = "clickhouse"
storage_config = {
    format = "avro-ohlc",
    schema_map = "avro-ohlc-to-clickhouse",
    schema = {
        table = "ohlc_1m",
        partition_by = "toYYYYMM(toDateTime(ts_ms / 1000))",
        order_by = "(ts_ms)",
        ttl = "toDateTime(ts_ms / 1000) + INTERVAL 1 YEAR",
    },
    host = "clickhouse",
}

# Source processor: input с format и явным framing
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# Transform processor: знает input/output format
[[processors]]
name = "ohlc-builder"
plugin = "./plugins/processor/ohlc.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "ohlc.1m" }
config = {
    input  = { format = "json" },
    output = { format = "avro-ohlc" },
    intervals = ["1m", "5m", "1h"]
}
```

```
TCP:9100 ──► [source: input.format=json, framing=newline] ──► Topic "quotes.raw" (memory)
                                                               │
                                                          [offset reader]
                                                               │
                                                               ▼
                                                   Processor "ohlc-builder":
                                                     (input.format=json, output.format=avro-ohlc)
                                                1. deserialize(json bytes) → Quote value
                                                2. aggregate into 1m candle
                                                3. serialize(avro) → candle bytes
                                                4. publish("ohlc.1m", TopicRecord { ts_ms, data })
                                                          │
                                                          ▼
                                              Topic "ohlc.1m":
                                                ClickHouse storage:
                                                  init(): schema (из Schema Mapping Pipeline)
                                                    → CREATE TABLE ohlc_1m (
                                                        ts_ms  Int64,
                                                        symbol String,
                                                        open   Float64,
                                                        high   Float64,
                                                        low    Float64,
                                                        close  Float64,
                                                        volume Int64
                                                      )
                                                  save(): serializer.deserialize(avro)
                                                    → extract по schema → INSERT
                                                  read(): SELECT → serialize(avro) → bytes
```

### Пример 6: Параллельная обработка с fan-out

Один source, три независимых потребителя с разными storage и скоростями.

```toml
[[formats]]
name = "proto-quote"
plugin = "./plugins/format/protobuf.so"
config = { descriptor = "schemas/quote.bin", message = "Quote" }

# Входной topic — не знает format
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 100000 }

# Потребитель 1: table mode — storage знает format для десериализации и upsert
[[topics]]
name = "quotes.live"
storage = "memory"
storage_config = { mode = "table", format = "proto-quote", key_field = "symbol" }

# Потребитель 2: файловый архив — storage не знает format (raw bytes)
[[topics]]
name = "quotes.archive"
storage = "file"
storage_config = { data_dir = "./data/archive", partition_by = "date", write_full = "block" }

# Потребитель 3: ClickHouse — storage знает format и schema_map
[[topics]]
name = "quotes.analytics"
storage = "clickhouse"
storage_config = {
    format = "proto-quote",
    schema_map = "proto-quote-to-clickhouse",
    schema = { table = "quotes_analytics" },
    host = "clickhouse",
    write_full = "drop",
    async_insert = true,
    insert_batch_size = 5000
}

# Source processor — input с format и явным framing
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "proto-quote", framing = "length_prefixed", prefix_type = "u32be" }
}

# Fan-out: три processor-а читают из одного topic-а
[[processors]]
name = "live-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.raw", read = "latest" }
target = { topic = "quotes.live" }

[[processors]]
name = "archive-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "quotes.archive" }

[[processors]]
name = "analytics-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "quotes.analytics" }

# Sink processor — output с format и явным framing
[[processors]]
name = "live-table"
plugin = "./plugins/processor/tcp-sink.so"
source = { topic = "quotes.live", read = "offset" }
config = {
    port = 9300,
    output = { format = "proto-quote", framing = "length_prefixed", prefix_type = "u32be" }
}
```

```
TCP:9100 ──► Topic "quotes.raw" (memory, 100K)
                    │
        ┌───────────┼───────────────┐
        │           │               │
   [latest]    [offset]        [offset]
   passthrough  passthrough     passthrough
        │           │               │
        ▼           ▼               ▼
  "quotes.live"  "quotes.archive"  "quotes.analytics"
  (table mode)   (file, raw)       (ClickHouse, columnar)
   upsert by     append bytes      deserialize → columns
   key(storage)
        │
   [sink: output.format=proto-quote, framing=length_prefixed]
        │
        ▼
   TCP:9300 → клиенты (актуальная таблица)
```

- `live-writer`: latest reader → `quotes.live` (table mode). Пропущенные промежуточные
  обновления не важны. Storage десериализует data, извлекает key по `key_field`, делает upsert.
- `archive-writer`: offset reader → `quotes.archive` (file). Block при переполнении —
  файловая система медленная, но ни один фрейм не потерян. File storage пишет raw bytes.
- `analytics-writer`: offset reader → `quotes.analytics` (ClickHouse). Drop при
  переполнении — ClickHouse может быть медленным при batch INSERT. Потеря части данных
  допустима для аналитики.

### Пример 7: Двойная цепочка с конвертацией формата (через processor)

Source присылает JSON. Один путь — raw хранение. Другой — конвертация в Avro
через processor и сохранение в ClickHouse с колонками.

```toml
[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

[[formats]]
name = "avro-quote"
plugin = "./plugins/format/avro.so"
config = { schema_file = "schemas/quote.avsc" }

# Входной topic — не знает format
[[topics]]
name = "quotes.json"
storage = "memory"
storage_config = { storage_size = 10000 }

# Путь 1: raw файловое хранение — storage не знает format
[[topics]]
name = "quotes.file"
storage = "file"
storage_config = { data_dir = "./data/json-raw", write_full = "block" }

# Путь 2: ClickHouse — storage знает format и schema_map из storage_config
[[topics]]
name = "quotes.avro"
storage = "clickhouse"
storage_config = {
    format = "avro-quote",
    schema_map = "avro-quote-to-clickhouse",
    schema = { table = "quotes" },
    host = "clickhouse",
    write_full = "drop"
}

# Source processor — input с format и явным framing
[[processors]]
name = "json-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.json" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# Путь 1: passthrough в файл
[[processors]]
name = "file-writer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.json", read = "offset" }
target = { topic = "quotes.file" }

# Путь 2: конвертация формата → ClickHouse
[[processors]]
name = "json-to-avro"
plugin = "./plugins/processor/format-convert.so"
source = { topic = "quotes.json", read = "offset" }
target = { topic = "quotes.avro" }
config = {
    input  = { format = "json" },
    output = { format = "avro-quote" }
}
```

```
TCP:9100 ──► [source: input.format=json, framing=newline] ──► Topic "quotes.json" (memory)
                                                          │
                                               ┌──────────┴──────────┐
                                               │                     │
                                        [offset]               [offset]
                                        passthrough             format-convert
                                               │                     │
                                               ▼                     ▼
                                      "quotes.file"          Processor "json-to-avro":
                                      (file, raw bytes)        (input=json, output=avro-quote)
                                                               deserialize(JSON) → Value
                                                               serialize(Avro) → bytes
                                                                     │
                                                                     ▼
                                                               "quotes.avro"
                                                               ClickHouse storage:
                                                               deserialize(Avro) → extract по schema → INSERT
```

`file-writer` — passthrough processor, просто перекладывает записи. Storage не знает format.
Processor `json-to-avro` знает оба формата из своего конфига (`config.input`, `config.output`).
ClickHouse знает format и schema_map из `storage_config` — Schema Mapping Pipeline строит TargetSchema при старте.

### Пример 8: Минимальный raw pipe (два топика, zero-copy)

Простейшая цепочка: данные проходят через два топика с разным framing.
Framing указан явно в processor-ах — format не используется.

```toml
# Raw топики без формата — framing нужно указать явно
[[topics]]
name = "raw.100"
storage = "memory"
storage_config = { storage_size = 100 }

[[topics]]
name = "raw.10"
storage = "memory"
storage_config = { storage_size = 1000, write_full = "block" }

# Transform: перенарезка фреймов (100 байт → 10 байт)
[[processors]]
name = "reslicer"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "raw.100", read = "offset" }
target = { topic = "raw.10" }

# Source processor: raw bytes, только framing без format
[[processors]]
name = "input"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "raw.100" }
config = {
    port = 9100,
    input = { framing = "fixed_size", frame_size = 100 }
}

# Sink processor: raw bytes, только framing без format
[[processors]]
name = "output"
plugin = "./plugins/processor/tcp-sink.so"
source = { topic = "raw.10", read = "offset" }
config = {
    port = 9300,
    output = { framing = "fixed_size", frame_size = 10 }
}
```

```
TCP:9100  ──►  [source: framing 100 bytes]  ──►  Topic "raw.100" (memory)
                                                        │
                                                  [offset, block]
                                                        │
                                                        ▼
                                                  Topic "raw.10":
                                                   один фрейм 100 байт
                                                   → 10 фреймов по 10 байт
                                                        │
                                                 [sink: framing 10 bytes]
                                                │
                                                ▼
                                           TCP:9300 → клиенты
```

Никакой десериализации. Данные — опак байты. Framing нарезает по-разному
на каждом этапе. Processor-ы указывают framing явно — format не используется.

### Пример 9: Агрегация (OHLC) — stateful processor с ключом

OHLC-агрегатор — stateful processor. Десериализует данные, группирует
по ключу (symbol) и временному окну, накапливает state, при закрытии окна
сериализует результат и публикует в target topic.

```toml
[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

[[formats]]
name = "avro-ohlc"
plugin = "./plugins/format/avro.so"
config = { schema_file = "schemas/ohlc.avsc" }

# Входные котировки — storage не знает format
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 50000 }

# State topic (table mode) — storage знает format для upsert по key
[[topics]]
name = "ohlc.state"
storage = "memory"
storage_config = { mode = "table", format = "json", key_field = "window_key" }

# Результат — ClickHouse знает format и schema_map из storage_config
[[topics]]
name = "ohlc.1m"
storage = "clickhouse"
storage_config = {
    format = "avro-ohlc",
    schema_map = "avro-ohlc-to-clickhouse",
    schema = { table = "ohlc_1m" },
    host = "clickhouse",
}

# Source — input с format и явным framing
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# OHLC агрегатор — знает input/output format
[[processors]]
name = "ohlc-builder"
plugin = "./plugins/processor/ohlc.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "ohlc.1m" }
config = {
    input  = { format = "json" },
    output = { format = "avro-ohlc" },
    intervals = ["1m"],
    key_field = "symbol",
    state_topic = "ohlc.state",
    state_format = "json"         # формат state записей
}
```

```
[Source: input.format=json, framing=newline] ──► Topic "quotes.raw" (memory)
                                                │
                                           [offset reader]
                                                │
                                                ▼
                                         Processor "ohlc-builder":
                                           (input.format=json, output.format=avro-ohlc)
                          1. deserialize(json) → { symbol: "BTC", bid: 42100, ... }
                          2. key = value["symbol"]  (из config.key_field)
                          3. window = current_1m_window(ts_ms)
                          4. update state[key, window]:
                               open  = first bid in window
                               high  = max(high, bid)
                               low   = min(low, bid)
                               close = bid
                               volume += 1
                          5. upsert state в "ohlc.state" topic (для recovery)
                          6. при закрытии окна:
                               candle = finalize(state[key, window])
                               serialize(avro) → bytes
                               publish("ohlc.1m", TopicRecord { ts_ms, data })
                               │
                               ▼
                        Topic "ohlc.1m":
                          ClickHouse storage:
                          deserialize(avro) → extract по schema → INSERT
```

Processor — механизм агрегации. Ему нужны:
- **input.format** — из `config.input`, для десериализации входных данных
- **output.format** — из `config.output`, для сериализации результата
- **key_field** — из конфига processor-а, для группировки
- **state** — внутренний (HashMap) или внешний (topic в table mode для recovery)

State topic `ohlc.state` работает в table mode (upsert по `window_key`).
Storage state topic-а знает `format = "json"` из `storage_config`.
При рестарте processor загружает snapshot из state topic и продолжает
с последней позиции. Без state_topic — рестарт начинает с нуля.

### Пример 10: Join (stream-table и stream-stream)

#### Stream-table join: обогащение котировок

Topic `quotes.raw` — поток котировок (высокая частота).
Topic `instruments` — справочник инструментов (table mode, редко меняется).
Processor соединяет каждую котировку с метаданными инструмента.

```toml
[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

# Поток котировок — storage не знает format
[[topics]]
name = "quotes.raw"
storage = "memory"
storage_config = { storage_size = 50000 }

# Справочник инструментов — storage знает format для table mode (upsert)
[[topics]]
name = "instruments"
storage = "memory"
storage_config = { mode = "table", format = "json", key_field = "symbol" }

# Результат — storage не знает format
[[topics]]
name = "quotes.enriched"
storage = "memory"
storage_config = { storage_size = 50000 }

# Join processor — знает input/output format из конфига
[[processors]]
name = "enrich-quotes"
plugin = "./plugins/processor/join.so"
source = { topic = "quotes.raw", read = "offset" }
target = { topic = "quotes.enriched" }
config = {
    input  = { format = "json" },
    output = { format = "json" },
    join_topic = "instruments",
    join_read = "snapshot",       # загрузить таблицу + подписаться на изменения
    join_key = "symbol",
    join_type = "left"            # котировка проходит даже без инструмента
}
```

```
Topic "quotes.raw"         Topic "instruments" (table mode)
     │                           │
[offset reader]            [snapshot + subscribe]
     │                           │
     ▼                           ▼
  Processor "enrich-quotes":
    │
    ├── при init: загружает snapshot из "instruments"
    │   → HashMap<symbol, instrument_data>
    │
    ├── при изменении "instruments":
    │   обновляет HashMap (subscribe)
    │
    ├── для каждой quote из "quotes.raw":
    │   1. deserialize(json) → { symbol: "BTC", bid: 42100 }
    │   2. lookup = instruments["BTC"]
    │       → { name: "Bitcoin", exchange: "Binance", lot_size: 0.001 }
    │   3. merge: { symbol: "BTC", bid: 42100,
    │               name: "Bitcoin", exchange: "Binance" }
    │   4. serialize(json) → bytes
    │   5. publish("quotes.enriched", TopicRecord)
    │
    ▼
Topic "quotes.enriched"
```

Это **stream-table join** (аналог KStream-KTable join в Kafka Streams):
- Поток (quotes) — offset reader, каждая запись обрабатывается
- Таблица (instruments) — snapshot при init + subscribe на изменения
- Lookup — O(1) по HashMap в памяти processor-а
- Синхронизация простая: таблица всегда актуальна через subscribe

#### Stream-stream join: windowed

Два потока, join по ключу в пределах временного окна.
Обе стороны буферизуются — processor держит два буфера.

```toml
# Поток сделок — storage не знает format
[[topics]]
name = "trades"
storage = "memory"
storage_config = { storage_size = 50000 }

# Поток ордеров — storage не знает format
[[topics]]
name = "orders"
storage = "memory"
storage_config = { storage_size = 50000 }

# Результат — storage не знает format
[[topics]]
name = "matched"
storage = "memory"
storage_config = { storage_size = 50000 }

# Windowed join processor — знает input/output format из конфига
[[processors]]
name = "trade-order-join"
plugin = "./plugins/processor/window-join.so"
source = { topic = "trades", read = "offset" }
target = { topic = "matched" }
config = {
    input  = { format = "json" },
    output = { format = "json" },
    join_topic = "orders",
    join_read = "offset",
    join_key = "order_id",
    window = "5s",                # join window: ±5 секунд
    join_type = "inner"
}
```

```
Topic "trades"              Topic "orders"
     │                           │
[offset reader]            [offset reader]
     │                           │
     ▼                           ▼
  Processor "trade-order-join":
    │
    ├── буфер trades:  HashMap<order_id, Vec<trade>> (window 5s)
    ├── буфер orders:  HashMap<order_id, Vec<order>> (window 5s)
    │
    ├── при trade:
    │   1. добавить в буфер trades
    │   2. lookup orders[order_id]
    │   3. если match → emit joined record
    │   4. expire записи старше window
    │
    ├── при order:
    │   1. добавить в буфер orders
    │   2. lookup trades[order_id]
    │   3. если match → emit joined record
    │   4. expire записи старше window
    │
    ▼
Topic "matched"
```

Синхронизация: processor подписан на оба topic-а, обрабатывает записи
в порядке поступления (interleaving). Window определяет время жизни буфера.
При каждой записи с любой стороны — lookup в буфере другой стороны.

### Пример 11: Arrow IPC streaming → конвертация в JSON

Source processor принимает Arrow IPC streaming по TCP. Каждый RecordBatch — один фрейм.
Transform конвертирует Arrow → JSON для потребителей, которым нужен JSON.

Arrow IPC streaming имеет встроенный framing: continuation bytes (0xFFFFFFFF) + message size.
Для source/sink это `framing = "arrow_ipc_streaming"`.

Arrow IPC file формат (с footer) используется storage напрямую — для записи .arrow файлов.

```toml
[[formats]]
name = "arrow-quotes"
plugin = "./plugins/format/arrow.so"
config = { schema_file = "schemas/quotes.arrow" }

[[formats]]
name = "json"
plugin = "./plugins/format/json.so"

# Topic: хранит Arrow RecordBatch-и как опак байты
[[topics]]
name = "quotes.arrow"
storage = "memory"
storage_config = { storage_size = 10000 }

# Topic: JSON для потребителей
[[topics]]
name = "quotes.json"
storage = "memory"
storage_config = { storage_size = 10000 }

# Topic: Arrow IPC file архив — storage пишет .arrow файлы
[[topics]]
name = "quotes.arrow-archive"
storage = "file"
storage_config = {
    data_dir = "./data/arrow-archive",
    format = "arrow-quotes",
    file_format = "arrow_ipc_file"
}

# Source: Arrow IPC streaming по TCP
[[processors]]
name = "arrow-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.arrow" }
config = {
    port = 9100,
    input = { format = "arrow-quotes", framing = "arrow_ipc_streaming" }
}

# Transform: Arrow → JSON
[[processors]]
name = "arrow-to-json"
plugin = "./plugins/processor/format-convert.so"
source = { topic = "quotes.arrow", read = "offset" }
target = { topic = "quotes.json" }
config = {
    input  = { format = "arrow-quotes" },
    output = { format = "json" }
}

# Passthrough: Arrow → file архив
[[processors]]
name = "arrow-archiver"
plugin = "./plugins/processor/passthrough.so"
source = { topic = "quotes.arrow", read = "offset" }
target = { topic = "quotes.arrow-archive" }
```

```
TCP:9100 (Arrow IPC stream)
    │
    ▼
[Source: input.format=arrow-quotes, framing=arrow_ipc_streaming]
    │
    ▼
Topic "quotes.arrow" (memory, 10K records)
    │
    ├── [offset] ──► format-convert (input=arrow, output=json) ──► Topic "quotes.json"
    │
    └── [offset] ──► passthrough ──► Topic "quotes.arrow-archive"
                                     (file storage, arrow_ipc_file)
```

Source processor разбирает Arrow IPC stream на RecordBatch-и (каждый — один TopicRecord).
Topic хранит RecordBatch-и как опак байты. Transform десериализует Arrow → сериализует JSON.
File storage использует Arrow IPC file формат для записи .arrow файлов с footer.

---

## Zero-copy системные вызовы

Linux предоставляет четыре syscall-а для передачи данных без копирования в userspace:

| Syscall | Направление | Ограничение |
|---------|------------|-------------|
| `sendfile(out_fd, in_fd, ...)` | файл → сокет (файл → файл на ≥5.3) | in_fd — файл (поддерживает mmap) |
| `splice(fd_in, fd_out, ...)` | pipe ↔ сокет/файл | один конец обязан быть pipe |
| `tee(fd_in, fd_out, ...)` | pipe → pipe (дубль без потребления) | оба конца — pipe |
| `copy_file_range(fd_in, fd_out, ...)` | файл → файл в kernel space | оба — файлы |

Суть: данные перемещаются в kernel space, минуя userspace buffer.
Нет `read() → Vec<u8> → write()` — ядро копирует напрямую между fd.

### Конфликт с TopicRecord

Текущая модель: `TopicRecord { ts_ms, data: Vec<u8> }`. Чтобы создать запись,
processor обязан:
1. `read()` байты из сокета в userspace
2. Применить framing (найти границы фреймов)
3. Записать `ts_ms`
4. Передать `data: Vec<u8>` в storage

Шаг 1 уже исключает zero-copy. **Framing требует видеть данные в userspace.**

Это значит: основной pipeline (framing → TopicRecord → storage) всегда проходит
через userspace. Zero-copy применим только в **специфических участках**, где
данные не нужны в userspace.

### Где zero-copy работает

**1. File storage → Socket: `sendfile` (replay/catch-up)**

```
File storage (файл на диске)
    │
    ▼  sendfile(socket_fd, file_fd, offset, count)
    │  данные: disk → kernel page cache → NIC
    │  НЕ проходят через userspace
    │
Socket (клиент)
```

Sink processor отдаёт клиенту исторические данные из file storage.
Фреймы уже записаны в файле — отправляем как есть, без `read() → write()`.
Это самый ценный кейс — на hot path при catch-up подписчиков.

**2. Socket → File: `splice` (raw capture)**

```
Socket (источник)
    │
    ▼  splice(socket_fd → pipe_fd)     zero-copy
    │  splice(pipe_fd → file_fd)       zero-copy
    │
File (на диске)
```

Source processor в passthrough-режиме: TCP поток пишется в файл без парсинга.
Для аудита/архивирования сырого потока. Без framing — нет TopicRecord,
нет ts_ms на каждую запись.

**3. Socket → Socket: `splice` (proxy)**

```
Socket (источник)
    │
    ▼  splice(socket_in → pipe_fd)     zero-copy
    │  splice(pipe_fd → socket_out)    zero-copy
    │
Socket (клиент)
```

Чистый прокси. Нет framing, нет обработки, нет записи в topic.

**4. Fan-out: `tee` + `splice`**

```
Socket (источник)
    │
    splice(socket → pipe_main)
    │
    tee(pipe_main → pipe_copy)         дублирование без потребления
    │                │
splice(→ file_fd)  splice(→ socket_out)
```

Одновременно архив + отдача клиенту без двойного копирования.

**5. File → File: `copy_file_range` (бэкап/ротация)**

```
copy_file_range(fd_src, fd_dst, len)   файл → файл в kernel space
```

Копирование/ротация файлов file storage. Не на hot path.

### Два режима processor-а

Для поддержки zero-copy processor может работать в двух режимах:

```
FRAMED (default):
  read() → Vec<u8> → framing → TopicRecord { ts_ms, data } → storage
  Полноценная обработка. Zero-copy невозможен.

PASSTHROUGH (zero-copy):
  splice/sendfile между fd напрямую
  Нет framing. Нет TopicRecord. Поток байтов как есть.
```

```toml
# Обычный processor с framing — zero-copy: нет
[[processors]]
name = "market-feed"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "quotes.raw" }
config = {
    port = 9100,
    input = { format = "json", framing = "newline", delimiter = "\n" }
}

# Raw passthrough — zero-copy: да (splice socket → file)
[[processors]]
name = "raw-capture"
plugin = "./plugins/processor/tcp-source.so"
target = { topic = "capture.raw" }
config = { port = 9200, mode = "passthrough" }
```

### Применимость в Gauss

| Участок | Syscall | Польза | Когда |
|---------|---------|--------|-------|
| Replay из file storage клиенту | `sendfile` | **Высокая** | catch-up подписчик читает историю |
| Raw capture TCP → файл | `splice` | **Средняя** | аудит/архив сырого потока |
| Fan-out: файл + клиент | `tee` + `splice` | **Средняя** | одновременная запись и отдача |
| File → File ротация | `copy_file_range` | **Низкая** | бэкап, не hot path |
| TCP proxy (сокет → сокет) | `splice` | **Низкая** | Gauss не прокси, ему нужен framing |

**Главный вывод**: zero-copy — точечная оптимизация, не общая стратегия.
Основной pipeline всегда проходит через userspace (framing обязывает).
Самый ценный кейс — `sendfile` для replay из file storage.
