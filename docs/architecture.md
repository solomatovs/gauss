# Gauss -- Схема архитектуры и взаимодействия компонентов

## Общая схема системы

```
┌────────────────────────────────────────────────────────────────────────┐
│                               DEPLOY                                   │
│                                                                        │
│  ┌──────────────┐        ┌─────────────────────────────────────────┐   │
│  │  quotes-gen  │  TCP   │            gauss-server                 │   │
│  │  (генератор) ├───────▶│                                         │   │
│  └──────────────┘ :9100  │  ┌─────────┐ ┌──────────┐ ┌──────────┐ │   │
│                          │  │ Sources ├▶│  Topics  ├▶│Processors│ │   │
│                          │  └─────────┘ │ Registry │ └──────────┘ │   │
│                          │              │          │ ┌──────────┐ │   │
│                          │              │          ├▶│  Sinks   │ │   │
│                          │              └──────────┘ └──────────┘ │   │
│                          │              ┌──────────┐              │   │
│                          │              │   API    │ :9200        │   │
│                          │              │  Server  ├──────────┐   │   │
│                          │              └──────────┘          │   │   │
│                          └─────────────────────────────────────┘   │
│                                                              │     │
│                                                              ▼     │
│                                                     ┌────────────┐ │
│                                                     │  Grafana   │ │
│                                                     │ datasource │ │
│                                                     └────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

## Бинарники (bins/)

```
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│    gauss-server      │  │     quotes-gen       │  │ grafana-datasource  │
│    bins/server       │  │  bins/quotes-gen     │  │ bins/grafana-       │
│                      │  │                      │  │   datasource        │
├──────────────────────┤  ├──────────────────────┤  ├─────────────────────┤
│ Основной сервер.     │  │ Генератор котировок. │  │ Grafana backend     │
│ Принимает данные     │  │ Создаёт синтетичес-  │  │ plugin. HTTP клиент │
│ через source-        │  │ кие Quote, отправля- │  │ к gauss-server API. │
│ плагины, обрабаты-   │  │ ет через sink-       │  │ Streaming через     │
│ вает processor'ами,  │  │ плагины (TCP +       │  │ WebSocket.          │
│ хранит в storage,    │  │ framing + codec).    │  │                     │
│ раздаёт через API.   │  │                      │  │ Запросы: candles,   │
│                      │  │ Режимы: auto,        │  │ quotes.             │
│                      │  │ manual, generate,    │  │                     │
│                      │  │ file.                │  │                     │
├──────────────────────┤  ├──────────────────────┤  ├─────────────────────┤
│ Зависимости:         │  │ Зависимости:         │  │ Зависимости:        │
│  api, plugin-host,   │  │  api, plugin-host    │  │  api                │
│  topic-engine,       │  │                      │  │  grafana-plugin-sdk │
│  pipeline,           │  │                      │  │  reqwest            │
│  topic-api-server    │  │                      │  │  tungstenite        │
└──────────────────────┘  └──────────────────────┘  └─────────────────────┘
```

## Библиотеки (libs/) -- граф зависимостей

```
                     ┌─────────────────────┐
                     │      libs/api       │
                     │    (server-api)     │
                     │                     │
                     │ Типы:               │
                     │   TopicRecord       │
                     │   RecordData        │
                     │   TopicQuery        │
                     │   OverflowPolicy    │
                     │   DataFormat        │
                     │                     │
                     │ Трейты:             │
                     │   TopicStorage      │
                     │   TopicProcessor    │
                     │   TopicSink         │
                     │   Transport         │
                     │   Framing           │
                     │   Codec             │
                     │   FormatSerializer  │
                     │   Middleware        │
                     │                     │
                     │ FFI:                │
                     │   PluginCreateResult│
                     │   CreatePluginFn    │
                     └────────┬────────────┘
                              │
              ┌───────────────┼────────────────┐
              │               │                │
              ▼               ▼                ▼
┌───────────────────┐ ┌──────────────┐ ┌────────────────┐
│ libs/plugin-host  │ │ libs/topic-  │ │ libs/pipeline  │
│                   │ │   engine     │ │                │
│ Загрузка .so      │ │              │ │ Конфигурация:  │
│ через libloading  │ │ Topic:       │ │  FormatConfig  │
│                   │ │  storage +   │ │  TopicConfig   │
│ Обёртки:          │ │  subscribers │ │  SourceConfig  │
│  PluginTransport  │ │  + format    │ │  ProcessorConf │
│  PluginFraming    │ │              │ │  SinkConfig    │
│  PluginCodec      │ │ TopicRegistry│ │                │
│  PluginStorage    │ │  (impl       │ │ Оркестрация:   │
│  PluginProcessor  │ │  Process-    │ │  spawn_source  │
│  PluginSink       │ │  Context)    │ │  spawn_process │
│  PluginMiddleware │ │              │ │  spawn_sink    │
│  PluginFormat     │ │ MemoryStorag │ │  spawn_pipelin │
└───────────────────┘ └──────┬───────┘ └───────┬────────┘
                             │                 │
                             └────────┬────────┘
                                      ▼
                          ┌──────────────────────┐
                          │ libs/topic-api-server │
                          │                      │
                          │ Axum HTTP + WS       │
                          │                      │
                          │ GET /api/topics      │
                          │ GET /api/topics/{n}  │
                          │ WS  /ws              │
                          └──────────────────────┘
```

## Плагины (plugins/) -- каталог

```
plugins/
├── transport/                ── Сетевой ввод/вывод ──
│   ├── tcp-server/           Transport: bind + accept (sources и sinks)
│   └── tcp-client/           Transport: connect (loopback и downstream)
│
├── framing/                  ── Границы сообщений ──
│   ├── lines/                Framing: разделитель \n (текстовые)
│   └── length-prefixed/      Framing: u32 BE длина (бинарные)
│
├── format/                   ── Кодеки (Codec + FormatSerializer) ──
│   ├── json/                 bytes <-> JSON <-> TopicRecord
│   ├── csv/                  bytes <-> CSV  <-> TopicRecord
│   ├── protobuf/             bytes <-> Protobuf <-> TopicRecord
│   └── avro/                 bytes <-> Avro <-> TopicRecord
│
├── storage/                  ── Хранилища ──
│   ├── file/                 JSONL, upsert по key+ts
│   ├── raw-file/             Бинарные файлы, append-only
│   └── clickhouse/           HTTP API к ClickHouse
│
├── stage/                    ── Процессоры ──
│   ├── ohlc/                 Quote -> OHLC Candle (N таймфреймов)
│   └── symbol-filter/        Фильтр по списку символов
│
└── middleware/                ── Middleware ──
    └── compress/             gzip/lz4 сжатие/распаковка
```

## Внутренняя архитектура gauss-server

```
┌──────────────────────────────────────────────────────────────────┐
│                         gauss-server                             │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                     TopicRegistry                          │  │
│  │                  (impl ProcessContext)                      │  │
│  │                                                            │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │  │
│  │  │ quotes.raw  │  │  ohlc.1s    │  │  ohlc.1m    │  ...   │  │
│  │  │             │  │             │  │             │        │  │
│  │  │ storage:    │  │ storage:    │  │ storage:    │        │  │
│  │  │  Memory     │  │  File (.so) │  │  File (.so) │        │  │
│  │  │  (ring buf) │  │  (.jsonl)   │  │  (.jsonl)   │        │  │
│  │  │             │  │             │  │             │        │  │
│  │  │ format:     │  │ format:     │  │ format:     │        │  │
│  │  │  json       │  │  json       │  │  json       │        │  │
│  │  │             │  │             │  │             │        │  │
│  │  │ subscribers:│  │ subscribers:│  │ subscribers:│        │  │
│  │  │  [mpsc tx]  │  │  [mpsc tx]  │  │  [mpsc tx]  │        │  │
│  │  └──────┬──────┘  └─────────────┘  └─────────────┘        │  │
│  │         │                                                  │  │
│  └─────────┼──────────────────────────────────────────────────┘  │
│            │ publish()                                            │
│  ┌─────────┴──────────────────────────────────────────────────┐  │
│  │                       Data Flow                            │  │
│  │                                                            │  │
│  │  SOURCES            PROCESSORS            SINKS            │  │
│  │                                                            │  │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  │  │
│  │  │ tcp-json      │  │ stage-ohlc    │  │ tcp-json-out  │  │  │
│  │  │               │  │               │  │               │  │  │
│  │  │ TCP :9100     │  │ trigger:      │  │ TCP :9300     │  │  │
│  │  │  -> framing   │  │  quotes.raw   │  │ subscribe:    │  │  │
│  │  │  -> codec     │  │               │  │  quotes.raw   │  │  │
│  │  │  -> topic     │  │ publishes to: │  │               │  │  │
│  │  │   quotes.raw  │  │  ohlc.1s      │  │ codec-json    │  │  │
│  │  │               │  │  ohlc.5s      │  │  -> framing   │  │  │
│  │  │               │  │  ohlc.1m      │  │  -> transport │  │  │
│  │  │               │  │  ohlc.1h ...  │  │  -> клиенты   │  │  │
│  │  └───────────────┘  └───────────────┘  └───────────────┘  │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                   API Server (:9200)                        │  │
│  │                                                            │  │
│  │  GET /api/topics        -> список topic'ов                 │  │
│  │  GET /api/topics/{name} -> query (key, from, to, limit)    │  │
│  │  WS  /ws                -> subscribe/unsubscribe/query     │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## Поток данных: Quote -> OHLC -> клиент

```
quotes-gen                            gauss-server
----------                            ------------

Symbol.tick()
    │
    ▼
┌──────────┐
│  Codec   │ serialize(Quote) -> Vec<u8>
│  (JSON)  │ {"symbol":"EURUSD","bid":1.085,...}
└────┬─────┘
     ▼
┌──────────┐
│ Framing  │ encode(data, buf) -> append \n
│ (lines)  │
└────┬─────┘
     ▼
┌──────────┐                  ┌──────────────┐
│Transport │  == TCP :9100 ==>│  Transport   │ accept() -> TcpStream
│(tcp-cli) │                  │ (tcp-server) │
└──────────┘                  └──────┬───────┘
                                     │ blocking thread
                                     ▼
                              ┌──────────────┐
                              │   Framing    │ decode(buf) -> frame
                              │   (lines)    │
                              └──────┬───────┘
                                     ▼
                              ┌──────────────┐
                              │ Middleware?   │ decode(bytes) -> bytes
                              │ (compress)    │ (опционально)
                              └──────┬───────┘
                                     ▼
                              ┌──────────────┐
                              │    Codec     │ decode(bytes) -> TopicRecord
                              │   (JSON)     │ {ts_ms, key:"EURUSD", data}
                              └──────┬───────┘
                                     ▼
                              ┌──────────────┐
                              │ mpsc channel │ buffer: 8192
                              │              │ overflow: back_pressure
                              └──────┬───────┘
                                     │ async task
                                     ▼
                          ┌────────────────────┐
                          │ Topic "quotes.raw"  │
                          │                    │
                          │ 1. storage.save()  │ MemoryStorage
                          │ 2. notify subs     │ (ring buffer)
                          └─────────┬──────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                  │
                  ▼                 ▼                  ▼
        ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
        │  Processor:  │  │    Sink:     │  │ WS subscriber│
        │  stage-ohlc  │  │ tcp-json-out │  │ (API Server) │
        │              │  │              │  │              │
        │ process(rec) │  │ codec.encode │  │ -> JSON      │
        │  -> bin 1s   │  │ framing.enc  │  │ -> WS msg    │
        │  -> bin 1m   │  │ transport    │  │ -> client    │
        │  -> bin 1h   │  │  -> TCP:9300 │  │              │
        └──────┬───────┘  └──────────────┘  └──────────────┘
               │
               │ ctx.publish("ohlc.1m", candle)
               ▼
     ┌────────────────────┐
     │  Topic "ohlc.1m"   │
     │                    │
     │ 1. storage.save()  │ FileStorage
     │ 2. notify subs     │ ./data/ohlc/1m/EURUSD.jsonl
     └─────────┬──────────┘
               │
               ▼
     ┌────────────────────┐
     │  WS / REST client  │ Grafana, dashboards ...
     └────────────────────┘
```

## Трёхслойная плагинная модель (Source / Sink pipeline)

```
                    SOURCE PIPELINE (inbound)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  Transport       Framing        Middleware?     Codec    │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐  ┌───────┐ │
│  │tcp-server│──>│  lines   │──>│ compress │─>│ json  │ │
│  │tcp-client│   │  len-pfx │   │ (gzip)   │  │ csv   │ │
│  │          │   │          │   └──────────┘  │ proto │ │
│  │  bytes   │   │  frame   │    decompress   │ avro  │ │
│  └──────────┘   └──────────┘                 └───┬───┘ │
│                                                  │     │
│                                       TopicRecord│     │
└──────────────────────────────────────────────────┼─────┘
                                                   ▼
                                          publish(topic)


                    SINK PIPELINE (outbound)
                  subscribe(topic)
                        │
┌───────────────────────┼──────────────────────────────────┐
│                       ▼                                  │
│  Codec       Middleware?      Framing        Transport   │
│  ┌───────┐  ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │ json  │─>│ compress │──>│  lines   │──>│tcp-server│ │
│  │ csv   │  │ (gzip)   │   │  len-pfx │   │tcp-client│ │
│  │ proto │  └──────────┘   │          │   │          │ │
│  │ avro  │   compress      │ + frame  │   │  bytes   │ │
│  └───────┘                 └──────────┘   └──────────┘ │
│                                                        │
└────────────────────────────────────────────────────────┘
```

## Комбинации плагинов (из config.toml)

```
┌──────────────────────────────────────────────────────────────┐
│                    Текстовые протоколы                        │
│                                                              │
│  TCP  +  lines (\n)  +  JSON    <-- основной (quotes.raw)   │
│  TCP  +  lines (\n)  +  CSV     <-- loopback (quotes.csv)   │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                    Бинарные протоколы                         │
│                                                              │
│  TCP  +  length-prefixed (u32 BE)  +  Protobuf  <-- loopback│
│  TCP  +  length-prefixed (u32 BE)  +  Avro      <-- loopback│
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                    С middleware                               │
│                                                              │
│  TCP  +  length-prefixed  +  gzip  +  JSON      <-- сжатие  │
└──────────────────────────────────────────────────────────────┘
```

## Сетевые порты (по умолчанию)

```
┌────────┬──────────────────────────────────────────┐
│  Порт  │  Назначение                              │
├────────┼──────────────────────────────────────────┤
│  9100  │  Source: TCP JSON inbound (quotes-gen ->) │
│  9200  │  API Server: HTTP REST + WebSocket        │
│  9300  │  Sink: TCP JSON outbound (-> клиенты)     │
│  9301  │  Sink/Source loopback: CSV                │
│  9302  │  Sink/Source loopback: Protobuf           │
│  9303  │  Sink/Source loopback: Avro               │
└────────┴──────────────────────────────────────────┘

quotes-gen  ==TCP==>  :9100  (gauss-server source)
grafana     ==HTTP=>  :9200  (gauss-server API)
grafana     ==WS===>  :9200/ws
клиенты     <=TCP===  :9300  (gauss-server sink, fan-out)
```

## Loopback-схема (формат-конвертация)

```
Один и тот же поток котировок транслируется в CSV,
Protobuf и Avro через loopback-пары sink <-> source.

                 quotes.raw (JSON, memory)
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │ Sink: CSV  │ │ Sink: Proto│ │ Sink: Avro │
  │ :9301      │ │ :9302      │ │ :9303      │
  │            │ │            │ │            │
  │ encode JSON│ │ encode JSON│ │ encode JSON│
  │ -> CSV     │ │ -> Proto   │ │ -> Avro    │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        │ TCP          │ TCP          │ TCP
        ▼              ▼              ▼
  ┌────────────┐ ┌────────────┐ ┌────────────┐
  │ Source: CSV│ │Source:Proto│ │Source: Avro│
  │ tcp-client │ │ tcp-client │ │ tcp-client │
  │ 127.0.0.1  │ │ 127.0.0.1  │ │ 127.0.0.1  │
  │ :9301      │ │ :9302      │ │ :9303      │
  │            │ │            │ │            │
  │ decode CSV │ │ decode Pro │ │ decode Avro│
  │ ->TopicRec │ │ ->TopicRec │ │ ->TopicRec │
  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
        ▼              ▼              ▼
   quotes.csv    quotes.proto    quotes.avro
   (memory)      (memory)        (memory)
```

## Topic publish/subscribe модель

```
                  Topic.publish(record)
                          │
               ┌──────────┼──────────┐
               ▼          │          ▼
       1. storage.save()  │  2. notify subscribers
                          │
             ┌────────────┼────────────────────┐
             │            │                    │
             ▼            ▼                    ▼
      ┌────────────┐ ┌────────────┐ ┌──────────────────┐
      │ Processor  │ │  Pipeline  │ │  WS subscriber   │
      │ subscriber │ │   Sink     │ │ (topic-api-serv) │
      │            │ │ subscriber │ │                  │
      │ mpsc rx    │ │ mpsc rx    │ │ mpsc rx          │
      │ buf: 4096  │ │ buf: 8192  │ │ buf: ws_buffer   │
      │ overflow:  │ │ overflow:  │ │ overflow:        │
      │ back_press │ │ drop       │ │ drop             │
      └────────────┘ └────────────┘ └──────────────────┘

Overflow policies:
  Drop         -- try_send(): канал полон -> сообщение теряется
  BackPressure -- send().await: спавнит задачу, ждёт место
```

## FFI плагинная модель

```
┌───────────────────────┐        ┌───────────────────────┐
│    Host (server)      │        │    Plugin (.so)        │
│                       │        │                       │
│ libloading::Library   │        │ #[no_mangle]          │
│     │                 │        │ extern "C" fn         │
│     │ dlopen()        │        │ qs_create_codec(      │
│     │ dlsym()         │ -FFI-->│   ptr, len            │
│     ▼                 │        │ ) -> PluginCreateResult│
│ CreatePluginFn        │        │                       │
│     │                 │        │ Box<Box<dyn Codec>>   │
│     ▼                 │        │  -> *mut () (thin ptr)│
│ PluginCreateResult    │<-------│                       │
│  .plugin_ptr ─────────┼──┐     │                       │
│                       │  │     │ qs_destroy_codec(ptr)  │
│ unsafe Box::from_raw  │<─┘     │  Box::from_raw -> Drop│
│  -> Box<dyn Codec>    │        │                       │
└───────────────────────┘        └───────────────────────┘

Типы плагинов и их FFI символы:

┌──────────────────┬─────────────────────┬─────────────────────────┐
│ Трейт            │ qs_create_*         │ qs_destroy_*            │
├──────────────────┼─────────────────────┼─────────────────────────┤
│ Codec            │ qs_create_codec     │ qs_destroy_codec        │
│ FormatSerializer │ qs_create_format    │ qs_destroy_format       │
│ Transport        │ qs_create_transport │ qs_destroy_transport    │
│ Framing          │ qs_create_framing   │ qs_destroy_framing      │
│ TopicStorage     │ qs_create_storage   │ qs_destroy_storage      │
│ TopicProcessor   │ qs_create_processor │ qs_destroy_processor    │
│ TopicSink        │ qs_create_sink      │ qs_destroy_sink         │
│ Middleware       │ qs_create_middleware│ qs_destroy_middleware   │
└──────────────────┴─────────────────────┴─────────────────────────┘

Конфиг: TOML -> JSON строка -> (ptr, len) через FFI -> serde_json
```

## Взаимодействие с внешними системами

```
┌──────────────┐                              ┌──────────────┐
│  quotes-gen  │                              │   Grafana    │
│              │                              │              │
│  TCP client  │── :9100 (JSON/lines) ──>     │  HTTP GET    │
│              │                         │    │  /api/topics │
└──────────────┘                         │    │  /api/topics/│
                                         │    │    {name}    │
                      gauss-server       │    │              │
                     ┌───────────┐       │    │  WS /ws      │
                     │           │       │    │   subscribe  │
┌──────────────┐     │   :9100   │<──────┘    │   query      │
│  TCP клиенты │     │   :9200   │<───────────┤              │
│ (downstream) │<────┤   :9300   │            └──────────────┘
│              │     │           │
└──────────────┘     └─────┬─────┘
                           │
              ┌────────────┼────────────┐
              ▼            │            ▼
      ┌────────────┐      │    ┌────────────┐
      │ ClickHouse │      │    │  Файловая  │
      │ (опционал) │      │    │  система   │
      │            │      │    │            │
      │ HTTP API   │      │    │ ./data/    │
      └────────────┘      │    │  ohlc/     │
                          │    │   1s/ 1m/  │
                          │    │   ...      │
                          │    │  {SYM}.jsonl│
                          │    └────────────┘
                          │
                    ┌─────┴──────┐
                    │  schemas/  │
                    │ quote.proto│
                    │ quote.avsc │
                    │ quote.desc │
                    └────────────┘
```

## Docker-инфраструктура

```
build/Dockerfile.build (multi-stage)

┌─────────────────────────────────────────────────┐
│ Stage 1: builder (Rust 1.88, Alpine)            │
│                                                 │
│   cargo build --release                         │
│     -> gauss-server binary                      │
│     -> quotes-gen binary                        │
│     -> libcodec_json.so, libcodec_csv.so ...    │
│     -> libframing_lines.so ...                  │
│     -> libstorage_file.so ...                   │
│     -> libstage_ohlc.so ...                     │
│     -> libtransport_tcp_server.so ...           │
│     -> libmiddleware_compress.so                │
├─────────────────────────────────────────────────┤
│ Stage 2: runtime (Alpine 3.22, minimal)         │
│                                                 │
│   COPY binaries + plugins + libgcc_s.so.1       │
│   COPY config.toml + schemas/                   │
│   ENTRYPOINT: gauss-server --config config.toml │
└─────────────────────────────────────────────────┘

docker-compose.yml (production)

┌─────────────────────────────────────────────────┐
│  gauss-server       :9100, :9200, :9300         │
│  quotes-gen         -> gauss-server:9100        │
│  grafana            -> gauss-server:9200        │
│  clickhouse         <- gauss-server (опционал)  │
└─────────────────────────────────────────────────┘
```
