import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

// ─── Format types ────────────────────────────────────────────

/** Supported data formats. Phase 1: only 'json' is implemented. */
export type DataFormat = 'json' | 'csv' | 'protobuf' | 'avro';

/** How the plugin connects to get data. Phase 1: only 'http' is implemented. */
export type ConnectionMode = 'http' | 'tcp';

/** Field value types that determine Grafana column type and extraction logic. */
export type FieldValueType = 'number' | 'string' | 'boolean' | 'time';

// ─── Field auto-discovery ───────────────────────────────────

/** A field suggestion returned by the backend /fields resource endpoint. */
export interface FieldSuggestion {
  path: string;
  type: FieldValueType;
}

// ─── Per-field configuration ─────────────────────────────────

/**
 * Configuration for a single extracted field.
 * The `path` semantics depend on the format:
 *   - json:     dot-notation path (e.g. "bid", "nested.price.value")
 *   - csv:      column index as string (e.g. "0", "3") or column name
 *   - protobuf: dotted field path (e.g. "quote.bid_price")
 *   - avro:     dotted field path
 */
export interface FieldConfig {
  /** Path/accessor within the record data. */
  path: string;
  /** Display alias in Grafana (if empty, path is used). */
  alias: string;
  /** Expected value type. */
  type: FieldValueType;
}

// ─── Query model ─────────────────────────────────────────────

export interface QuotesQuery extends DataQuery {
  /** Topic name (e.g. "quotes.raw", "ohlc.1m"). Supports $variables. */
  topic: string;
  /** Partition key filter (e.g. "EURUSD"). Supports $variables. */
  key: string;
  /** Data format of the topic's messages. */
  format: DataFormat;
  /** Connection mode. Phase 1: always 'http'. */
  connectionMode: ConnectionMode;
  /** Individual field extraction configurations. */
  fieldConfigs: FieldConfig[];
  /** @deprecated Use fieldConfigs instead. Kept for backward compat. */
  fields?: string;
}

export const DEFAULT_QUERY: Partial<QuotesQuery> = {
  topic: 'quotes.raw',
  key: 'EURUSD',
  format: 'json',
  connectionMode: 'http',
  fieldConfigs: [
    { path: 'bid', alias: '', type: 'number' },
    { path: 'ask', alias: '', type: 'number' },
  ],
};

// ─── DataSource config ───────────────────────────────────────

export interface QuotesDataSourceOptions extends DataSourceJsonData {
  url?: string;
}
