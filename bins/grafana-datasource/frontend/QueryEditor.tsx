import React from 'react';
import { InlineField, InlineFieldRow, Input, Select, Button, IconButton } from '@grafana/ui';
import { SelectableValue, QueryEditorProps } from '@grafana/data';
import { QuotesDataSource } from './datasource';
import {
  QuotesQuery,
  QuotesDataSourceOptions,
  FieldConfig,
  DataFormat,
  FieldValueType,
  ConnectionMode,
  FieldSuggestion,
} from './types';

type Props = QueryEditorProps<QuotesDataSource, QuotesQuery, QuotesDataSourceOptions>;

const MODE_OPTIONS: Array<SelectableValue<ConnectionMode>> = [
  { label: 'HTTP API', value: 'http' },
  { label: 'Streaming', value: 'tcp', description: 'Real-time streaming via WebSocket' },
];

const FORMAT_OPTIONS: Array<SelectableValue<DataFormat>> = [
  { label: 'JSON', value: 'json' },
  { label: 'CSV', value: 'csv' },
  { label: 'Protobuf', value: 'protobuf' },
  { label: 'Avro', value: 'avro' },
];

const TYPE_OPTIONS: Array<SelectableValue<FieldValueType>> = [
  { label: 'Number', value: 'number' },
  { label: 'String', value: 'string' },
  { label: 'Boolean', value: 'boolean' },
  { label: 'Time', value: 'time' },
];

/** Migrate legacy comma-separated fields string to FieldConfig array. */
function migrateFields(fields?: string): FieldConfig[] {
  if (!fields) {
    return [];
  }
  return fields
    .split(',')
    .map((f) => f.trim())
    .filter(Boolean)
    .map((f) => ({ path: f, alias: '', type: 'number' as FieldValueType }));
}

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {
  // Resolve fieldConfigs with legacy migration
  const fieldConfigs: FieldConfig[] =
    query.fieldConfigs && query.fieldConfigs.length > 0
      ? query.fieldConfigs
      : migrateFields(query.fields);

  const [fieldSuggestions, setFieldSuggestions] = React.useState<Array<SelectableValue<string>>>([]);

  // Fetch field suggestions when topic or key changes
  React.useEffect(() => {
    const topic = query.topic;
    if (!topic) {
      setFieldSuggestions([]);
      return;
    }
    let cancelled = false;
    datasource.getFields(topic, query.key).then((suggestions: FieldSuggestion[]) => {
      if (cancelled) {
        return;
      }
      setFieldSuggestions(
        suggestions.map((s) => ({
          label: s.path,
          value: s.path,
          description: s.type,
        }))
      );
    });
    return () => {
      cancelled = true;
    };
  }, [datasource, query.topic, query.key]);

  const onBlur = () => onRunQuery();

  const updateQuery = (patch: Partial<QuotesQuery>) => {
    onChange({ ...query, ...patch });
  };

  const onFieldConfigChange = (index: number, updated: FieldConfig) => {
    const newConfigs = [...fieldConfigs];
    newConfigs[index] = updated;
    updateQuery({ fieldConfigs: newConfigs, fields: undefined });
  };

  const onAddField = () => {
    const newConfigs = [...fieldConfigs, { path: '', alias: '', type: 'number' as FieldValueType }];
    updateQuery({ fieldConfigs: newConfigs, fields: undefined });
  };

  const onRemoveField = (index: number) => {
    const newConfigs = fieldConfigs.filter((_, i) => i !== index);
    updateQuery({ fieldConfigs: newConfigs, fields: undefined });
    onRunQuery();
  };

  return (
    <div className="gf-form-group">
      {/* Row 1: Mode, Format, Topic */}
      <InlineFieldRow>
        <InlineField label="Mode" labelWidth={10} tooltip="Connection mode">
          <Select
            options={MODE_OPTIONS}
            value={query.connectionMode || 'http'}
            onChange={(v) => {
              updateQuery({ connectionMode: v.value! });
              onRunQuery();
            }}
            width={16}
          />
        </InlineField>
        <InlineField label="Format" labelWidth={10} tooltip="Message format in the topic">
          <Select
            options={FORMAT_OPTIONS}
            value={query.format || 'json'}
            onChange={(v) => {
              updateQuery({ format: v.value! });
              onRunQuery();
            }}
            width={16}
          />
        </InlineField>
        <InlineField label="Topic" labelWidth={10} tooltip="Topic name (e.g. quotes.raw, ohlc.1m). Supports $variables.">
          <Input
            value={query.topic || ''}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateQuery({ topic: e.target.value })}
            onBlur={onBlur}
            placeholder="quotes.raw"
            width={32}
          />
        </InlineField>
      </InlineFieldRow>

      {/* Row 2: Key */}
      <InlineFieldRow>
        <InlineField label="Key" labelWidth={10} tooltip="Partition key filter (e.g. EURUSD). Supports $variables. Leave empty for all.">
          <Input
            value={query.key || ''}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateQuery({ key: e.target.value })}
            onBlur={onBlur}
            placeholder="EURUSD"
            width={32}
          />
        </InlineField>
      </InlineFieldRow>

      {/* Section: Field configurations */}
      <div style={{ marginTop: 8 }}>
        {fieldConfigs.map((fc, i) => (
          <InlineFieldRow key={i}>
            <InlineField label={i === 0 ? 'Fields' : ''} labelWidth={10} tooltip="Dot-notation path to the value in record.data. Select from discovered fields or type a custom path.">
              <Select
                options={fieldSuggestions}
                value={fc.path ? { label: fc.path, value: fc.path } : undefined}
                onChange={(v) => {
                  const suggestion = (v as SelectableValue<string>);
                  const newType = suggestion.description as FieldValueType || fc.type;
                  onFieldConfigChange(i, { ...fc, path: suggestion.value || '', type: newType });
                  onRunQuery();
                }}
                onBlur={onBlur}
                allowCustomValue
                placeholder="field.path"
                width={24}
                isClearable
              />
            </InlineField>
            <InlineField label="as" labelWidth={4} tooltip="Display alias (optional)">
              <Input
                value={fc.alias}
                placeholder="(same)"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  onFieldConfigChange(i, { ...fc, alias: e.target.value })
                }
                onBlur={onBlur}
                width={16}
              />
            </InlineField>
            <InlineField label="" labelWidth={0}>
              <Select
                options={TYPE_OPTIONS}
                value={fc.type || 'number'}
                onChange={(v) => {
                  onFieldConfigChange(i, { ...fc, type: v.value! });
                  onRunQuery();
                }}
                width={14}
              />
            </InlineField>
            <IconButton name="trash-alt" onClick={() => onRemoveField(i)} tooltip="Remove field" />
          </InlineFieldRow>
        ))}
        <Button variant="secondary" size="sm" icon="plus" onClick={onAddField}>
          Add field
        </Button>
      </div>
    </div>
  );
}
