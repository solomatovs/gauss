import React from 'react';
import { InlineField, Input } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { QuotesDataSourceOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<QuotesDataSourceOptions>;

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const { jsonData } = options;

  const onUrlChange = (value: string) => {
    onOptionsChange({
      ...options,
      jsonData: { ...jsonData, url: value },
    });
  };

  return (
    <div className="gf-form-group">
      <div className="gf-form">
        <InlineField
          label="Server URL"
          labelWidth={14}
          tooltip="gauss-server HTTP API address (host:port)"
        >
          <Input
            value={jsonData.url || ''}
            onChange={(e) => onUrlChange(e.currentTarget.value)}
            placeholder="http://gauss-server:9200"
            width={40}
          />
        </InlineField>
      </div>
    </div>
  );
}
