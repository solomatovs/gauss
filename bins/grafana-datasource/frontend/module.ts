import { DataSourcePlugin } from '@grafana/data';
import { QuotesDataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { QuotesQuery, QuotesDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<QuotesDataSource, QuotesQuery, QuotesDataSourceOptions>(
  QuotesDataSource
)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
