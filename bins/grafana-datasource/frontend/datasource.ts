import { DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { QuotesQuery, QuotesDataSourceOptions, FieldSuggestion } from './types';

export class QuotesDataSource extends DataSourceWithBackend<QuotesQuery, QuotesDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<QuotesDataSourceOptions>) {
    super(instanceSettings);
  }

  applyTemplateVariables(query: QuotesQuery, scopedVars: ScopedVars): QuotesQuery {
    const templateSrv = getTemplateSrv();
    return {
      ...query,
      topic: templateSrv.replace(query.topic, scopedVars),
      key: templateSrv.replace(query.key, scopedVars),
      fieldConfigs: (query.fieldConfigs ?? []).map((fc) => ({
        ...fc,
        path: templateSrv.replace(fc.path, scopedVars),
        alias: fc.alias ? templateSrv.replace(fc.alias, scopedVars) : '',
      })),
      fields: undefined,
    };
  }

  async getFields(topic: string, key?: string): Promise<FieldSuggestion[]> {
    const params: Record<string, string> = { topic };
    if (key) {
      params.key = key;
    }
    try {
      return await this.getResource('fields', params);
    } catch {
      return [];
    }
  }
}
