import AbstractDriver from "@sqltools/base-driver";
import queries from "./queries";
import {
  IConnectionDriver,
  MConnectionExplorer,
  NSDatabase,
  ContextValue,
  Arg0,
  IQueryOptions,
} from "@sqltools/types";
import { v4 as generateId } from "uuid";
import { QueryResult } from "./types";
import { QueryParser } from "./parser";
import * as presto from "presto-client";

type DriverLib = presto.Client;
type DriverOptions = any;

export default class PrestoDriver
  extends AbstractDriver<DriverLib, DriverOptions>
  implements IConnectionDriver
{
  queries = queries;

  public async open(): Promise<presto.Client> {
    if (this.connection) {
      return this.connection;
    }

    const { server, catalog, schema, user, password, prestoOptions = {} } = this.credentials;
    
    // URL 형식 처리
    let hostUrl = server;
    
    // URL에 프로토콜이 포함되어 있지 않은 경우 기본값으로 http:// 추가
    if (!hostUrl.startsWith('http://') && !hostUrl.startsWith('https://')) {
      hostUrl = `http://${hostUrl}`;
    }

    // 호스트와 포트 분리
    let host = '';
    let port = 8080;
    
    try {
      const url = new URL(hostUrl);
      host = url.hostname;
      port = url.port ? parseInt(url.port, 10) : (url.protocol === 'https:' ? 443 : 8080);
    } catch (urlError) {
      return Promise.reject(urlError);
    }

    // SSL 옵션 설정
    let ssl = null;
    if (hostUrl.startsWith('https://')) {
      ssl = {
        rejectUnauthorized: false
      };
    }

    const clientOptions = {
      host,
      port,
      user,
      catalog,
      schema,
      source: 'sqltools-driver',
      basic_auth: password ? { user, password } : null,
      ssl,
      checkInterval: prestoOptions.checkInterval || 800,
      timezone: prestoOptions.timezone || 'Asia/Seoul'
    };

    const conn = new presto.Client(clientOptions);
    this.connection = Promise.resolve(conn);

    return this.connection;
  }

  public async close(): Promise<void> {
    if (!this.connection) return Promise.resolve();
    await this.connection;
    this.connection = null;
  }

  private buildResult = (
    query: string,
    queryResult: QueryResult,
    opts: IQueryOptions
  ): NSDatabase.IResult => {
    const columns = queryResult.columns ?? [];
    const rows = queryResult.rows ?? [];
    const msg = queryResult.error
      ? queryResult.error.message
      : `Successfully executed. ${rows.length} rows were affected.`;

    return {
      requestId: opts.requestId,
      resultId: generateId(),
      connId: this.getId(),
      cols: columns.map((col) => col.name),
      results: rows,
      messages: [this.prepareMessage(msg)],
      error: queryResult.error ? true : false,
      rawError: queryResult.error,
      query,
    };
  };

  private executeQuery(db: presto.Client, query: string): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const columns = [];
      const rows = [];

      db.execute({
        query,
        schema: this.credentials.schema,
        catalog: this.credentials.catalog,
        timezone: this.credentials.prestoOptions?.timezone,
        
        columns: (err, data) => {
          if (err) {
            reject({ error: err });
            return;
          }
          
          data.forEach(col => {
            columns.push({
              name: col.name,
              type: col.type
            });
          });
        },
        
        data: (err, data) => {
          if (err) {
            reject({ error: err });
            return;
          }
          
          if (data && columns.length > 0) {
            data.forEach(rowData => {
              const row = {};
              rowData.forEach((value, colIndex) => {
                if (colIndex < columns.length) {
                  row[columns[colIndex].name] = value;
                }
              });
              rows.push(row);
            });
          }
        },
        
        error: (err) => {
          reject({
            error: err instanceof Error ? err : new Error(String(err))
          });
        },
        
        success: () => {
          resolve({
            columns,
            rows
          });
        }
      });
    });
  }

  public query: typeof AbstractDriver["prototype"]["query"] = async (
    query: string,
    opt = {}
  ) => {
    const resultsAgg: NSDatabase.IResult[] = [];
    const db = await this.open();

    for (const q of QueryParser.statements(query)) {
      const iresult: NSDatabase.IResult = await this.executeQuery(db, q)
        .then((result) => this.buildResult(q, result, opt))
        .catch((error) => this.buildResult(q, { error: error }, opt));

      resultsAgg.push(iresult);
    }

    return resultsAgg;
  };

  public async testConnection() {
    await this.open();
    const testSelect = await this.query("SELECT 1", {});

    if (testSelect.length > 0 && testSelect[0].error) {
      const msg = testSelect[0].messages
        .map((m: { message: string; date: Date }) => m.message)
        .join("\n");

      return Promise.reject({ message: msg });
    }
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({
    item,
    parent,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.queryResults(
          queries.fetchSchemas({
            database: this.credentials.catalog,
          } as NSDatabase.IDatabase)
        );
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: "Tables",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.TABLE,
          },
          {
            label: "Views",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.VIEW,
          },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(
          queries.fetchColumns(item as NSDatabase.ITable)
        );
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({
    parent,
    item,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    switch (item.childType) {
      case ContextValue.TABLE:
        return this.queryResults(
          queries.fetchTables(parent as NSDatabase.ISchema)
        );
      case ContextValue.VIEW:
        return this.queryResults(
          queries.fetchViews(parent as NSDatabase.ISchema)
        );
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(
    itemType: ContextValue,
    search: string,
    _extraParams: any = {}
  ): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(queries.searchTables({ search }));
      case ContextValue.COLUMN:
        return this.queryResults(
          queries.searchColumns({ search, ..._extraParams })
        );
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver["getStaticCompletions"] =
    async () => {
      return {};
    };
}
