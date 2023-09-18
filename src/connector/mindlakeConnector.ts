import {DataType, MindLake} from "mind-lake-sdk";
import {ResultType} from "mind-lake-sdk/dist/types/index";
import Result from "mind-lake-sdk/dist/util/result";
import {datapack_history, datapack_history_columns, MindLakeColumn, Utils,} from "../util/utils";
import dayjs from "dayjs";
import {ColumnType} from "mind-lake-sdk/dist/types";
import {MindLocalFileConnector} from "./mindLocalFileConnector";

export class MindLakeConnector {
  private mindLocalFileConnector: MindLocalFileConnector;

  constructor(mindLocalFileConnector: MindLocalFileConnector) {
    this.mindLocalFileConnector = mindLocalFileConnector;
  }

  public async loadFromMindByQuery(
    executeSql: string,
    mindLake: MindLake,
    encrypt?: boolean
  ): Promise<ResultType> {
    try {
      if (encrypt === undefined) {
        encrypt = true;
      }
      if (!mindLake) {
        return Result.fail({ code: 60001, message: "user not login" });
      }
      // 1.get sql execute result
      const result = await this.loadFromMindAndDecrypt(executeSql, mindLake);
      // 2.random dek
      const columns = await this.mindLocalFileConnector.genMetaDataColumn(
        result.result.columnRefs
      );
      // 3.get csv string
      let csvString = undefined;
      if (!encrypt) {
        csvString = await this.getCsvStringByResultType(result);
      } else {
        const encryptResult =
          await this.mindLocalFileConnector.encryptWithColumnArray(
            result,
            columns
          );
        csvString = await this.getCsvStringByResultType(encryptResult);
      }
      const metaString = await this.mindLocalFileConnector.buildMetaDataString(columns);
      return Result.success({ csvString, metaString });
    } catch (e) {
      console.log("loadFromMindByQuery error", e);
      return Result.fail({
        code: 60002,
        message: "user load from mind error.",
      });
    }
  }

  private convertColumnRef(
    columnRefStringList: Array<any>
  ): Array<MindLakeColumn> {
    const columnList = new Array<MindLakeColumn>();
    try {
      for (const row of columnRefStringList) {
        const jsonString = JSON.stringify(row);
        const jsonObject = JSON.parse(jsonString) as MindLakeColumn;
        columnList.push(jsonObject);
      }
      return columnList;
    } catch (error) {
      console.error("Exception:", error);
      return columnList;
    }
  }

  public async loadFromMindAndDecrypt(
    sqlStatement: string,
    mindLake: MindLake
  ): Promise<ResultType> {
    try {
      const dataLake = mindLake.dataLake;
        const queryResult = await dataLake.queryForDataAndMeta(sqlStatement);
      console.log(queryResult);
      if (queryResult.code != 0) {
        throw new Error(queryResult.message);
      }
      const crypto = mindLake.crypto;
      const newData = new Array<Array<string>>();
      for (const row of queryResult.result.data) {
        const newDataColumn = new Array<string>();
        for (const idx in row) {
          const temp = row[idx];
          let tempData = null;
          if (temp != null && temp.startsWith("\\x")) {
            const decryptData = await crypto.decrypt(row[idx]);
            if (decryptData.code != 0) {
                return Result.fail({ code: decryptData.code, message: decryptData.message });
            }
            tempData = decryptData.result;
          } else {
            tempData = row[idx];
          }
          newDataColumn.push(tempData);
        }
        newData.push(newDataColumn);
      }
      const columnList = this.convertColumnRef(queryResult.result.columnRefs);
      queryResult.result.data = newData;
      queryResult.result.columnList = columnList;
      return queryResult;
    } catch (error) {
      console.error("Exception:", error);
      return Result.fail({
        code: 60013,
        message: "loadFromMindAndDecrypt failed",
      });
    }
  }
  // private

  public async executeSqlWithDecrypt(
    sqlStatement: string,
    mindLake: MindLake
  ): Promise<ResultType> {
    try {
      const dataLake = mindLake.dataLake;
        const queryResult = await dataLake.query(sqlStatement);
      if (queryResult.code != 0) {
        throw new Error(queryResult.message);
      }
      const columnList = queryResult.result.columnList;
      const crypto = mindLake.crypto;
      const newData = new Array<Array<string>>();
      for (const row of queryResult.result.data) {
        const newDataColumn = new Array<string>();
        for (const idx in row) {
          const temp = row[idx];
          let tempData = null;
          if (temp != null && temp.startsWith("\\x")) {
            const decryptData = await crypto.decrypt(row[idx]);
            if (decryptData.code != 0) {
                return Result.fail({ code: decryptData.code, message: decryptData.message });
            }
            tempData = decryptData.result;
          } else {
            tempData = row[idx];
          }
          newDataColumn.push(tempData);
        }
        newData.push(newDataColumn);
      }
      queryResult.result.data = newData;
      return queryResult;
    } catch (error) {
      console.error("Exception:", error);
      return Result.fail({ code: 60013, message: "executeSql failed" });
    }
  }

  public async getCsvStringByResultType(result: ResultType): Promise<string> {
    const csvRows: Array<string> = [];
    const queryResult = result;
    const columnList = new Array<string>();
    for (const column of queryResult.result.columnList) {
      columnList.push(column.name);
    }
    const newData = new Array<Array<string>>();
    newData.push(columnList);
    for (const row of queryResult.result.data) {
      newData.push(row);
    }
    for (const row of newData) {
      const csvRow: string = row.join(",");
      csvRows.push(csvRow);
    }
    return csvRows.join("\n");
  }

  public async initTable(mindLake: MindLake): Promise<boolean> {
    try {
      const res1 = await mindLake.dataLake.query(
        `SELECT 1 FROM ${datapack_history.tableName}`
      );
      //if table exist
      if (res1.code === 0) {
        return true;
      }
      const result = await mindLake.dataLake.createTable(
        datapack_history.tableName,
        datapack_history_columns,
        datapack_history.pkColumns
      );
      return result.code === 0;
    } catch (error) {
      console.error("Exception:", error);
      return false;
    }
  }

  public async insertDataPackHistory(
    mindLake: MindLake,
    hashId: string,
    type: string,
    source: string
  ): Promise<boolean> {
    try {
      const res1 = await mindLake.dataLake.query(
        `SELECT 1 FROM ${datapack_history.tableName}`
      );
      if (res1.code != 0) {
        console.error("datapack_history not exist", hashId, type, source);
        return false;
      }
      const url = Utils.buildDownloadUrl(hashId, source);
      const update_timestamp = dayjs().format("YYYY-MM-DD HH:mm:ss");
      const result = await mindLake.dataLake.query(`INSERT INTO "${
        datapack_history.tableName
      }"
                (${datapack_history_columns
                  .map((c) => c.columnName)
                  .join(",")}) 
                VALUES ('${hashId}', '${type}', '${source}', '${url}', '${update_timestamp}', '${update_timestamp}')`);
      return result.code === 0;
    } catch (error) {
      console.error("Exception:", error);
      return false;
    }
  }

  public async saveDataPackHistory2MindLake(
    hashId: string,
    mindLake: MindLake,
    type: string,
    source: string
  ): Promise<boolean> {
    try {
      await this.initTable(mindLake);
      return await this.insertDataPackHistory(mindLake, hashId, type, source);
    } catch (error) {
      console.error("Exception:", error);
      return false;
    }
  }

  public async getDataPackHistory(mindLake: MindLake): Promise<ResultType> {
    try {
      let result = await mindLake.dataLake.query(
        `SELECT * FROM "${datapack_history.tableName}" order by create_timestamp desc`
      );
      if (result.code === 40017) {
        const initTableResult = this.initTable(mindLake);
        return Result.success(initTableResult);
      } else if (result.code != 0) {
        console.error("getDataPackHistory error:", result);
        return Result.fail({
          code: 60014,
          message: "getDataPackHistory failed",
        });
      }
      const data = result.result;
      return Result.success(data);
    } catch (error) {
      console.error("getDataPackHistory error", error);
      return Result.fail({ code: 60014, message: "executeSql failed" });
    }
  }

  public async saveToMindLake(
    mindLake: MindLake,
    tableName: string,
    columns: Array<ColumnType>,
    data: Array<Array<string>>,
    pkColumns?: Array<string>
  ): Promise<ResultType> {
    try {
      const result = await mindLake.dataLake.createTable(
        tableName,
        columns,
        pkColumns
      );
      if (!result || result.code != 0) {
        console.error("createTable error", tableName);
        return Result.fail({
          code: 60015,
          message: "saveToMindLake failed create table error",
        });
      }
      for (const row of data) {
        let insertValue = "";
        for (let index = 0; index < row.length; index++) {
          const cell = row[index];
          const column = columns[index];

          if (column.encrypt) {
            const encryptResult = await mindLake.crypto.encrypt(
              cell,
              `${tableName}.${column.columnName}`
            );
            if (!encryptResult) {
              return encryptResult;
            }
            insertValue += `'${encryptResult.result.data}',`;
          } else {
            if (
              column.type === DataType.text ||
              column.type === DataType.timestamp
            ) {
              insertValue += `'${cell}',`;
            } else {
              insertValue += `${cell},`;
            }
          }
        }
        const insertSql = `INSERT INTO "${tableName}" VALUES (${insertValue.slice(
          0,
          -1
        )})`;
        const insertResult = await mindLake.dataLake.query(insertSql);
        if (!insertResult || insertResult.code != 0) {
          return Result.fail({ code: 60015, message: "saveToMindLake failed" });
        }
      }
      return Result.success("success");
    } catch (error) {
      console.error("saveToMindLake error:", error);
      return Result.fail({ code: 60015, message: "saveToMindLake failed" });
    }
  }
}
