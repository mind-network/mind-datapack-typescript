import { MindLake } from "mind-lake-sdk";
import { ResultType } from "mind-lake-sdk/dist/types";
import { MindLakeConnector } from "./connector/mindlakeConnector";
import { MindLocalFileConnector } from "./connector/mindLocalFileConnector";
import { extParam, Utils } from "./util/utils";
import { Connector } from "./connector/connector";
import Result from "mind-lake-sdk/dist/util/result";
import { MindDataConstants } from "./util/constant";

const utils = new Utils();

/**
 * dataPack index
 *
 * @author tanlee
 * @since 2023.9.4
 */
export class DataPack {
  private mindLakeConnector: MindLakeConnector;

  private mindLocalFileConnector: MindLocalFileConnector;

  constructor() {
    this.mindLocalFileConnector = new MindLocalFileConnector();
    this.mindLakeConnector = new MindLakeConnector(this.mindLocalFileConnector);
  }

  public getMindLakeConnector(): MindLakeConnector {
    return this.mindLakeConnector;
  }

  public getMindLocalFileConnector(): MindLocalFileConnector {
    return this.mindLocalFileConnector;
  }

  /**
   * load string by mind lake use execute sql
   *
   * @param executeSql executeSql
   * @param mindLake mindLake
   * @param encrypt encrypt
   */
  public async loadFromMindByQuery(
    executeSql: string,
    mindLake: MindLake,
    encrypt?: boolean
  ): Promise<ResultType> {
    return this.mindLakeConnector.loadFromMindByQuery(
      executeSql,
      mindLake,
      encrypt
    );
  }

  /**
   * load from connector
   *
   * @param fileName fileName
   * @param identifier identifier
   * @param connector connector
   */
  public async loadFrom(
    fileName: string,
    identifier: string,
    connector: Connector
  ): Promise<File[]> {
    return connector.loadFrom(fileName, identifier, this.mindLakeConnector);
  }

  /**
   * save to connector
   *
   * @param dataString dataString
   * @param metaDataString metaDataString
   * @param mindLake mindLake
   * @param connector connector
   * @param param param
   */
  public async saveTo(
    dataString: string,
    metaDataString: string,
    mindLake: MindLake,
    connector: Connector,
    param?: extParam
  ): Promise<ResultType> {
    return connector.saveTo(
      dataString,
      metaDataString,
      mindLake,
      this.mindLakeConnector,
      param
    );
  }

  public async executeSqlToLocalFile(
    executeSql: string,
    fileName: string,
    mindLake: MindLake,
    ignoreEncrypt: boolean
  ): Promise<ResultType> {
    try {
      if (!fileName) {
        fileName = "datapack";
      }
      if (!mindLake) {
        return Result.fail({ code: 60001, message: "user not login" });
      }

      // 1.get sql execute result
      const result = await this.mindLakeConnector.loadFromMindAndDecrypt(
        executeSql,
        mindLake
      );
      // 2.random dek
      const columns = await this.mindLocalFileConnector.genMetaDataColumn(
        result.result.columnRefs
      );
      // 3.get csv string
      let csvString = undefined;
      if (ignoreEncrypt) {
        csvString = await this.mindLakeConnector.getCsvStringByResultType(
          result
        );
      } else {
        const encryptResult =
          await this.mindLocalFileConnector.encryptWithColumnArray(
            result.result.data,
            columns
          );
        csvString = await this.mindLakeConnector.getCsvStringByResultType(
          encryptResult
        );
      }
      const metaString = JSON.stringify(columns);
      const dataFileName = fileName + MindDataConstants.DATA_FILE_NAME_EXT;
      const metaDataName = fileName + MindDataConstants.META_DATA_EXT;
      const csvFile = utils.stringToFile(csvString, dataFileName, "text/plain");
      const metaDataFile = utils.stringToFile(
        metaString,
        metaDataName,
        "text/plain"
      );
      return Result.success({ csvFile: csvFile, metaDataFile: metaDataFile });
    } catch (e) {
      console.log("saveToLocalFile error", e);
      return Result.fail({ code: 60002, message: "saveToLocalFile error." });
    }
  }
}
