import { genAPI, getTokenTagByEver } from "arseeding-js";
import { extParam, Utils } from "../util/utils";
import { ResultType } from "mind-lake-sdk/dist/types";
import Result from "mind-lake-sdk/dist/util/result";
import { Buffer } from "buffer";
import { MindLakeConnector } from "./mindlakeConnector";
import { MindLake } from "mind-lake-sdk";
import { MindDataConstants } from "../util/constant";
import { Connector } from "./connector";

const CSV_SAVE_URL = "https://arseed.web3infura.io/";
const META_SAVE_URL = "https://arseed.web3infura.io/bundle/tx/";

export class ArweaveConnector implements Connector {
  public async loadFrom(
    fileName: string,
    identifier: string,
    mindLakeConnector: MindLakeConnector
  ): Promise<File[]> {
    return this.loadFromArweave(fileName, identifier);
  }

  public async saveTo(
    dataString: string,
    metaDataString: string,
    mindLake: MindLake,
    mindLakeConnector: MindLakeConnector,
    param?: extParam
  ): Promise<ResultType> {
    return this.saveToArweaveByString(
      dataString,
      metaDataString,
      mindLake,
      mindLakeConnector,
      param?.symbol
    );
  }

  public async saveToArweaveByString(
    dataString: string,
    metaDataString: string,
    mindLake: MindLake,
    mindLakeConnector: MindLakeConnector,
    symbol?: string
  ): Promise<ResultType> {
    try {
      var csvBuffer = Buffer.from(dataString);
      var metaBuffer = Buffer.from(metaDataString);
      const instance = await genAPI(window.ethereum);
      const arseedUrl = "https://arseed.web3infura.io";
      if (!symbol) {
        symbol = "ACNH";
      }
      const tokenTags = await getTokenTagByEver(symbol);
      const payCurrencyTag = tokenTags[0];
      const csvTags = {
        tags: [{ name: "Content-Type", value: "text/csv" }],
      };
      csvTags.tags.push({ name: "App-Name", value: "Mind-DataPack" });
      csvTags.tags.push({
        name: "MindDataPackMetaData",
        value: metaDataString,
      });
      const res = await instance.sendAndPay(
        arseedUrl,
        csvBuffer,
        payCurrencyTag,
        csvTags
      );
      const hashId = res.order.itemId;
      const tagId = res.order.tag;
      await mindLakeConnector.saveDataPackHistory2MindLake(
        hashId,
        mindLake,
        "upload",
        "arweave"
      );
      return Result.success({ hashId, tagId });
    } catch (e) {
      console.error("Exception:", e);
      return Result.fail({ code: 60012, message: "Encrypt data failed" });
    }
  }

  public async loadFromArweave(
    fileName: string,
    hashId: string
  ): Promise<File[]> {
    var files = [];
    try {
      const dataUrl = CSV_SAVE_URL + hashId;
      const res = await fetch(dataUrl);
      const blob = await res.blob();
      const dataFileName = fileName + MindDataConstants.DATA_FILE_NAME_EXT;
      const dataFile = new File([blob], dataFileName, { type: blob.type });
      files.push(dataFile);
      const dataMetaUrl = META_SAVE_URL + hashId;
      const metaRes = await fetch(dataMetaUrl);
      const metaBlob = await metaRes.blob();
      const metaDataFileName = fileName + MindDataConstants.META_DATA_EXT;
      const dataMetaFile = new File([metaBlob], metaDataFileName, {
        type: blob.type,
      });
      files.push(dataMetaFile);
      return files;
    } catch (error) {
      console.error("loadFromArweave error", error);
      return files;
    }
  }
}
