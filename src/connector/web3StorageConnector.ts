import { extParam } from "../util/utils";
import { ResultType } from "mind-lake-sdk/dist/types";
import Result from "mind-lake-sdk/dist/util/result";
import JSZip from "jszip";
import axios from "axios";
import { MindLakeConnector } from "./mindlakeConnector";
import { MindLake } from "mind-lake-sdk";
import { Connector } from "./connector";

let zip = new JSZip();

const dataFileName = "datapack.csv";
const metaDataName = "datapack.meta.json";
const SAVE_URL = "https://dweb.link/ipfs/";

export class Web3StorageConnector implements Connector {
  private token: string | undefined;

  constructor(token?: string) {
    if (!token) {
      this.token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweEQ2N2UxQ2NGNjQ0OGMzZTdBQzg1RjEwQjZmMDZiMTJCMzlBNWNjNkEiLCJpc3MiOiJ3ZWIzLXN0b3JhZ2UiLCJpYXQiOjE2OTIyNjM4Nzc0MzAsIm5hbWUiOiJtaW5kLWxha2UtdGVzdCJ9.kEWB5K2Y4AGHdHGd5I9NtElrr_t4mkPTBmlJfOLC8j4";
    } else {
      this.token = token;
    }
  }

  public async loadFrom(
    fileName: string,
    identifier: string,
    mindLakeConnector: MindLakeConnector
  ): Promise<File[]> {
    return this.loadFromWeb3Storage(fileName, identifier);
  }

  public async saveTo(
    dataString: string,
    metaDataString: string,
    mindLake: MindLake,
    mindLakeConnector: MindLakeConnector,
    param?: extParam
  ): Promise<ResultType> {
    return this.saveToWeb3StorageByString(
      dataString,
      metaDataString,
      mindLake,
      mindLakeConnector
    );
  }

  public async saveToWeb3StorageByString(
    dataString: string,
    metaDataString: string,
    mindLake: MindLake,
    mindLakeConnector: MindLakeConnector
  ): Promise<ResultType> {
    try {
      if (!this.token) {
        return Result.fail({
          code: 60011,
          message: "saveToWeb3StorageByString error, token is empty",
        });
      }
      zip.file(dataFileName, dataString);
      zip.file(metaDataName, metaDataString);
      const zipFileName = "myFiles";
      const zipContent = await zip.generateAsync({ type: "blob" });

      let formdata = new FormData();
      formdata.append("file", zipContent);
      const headers: { [key: string]: string } = {};
      headers["Authorization"] = "Bearer " + this.token;

      const response = await axios.post(
        `https://api.web3.storage/upload`,
        formdata,
        { headers }
      );
      if (response.status === 200) {
        console.log("File uploaded successfully:", response.data);
        await mindLakeConnector.saveDataPackHistory2MindLake(
          response.data.cid,
          mindLake,
          "upload",
          "web3storage"
        );
        return Result.success({ hashId: response.data.cid });
      } else {
        console.error("File upload failed:", response.data);
        return Result.fail({
          code: 60012,
          message: "saveToWeb3StorageByString data failed",
        });
      }
    } catch (e) {
      console.error("Exception:", e);
      return Result.fail({
        code: 60012,
        message: "saveToWeb3StorageByString data failed",
      });
    }
  }

  public async loadFromWeb3Storage(
    fileName: string,
    cid: string
  ): Promise<File[]> {
    let file = undefined;
    var files = [];
    try {
      const dataUrl = SAVE_URL + cid;
      const res = await fetch(dataUrl);
      const blob = await res.blob();
      file = new File([blob], fileName, { type: blob.type });
      files.push(file);
      return files;
    } catch (error) {
      console.error("loadFromWeb3Storage", error);
      // @ts-ignore
      return files;
    }
  }
}
