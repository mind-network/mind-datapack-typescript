import {DataType, MindLake} from "mind-lake-sdk"
import {ResultType} from "mind-lake-sdk/dist/types/index"
import Result from "mind-lake-sdk/dist/util/result";
import {Column, extParam, MetaColumn, MetaDataParam, MindLakeColumn, Utils} from "../util/utils";
import {CipherHelper} from "mind-lake-sdk/dist/util/cipher";
import {Buffer} from "buffer";
import {encrypt} from '@metamask/eth-sig-util';
import {Web3Provider} from '@ethersproject/providers';
import {createHash} from 'crypto';
import {RequestArguments} from "mind-lake-sdk/dist/util/web3";
import {Connector} from "./connector";
import {MindLakeConnector} from "./mindlakeConnector";
import {MindDataConstants} from "../util/constant";


const utils = new Utils();

export interface Web3WithWalletProvider extends Web3Provider {
    request(args: RequestArguments): Promise<unknown>;
    selectedAddress: string;
}

export class MindLocalFileConnector implements Connector {

    private readonly provider: Web3WithWalletProvider;

    private publicKey!: string;

    private account!: string;

    constructor() {
        this.provider = window.ethereum;
    }

    public async loadFrom(fileName: string, identifier: string, mindLakeConnector: MindLakeConnector): Promise<File[]> {
        return Promise.resolve([]);
    }

    public async saveTo(dataString: string, metaDataString: string, mindLake: MindLake, mindLakeConnector: MindLakeConnector, param?: extParam): Promise<ResultType> {
        try {
            let fileName = undefined;
            if (!param || !param.fileName) {
                fileName = "datapack";
            } else {
                fileName = param.fileName;
            }
            if(!mindLake) {
                return Result.fail({code:60001, message:'user not login'});
            }
            const dataFileName = fileName + MindDataConstants.DATA_FILE_NAME_EXT;
            const metaDataName = fileName + MindDataConstants.META_DATA_EXT;
            const csvFile = utils.stringToFile(dataString, dataFileName, "text/plain");
            const metaDataFile = utils.stringToFile(metaDataString, metaDataName, "text/plain");
            return Result.success({csvFile: csvFile, metaDataFile: metaDataFile});
        } catch (e) {
            console.error("saveToLocalFile error", e);
            return Result.fail({code:60002, message:'saveToLocalFile error.'});
        }
    }


    public encrypt(data: any, columnMeta : Column): ResultType {
        try {
            data = Utils.encodeDataByType(data, columnMeta.type);
            const checkCode = this.genCheckCode(data, 1);
            const dataToEnc = Buffer.concat([data, checkCode]);
            const iv = CipherHelper.randomBytes(16);
            if (columnMeta.dataKey == undefined) {
                return Result.fail({code:60012, message:'dataKey is null'});
            }
            const encryptedData = CipherHelper.aesEncrypt(columnMeta.dataKey, iv, dataToEnc);
            const buf = Buffer.concat([iv, encryptedData]);
            const checkCode2 = this.genCheckCode(buf, 1);

            const result = Buffer.concat([checkCode2, buf]);
            const resultHex = '\\x' + result.toString('hex');
            return Result.success(resultHex);
        } catch (e) {
            console.error('Exception:', e);
            return Result.fail({code:60012, message:'Encrypt data failed'});
        }
    }

    public decrypt(cipher: string, columnMeta: Column): ResultType {
        try {
            cipher = cipher.slice(2);
            let cipherBuffer = Buffer.from(cipher, 'hex');
            const iv = cipherBuffer.slice(1, 17);
            if (columnMeta.dataKey == undefined) {
                return Result.fail('check code not match');
            }
            cipherBuffer = cipherBuffer.slice(17);
            const data = CipherHelper.aesDecrypt(columnMeta.dataKey, iv, cipherBuffer);
            const returnData = data.subarray(0, -1);
            const checkCode = data.slice(-1);
            const calculatedCheckCode = this.genCheckCode(returnData, 1);
            if (checkCode[0] !=calculatedCheckCode[0]) {
                return Result.fail({ code : 60013, message : 'check code not match'});
            }
            if (columnMeta.type == undefined) {
                return Result.fail({ code : 60013, message : 'decrypt data failed'});
            }
            const decodedData =  Utils.decodeDataByType(Uint8Array.from(returnData), columnMeta.type);
            return Result.success(decodedData);
        } catch (error) {
            console.error("Exception:", error);
            return Result.fail({ code : 60013, message : 'decrypt data failed'});
        }
    }

    /**
     * encrpt
     * @param queryResult
     * @param columns
     */
    public async encryptWithColumnArray(queryResult : ResultType, columns : Array<Column>) : Promise<ResultType> {
        try {
            const newData = new Array<Array<string>>();
            for (const row of queryResult.result.data) {
                const newDataColumn = new Array<string>();
                for (const idx in row) {
                    const temp = row[idx];
                    let tempData = null;
                    let column = columns[idx];
                    if (column.encrypt) {
                        const result = this.encrypt(temp, column);
                        tempData = result.result;
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
            console.error(error);
            return Result.fail({ code : 60013, message : 'decrypt data failed'});
        }
    }

    /**
     * encrpt
     * @param queryResult
     * @param column
     */
    public async encryptColumn(queryResult : ResultType, column : Column) : Promise<ResultType> {
        try {
            const newData = new Array<Array<string>>();
            for (const row of queryResult.result.data) {
                const newDataColumn = new Array<string>();
                for (const idx in row) {
                    const temp = row[idx];
                    let tempData = null;
                    if (column.encrypt) {
                        const result = this.encrypt(temp, column);
                        tempData = result.result;
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
            console.error(error);
            return Result.fail({ code : 60013, message : 'decrypt data failed'});
        }
    }

    public async decryptColumnData(data : Array<Array<string>>, columns : Array<Column>) : Promise<Array<Array<string>>> {
        try {
            const newData = new Array<Array<string>>();
            for (const row of data) {
                const newDataColumn = new Array<string>();
                for (const idx in row) {
                    const temp = row[idx];
                    let tempData = null;
                    let column = columns[idx];
                    if (column.encrypt) {
                        const result = this.decrypt(temp, column);
                        tempData = result.result;
                    } else {
                        tempData = row[idx];
                    }
                    newDataColumn.push(tempData);
                }
                newData.push(newDataColumn);
            }
            return newData;
        } catch (error) {
            console.error(error);
            return new Array<Array<string>>();
        }
    }

    /**
     * decrypt with column array
     *
     * @param queryResult
     * @param columns
     */
    public async decryptWithColumnArray(queryResult : ResultType, columns : Array<Column>) : Promise<ResultType> {
        try {
            const newData = new Array<Array<string>>();
            for (const row of queryResult.result.data) {
                const newDataColumn = new Array<string>();
                for (const idx in row) {
                    const temp = row[idx];
                    let tempData = null;
                    let column = columns[idx];
                    if (column.encrypt) {
                        const result = this.decrypt(temp, column);
                        tempData = result.result;
                        console.log("tempData", tempData);
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
            console.error(error);
            return Result.fail({ code : 60013, message : 'decrypt data failed'});
        }
    }

    public async decryptColumn(queryResult : ResultType, column : Column) : Promise<ResultType> {
        try {
            const newData = new Array<Array<string>>();
            for (const row of queryResult.result.data) {
                const newDataColumn = new Array<string>();
                for (const idx in row) {
                    const temp = row[idx];
                    let tempData = null;
                    if (column.encrypt) {
                        const result = this.decrypt(temp, column);
                        tempData = result.result;
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
            console.error(error);
            return Result.fail({ code : 60013, message : 'decrypt data failed'});
        }
    }

    public genRandomColumn(): Column {
        const column = new Column("content", DataType.text, true);
        return column;
    }

    public genMetaDataColumn(mindLakeColumns : Array<MindLakeColumn>) : Array<Column> {
        let columnList = new Array<Column>();
        for (let i = 0; i < mindLakeColumns.length; i++) {
            let mindLakeColumn = mindLakeColumns[i];
            let dataType = Utils.convertNumberToType(mindLakeColumn.type);
            const column = new Column(mindLakeColumn.name, dataType, mindLakeColumn.enc);
            columnList.push(column);
        }
        return columnList;
    }

    public async buildMetaDataString(columns : Array<Column>) : Promise<string> {
        const sha256Hash = createHash('sha256');
        sha256Hash.update('input');
        const sha256HashHex = sha256Hash.digest('hex');
        const ignoreEncrypt = false;
        const columnList = new Array<MetaColumn>;
        for (const column of columns) {
            const metaColumn = new MetaColumn();
            metaColumn.ColumnName = column.columnName;
            metaColumn.DataType = Utils.convertTypeToString(column.type);
            metaColumn.Encrypt = column.encrypt;
            if (column.encrypt && column.dataKey) {
                const dataKeyString = column.dataKey.toString('hex');
                // metaColumn.DataKeyCipher = column.dataKey;
                const bufferResult = await this.encryptDek(dataKeyString);
                // @ts-ignore
                metaColumn.DataKeyCipher = bufferResult.toString('base64');
            }
            columnList.push(metaColumn);
        }
        const metaDataParam : MetaDataParam = {
            FileName: "datapack.meta.json",
            FileHash: sha256HashHex,
            IgnoreEncrypt: ignoreEncrypt,
            Column:columnList,
            Version: "1.0"
        }
        return JSON.stringify(metaDataParam);
    }

    private genCheckCode(encodeData: Uint8Array, resultSize: number) : Buffer {
        let tmpCode = new Uint8Array(resultSize);
        for (let i = 0; i < encodeData.length; i++) {
            let n = i % resultSize;
            tmpCode[n] ^= encodeData[i];
        }
        return Buffer.from(tmpCode);
    }

    /**
     * get wallet publicKey
     * @private
     */
    private async getEncryptionPublicKey(): Promise<string | undefined> {
        await this.getWalletAccount();
        if (this.provider) {
            if (this.publicKey) {
                return this.publicKey;
            }
            const keyB64: string = (await this.provider.request({
                method: 'eth_getEncryptionPublicKey',
                params: [this.account],
            })) as string;
            this.publicKey = keyB64;
            return this.publicKey;
        }
    }

    /**
     * get walletAddress
     */
    public async getWalletAccount(): Promise<string> {
        if (!this.account || !MindLake.isConnected) {
            const accounts = (await this.provider.request({
                method: 'eth_requestAccounts',
            })) as string[];
            if (!accounts.length) {
                throw new Error('No accounts returned');
            }
            this.account = accounts[0];
        }
        return this.account;
    }

    /**
     * encrypt dek
     * @param dek
     * @private
     */
    private async encryptDek(dek: string): Promise<Buffer | undefined> {
        const pubKey = await this.getEncryptionPublicKey();
        if (pubKey) {
            const enc = encrypt({
                publicKey: pubKey,
                data: Buffer.from(dek, 'hex').toString('base64'),
                version: 'x25519-xsalsa20-poly1305',
            });
            const buf = Buffer.concat([
                Buffer.from(enc.ephemPublicKey, 'base64'),
                Buffer.from(enc.nonce, 'base64'),
                Buffer.from(enc.ciphertext, 'base64'),
            ]);
            return buf;
        }
    }
}