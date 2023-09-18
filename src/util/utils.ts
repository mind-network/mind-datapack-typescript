import {DataType, MindLake} from "mind-lake-sdk";
import {CipherHelper} from "mind-lake-sdk/dist/util/cipher";
import Decimal from 'decimal.js';
import {Buffer} from "buffer";
import {ResultType} from "mind-lake-sdk/dist/types";

/**
 * base column
 *
 * @author tanlee
 * @since 2023.8.10
 */
export interface MindLakeColumn {
    name: string;
    type: number;
    enc: boolean;
}

/**
 * connector ext param
 */
export type extParam = {
    symbol ?: string;
    token ?: string;
    projectId ?: string;
    projectSecret ?:string;
    fileName ?:string;
}

/**
 * metadata param
 */
export type MetaDataParam = {
    FileName ?: string;
    FileHash ?: string;
    IgnoreEncrypt ?: boolean;
    Column ?: Array<MetaColumn>;
    Version ?: string;
}

/**
 * metaColumn
 */
export class MetaColumn {
    public ColumnName ?: string;
    public DataType ?: string;
    public Encrypt ?: boolean;
    public DataKeyCipher ?: string;
}


/**
 * data pack upload or download history
 */
export const datapack_history = {
    tableName: 'datapack_history',
    columns: {
        // id: {
        //     columnName: 'id',
        //     type: MindLake.DataType.text,
        //     encrypt: false
        // },
        hashid: {
            columnName: 'hashid',
            type: MindLake.DataType.text,
            encrypt: false
        },
        type: {
            columnName: 'type',
            type: MindLake.DataType.text,
            encrypt: false
        },
        source: {
            columnName: 'source',
            type: MindLake.DataType.text,
            encrypt: false
        },
        download_url: {
            columnName: 'download_url',
            type: MindLake.DataType.text,
            encrypt: false
        },
        create_timestamp: {
            columnName: 'create_timestamp',
            type: MindLake.DataType.timestamp,
            encrypt: false
        },
        update_timestamp: {
            columnName: 'update_timestamp',
            type: MindLake.DataType.timestamp,
            encrypt: false
        }
    },
    pkColumns:['hashid']
};

// @ts-ignore
export const datapack_history_columns = Object.getOwnPropertyNames(datapack_history.columns).map(key => ({...datapack_history.columns[key]}));


export class Column {

    public dataKey : Buffer | undefined;
    public columnName : string | undefined;
    public type : DataType | undefined;
    public encrypt : boolean | undefined;
    public dataKeyString : string | undefined;

    constructor(columnName?: string, dataType?: DataType, encrypt?: boolean, dataKey?: Buffer | undefined) {
        this.columnName = columnName;
        this.type = dataType;
        this.encrypt = encrypt;
        if (encrypt) {
            if (dataKey) {
                this.dataKey = dataKey;
            } else {
                this.dataKey = CipherHelper.randomBytes(16);
            }
        } else {
            this.dataKey = undefined;
        }
        if (this.dataKey != undefined) {
            this.dataKeyString = this.dataKey.toString("hex");
        }
    }

}

export class Utils {

    public static convertTypeToNumber(dataType: DataType) : number {
        switch (dataType) {
            case DataType.int4:
                return 1;
            case DataType.int8:
                return 2;
            case DataType.float4:
                return 3;
            case DataType.float8:
                return 4;
            case DataType.decimal:
                return 5;
            case DataType.text:
                return 6;
            case DataType.timestamp:
                return 7;
            default:
                throw new Error('Unsupported encryption type');
        }
    }

    public static convertTypeToString(dataType: DataType | undefined) : string {
        switch (dataType) {
            case DataType.int4:
                return 'int4';
            case DataType.int8:
                return 'int8';
            case DataType.float4:
                return 'float4';
            case DataType.float8:
                return 'float8';
            case DataType.decimal:
                return 'decimal';
            case DataType.text:
                return 'text';
            case DataType.timestamp:
                return 'timestamp';
            default:
                throw new Error('Unsupported encryption type');
        }
    }


    public static convertNumberToType(type: number) : DataType {
        switch (type) {
            case 1:
                return DataType.int4;
            case 2:
                return DataType.int8;
            case 3:
                return DataType.float4;
            case 4:
                return DataType.float8;
            case 5:
                return DataType.decimal;
            case 6:
                return DataType.text;
            case 7:
                return DataType.timestamp;
            default:
                throw new Error('Unsupported encryption type');
        }
    }

    public static encodeDataByType(data: any, encType: DataType | undefined): Buffer {
        let result!: Uint8Array;
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        switch (encType) {
            case DataType.int4:
                view.setInt32(0, data, true);
                result = new Uint8Array(buffer, 0, 4);
                break;
            case DataType.int8:
                view.setBigInt64(0, BigInt(data), true);
                result = new Uint8Array(buffer);
                break;
            case DataType.float4:
                view.setFloat32(0, data, true);
                result = new Uint8Array(buffer, 0, 4);
                break;
            case DataType.float8:
                view.setFloat64(0, data, true);
                result = new Uint8Array(buffer);
                break;
            case DataType.decimal:
                const val = new Decimal(data);
                result = new TextEncoder().encode(val.toString());
                break;
            case DataType.text:
                result = new TextEncoder().encode(data);
                break;
            case DataType.timestamp:
                const uSec = BigInt(Math.floor(data * 1000));
                const offset = BigInt(
                    Math.floor(new Date().getTimezoneOffset() * 60 * 1000000),
                );
                const adjustedUSec = uSec - BigInt(946684800000000) + offset;
                view.setBigInt64(0, adjustedUSec, true);
                result = new Uint8Array(buffer);
                break;
            default:
                throw new Error('Unsupported encryption type');
        }
        return Buffer.from(result);
    }

    public static decodeDataByType(data: any, encType: number): any {
        let result: any;
        if (encType === DataType.int4) {
            // enc_int4
            const size = 4;
            const buf = data.slice(0, size);
            result = new Int32Array(buf.buffer)[0];
        } else if (encType === DataType.int8) {
            // enc_int8
            const size = 8;
            const buf = data.slice(0, size);
            result = new BigInt64Array(buf.buffer)[0];
            result = result.toString();//end n ?
        } else if (encType === DataType.float4) {
            // enc_float4
            const size = 4;
            const buf = data.slice(0, size);
            result = new Float32Array(buf.buffer)[0];
        } else if (encType === DataType.float8) {
            // enc_float8
            const size = 8;
            const buf = data.slice(0, size);
            result = new Float64Array(buf.buffer)[0];
        } else if (encType === DataType.decimal) {
            // enc_decimal
            result = new Decimal(Buffer.from(data).toString());
            result = result.toString();
        } else if (encType === DataType.text) {
            // enc_text
            result = new TextDecoder().decode(data);
        } else if (encType === DataType.timestamp) {
            // enc_timestamp
            const size = 8;
            const buf = data.slice(0, size);
            let u_sec = new BigInt64Array(buf.buffer)[0];
            u_sec += BigInt(946684800000000);
            u_sec -= BigInt(new Date().getTimezoneOffset() * 60 * 1000000);
            const time_stamp = Number(u_sec) / 1000000.0;
            result = time_stamp * 1000;
        } else {
            throw new Error('Unsupported encryption type');
        }
        return result;
    }

    public stringToFile(str : string, fileName : string, fileType : string): File {
        const blob = new Blob([str], { type : fileType } );
        return new File([blob], fileName, { type : fileType });
    }

    public static buildDownloadUrl(hashId: string, source : string ) : string{
        if (source === "arweave") {
            return "https://arseed.web3infura.io/" + hashId +
                "," + "https://arseed.web3infura.io/bundle/tx/" + hashId;
        } else if (source === 'web3storage') {
            return "https://dweb.link/ipfs/" + hashId;
        } else {
            return hashId;
        }
    }

    public static async fileToString(file: File): Promise<string> {
        return new Promise((resolve, reject) => {
            const fileReader = new FileReader();

            fileReader.onload = (event) => {
                const fileContent = event.target?.result;
                if (typeof fileContent === 'string') {
                    resolve(fileContent);
                } else {
                    reject(new Error('Failed to convert File to string.'));
                }
            };

            fileReader.readAsText(file);
        });
    }

    public parseCsvString(csvString: string): Array<Array<string>> {
        const lines = csvString.trim().split('\n');
        const result: Array<Array<string>> = [];
        for (const line of lines) {
            const cells = line.split(',');
            result.push(cells);
        }
        return result;
    }

    public convertResultType(data : Array<Array<string>>) : ResultType {
        return {
            code: 0,
            result: {data},
            message: undefined
        };
    }

    public formatArrayToString(array: Array<Array<string>>): string {
        let formattedString = '';

        for (const row of array) {
            const rowString = row.join(', ');
            formattedString += `[${rowString}]\n`;
            formattedString += "<br />";
        }

        return formattedString;
    }

    public async downloadAndConvertToFile(url: string, fileName: string): Promise<File | null> {
        try {
            const response = await fetch(url);

            if (!response.ok) {
                throw new Error('Network response was not ok');
            }

            const blob = await response.blob();
            return new File([blob], fileName, {type: blob.type});

        } catch (error) {
            console.error('Error downloading or converting file:', error);
            return null;
        }
    }
}