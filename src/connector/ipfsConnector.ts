// import {Column, Utils} from "./utils";
// import {ResultType} from "mind-lake-sdk/dist/types";
// import {CipherHelper} from "mind-lake-sdk/dist/util/cipher";
// import Result from "mind-lake-sdk/dist/util/result";
// import {Buffer} from "buffer";
// import {uploadToGreenfield, connectToMetaMask} from "./greenfieldConnector";
// import { create , IPFSHTTPClient} from 'ipfs-http-client'
// import {genAPI, getTokenTagByEver} from "arseeding-js";
//
//
// const DEFAULT_PROJECT_ID = "2UIVbXEd6jAxK8wCkDkNgYjivyh"
// const DEFAULT_PROJECT_SECRET = "fdd2d830acca802724cb773b711b9a8b"
// const auth = 'Basic ' + Buffer.from(DEFAULT_PROJECT_ID + ':' + DEFAULT_PROJECT_SECRET).toString('base64');
// const utils = new Utils();
//
// export class IpfsConnector {
//
//     private projectId : string;
//
//     private projectSecret : string;
//
//     private auth : string;
//
//     constructor(projectId: string, projectSecret: string) {
//         this.projectId = projectId;
//         this.projectSecret = projectSecret;
//         this.auth = 'Basic ' + Buffer.from(this.projectId + ':' + this.projectSecret).toString('base64');
//     }
//
//
//     public async saveToIpfsByString(dataString: string, metaDataString : string): Promise<ResultType> {
//         let client = IPFSHTTPClient | undefined;
//         try {
//             console.log(create);
//             client = create({
//                 host: 'ipfs.infura.io',
//                 port: 5001,
//                 protocol: 'https',
//                 headers: {
//                     authorization: this.auth,
//                 },}
//             );
//             const dataFileName = 'data.csv';
//             const file1 = utils.stringToFile(dataString, dataFileName, "text/plain");
//             const dataResult = await client.add(file1);
//             const url1 = `https://infura-ipfs.io/ipfs/${dataResult.path}`;
//             console.log("dataResult:", dataResult);
//             console.log("IPFS URI: ", url1);
//             const metaFileName = 'meta.json';
//             const file2 = utils.stringToFile(metaDataString, metaFileName, "text/plain");
//             const metaResult = await client.add(file2);
//             const url = `https://infura-ipfs.io/ipfs/${metaResult.path}`;
//             console.log("metaResult:", metaResult);
//             console.log("IPFS URI: ", url);
//             return Result.success({datahashId: dataResult.path, metaHashId: metaResult.path});
//         } catch (e) {
//             console.error('Exception:', e);
//             return Result.fail({code:60002, message:'save to ipfs error'});
//         }
//     }
//
//
// }