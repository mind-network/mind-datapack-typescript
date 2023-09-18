import {ResultType} from "mind-lake-sdk/dist/types";
import {MindLake} from "mind-lake-sdk";
import {extParam} from "../util/utils";
import {MindLakeConnector} from "./mindlakeConnector";

/**
 * Connector interface, include mindlake , greenfield , ipfs, arweave, web3Storage
 *
 * @author tanlee
 * @since 2023.9.4
 */
export interface Connector {

    /**
     * loadFrom
     *
     * @param fileName fileName
     * @param identifier identifier
     * @param mindLakeConnector mindLakeConnector
     */
    loadFrom( fileName: string, identifier: string, mindLakeConnector: MindLakeConnector): Promise<File[]>;

    /**
     * saveTo
     *
     * @param dataString dataString
     * @param metaDataString metaDataString
     * @param mindLake mindLake
     * @param mindLakeConnector mindLakeConnector
     * @param param param
     */
    saveTo(dataString: string, metaDataString : string, mindLake : MindLake, mindLakeConnector: MindLakeConnector, param ?: extParam): Promise<ResultType>;
}