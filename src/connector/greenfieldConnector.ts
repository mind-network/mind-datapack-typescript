import { Client, GRNToString, MsgCreateObjectTypeUrl, newBucketGRN, PermissionTypes } from '@bnb-chain/greenfield-chain-sdk';
import { FileHandler } from '@bnb-chain/greenfiled-file-handle';
import { Wallet } from '@ethersproject/wallet';
import Web3 from 'web3';
import { Buffer } from 'buffer';

const GREEN_CHAIN_ID = 5600;
const GREENFIELD_RPC_URL = 'https://gnfd-testnet-fullnode-tendermint-us.bnbchain.org';

window.Buffer = Buffer;

export const connectToMetaMask = async function connectToMetaMask(chainID: number, rpcUrl: string) {
    // Check if MetaMask is installed
    if (typeof window.ethereum === 'undefined') {
        console.log('MetaMask is not installed.');
        return;
    }
    // Access MetaMask's Ethereum provider
    const provider = window.ethereum;

    // Request user's permission to connect
    // This will open the MetaMask popup for user interaction
    await provider.request({ method: 'eth_requestAccounts' });

    // Create a Web3 instance using the provider
    const web3 = new Web3(provider);

    // Now you can interact with MetaMask-connected accounts using 'web3'

    // Get the current selected account
    const accounts = await web3.eth.getAccounts();
    const address = accounts[0];

    console.log('address', address);

    // Get the current network ID
    const networkId = await web3.eth.net.getId();
    console.log('Connected to network:', networkId);

    // Check if connected to the right network
    if (networkId !== chainID) {
        try {
            await window.ethereum.request({
                method: 'wallet_switchEthereumChain',
                params: [{ chainId: web3.utils.toHex(chainID) }]
            });
        } catch (err) {
            // This error code indicates that the chain has not been added to MetaMask
            if (err.code === 4902) {
                console.log('Greenfield Testnet not added to MetaMask');
                // Add chain to MetaMask
                await window.ethereum.request({
                    method: 'wallet_addEthereumChain',
                    params: [
                        {
                            chainName: 'Greenfield Testnet',
                            chainId: web3.utils.toHex(chainID),
                            nativeCurrency: { name: 'BNB', decimals: 18, symbol: 'BNB' },
                            rpcUrls: [rpcUrl]
                        }
                    ]
                });
            }
        }
    }
    return address;
}

async function createTempWalletAndGrantFee(client: Client, address: string, bucketName: string) {
    console.log('client', client);
    // 1. create temporary account
    const wallet = Wallet.createRandom();

    // 2. allow temporary account to submit specified tx and amount
    const grantAllowanceTx = await client.feegrant.grantAllowance({
        granter: address,
        grantee: wallet.address,
        allowedMessages: [MsgCreateObjectTypeUrl],
        amount: Web3.utils.toWei('0.09', 'ether'),
        denom: 'BNB',
    });
    // console.log('grantAllowanceTx', grantAllowanceTx);

    // 3. Put bucket policy so that the temporary account can create objects within this bucket
    const statement: PermissionTypes.Statement = {
        effect: PermissionTypes.Effect.EFFECT_ALLOW,
        actions: [PermissionTypes.ActionType.ACTION_CREATE_OBJECT],
        resources: [GRNToString(newBucketGRN(bucketName))],
    };
    // console.log('statement', statement);
    const putPolicyTx = await client.bucket.putBucketPolicy(bucketName, {
        operator: address,
        statements: [statement],
        principal: {
            type: PermissionTypes.PrincipalType.PRINCIPAL_TYPE_GNFD_ACCOUNT,
            value: wallet.address,
        },
    });
    // console.log('putPolicyTx', putPolicyTx);

    // 4. broadcast txs include 2 msg
    const txs = await client.basic.multiTx([grantAllowanceTx, putPolicyTx]);
    console.log('txs', txs);
    const simuluateInfo = await txs.simulate({
        denom: 'BNB',
    });

    // console.log('simuluateInfo', simuluateInfo);
    const res = await txs.broadcast({
        denom: 'BNB',
        gasLimit: Number(210000),
        gasPrice: '5000000000',
        payer: address,
        granter: '',
    });

    console.log('res', res);

    if (res.code === 0) {
        console.log('succeed to create bucket policy and grant allowance');
        return wallet;
    }
}

export const uploadToGreenfield = async (bucketName: string, path: string, files: File[]) => {
    const address = await connectToMetaMask(GREEN_CHAIN_ID, GREENFIELD_RPC_URL);
    if (address === undefined) {
        console.log('Fail to connect to wallet');
        return;
    }
    const client = Client.create(GREENFIELD_RPC_URL, GREEN_CHAIN_ID.toString());
    const wallet = await createTempWalletAndGrantFee(client, address, bucketName);
    if (wallet === undefined) {
        console.log('Fail to create temp wallet and grant fee');
        return;
    }
    const granteeAddr = wallet.address;
    const privateKey = wallet.privateKey;

    console.log('Created a temp wallet %s with permission to bucket < %s >', wallet.address, bucketName);

    for (const file of files) {
        var objectName;
        if (path.endsWith('/')) {
            objectName = path + file.name
        } else {
            objectName = path + '/' + file.name
        }
        console.log('Processing file %s.%s', bucketName, objectName);
        const fileBytes = await file.arrayBuffer();
        const hashResult = await FileHandler.getPieceHashRoots(new Uint8Array(fileBytes));
        const { contentLength, expectCheckSums } = hashResult;
        console.log('File name: %s, size: %s, contentLength: %s, expectCheckSums: %s', file.name, file.size, contentLength, expectCheckSums);
        const createObjectTx = await client.object.createObject({
            bucketName: bucketName,
            objectName: objectName,
            visibility: 'VISIBILITY_TYPE_PUBLIC_READ',
            redundancyType: 'REDUNDANCY_EC_TYPE',
            contentLength,
            expectCheckSums,
            fileType: file.type,
            signType: 'authTypeV1',
            creator: granteeAddr,
            privateKey: privateKey,
        });

        const simulateInfo = await createObjectTx.simulate({
            denom: 'BNB',
        });

        // console.log('simulateInfo', simulateInfo);

        const res = await createObjectTx.broadcast({
            denom: 'BNB',
            gasLimit: Number(simulateInfo?.gasLimit),
            gasPrice: simulateInfo?.gasPrice || '5000000000',
            payer: granteeAddr,
            granter: address,
            privateKey,
        });

        if (res.code === 0) {
            console.log('succeed to create object');
        }

        var uploadRes;
        do {
            uploadRes = await client.object.uploadObject({
                bucketName: bucketName,
                objectName: objectName,
                body: file,
                txnHash: res.transactionHash,
                signType: 'authTypeV1',
                privateKey,
            });
            console.log('uploadRes', uploadRes);
        } while (uploadRes.statusCode === 404);

        if (uploadRes.code === 0) {
            console.log('succeed to upload object');
        }
    }
};

export const downloadFromGreenfield = async (bucketName: string, path: string, filenames: string[]) => {
    var files = [];
    try {
        path = path.replace(/\/$/, '');
        for (const filename of filenames) {
            const url = `https://gnfd-testnet-sp1.bnbchain.org/view/${bucketName}/${path}/${filename}`;
            const res = await fetch(url);
            const blob = await res.blob();
            const file = new File([blob], filename, { type: blob.type });
            files.push(file);
        }
        return files;
    } catch (err) {
        console.error(err);
        return files;
    }
};