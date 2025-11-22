'use strict';

const ArtistRegistry = require('./api/ArtistRegistry');
const MusicFactory = require('./api/MusicFactory');
const FractionalVaultV3=require("./api/FractionalVaultV3");
const VestingVault=require("./api/VestingVault");
const Ognft=require("./api/Ognft");
const Earlynft=require("./api/Earlynft");
const Connft=require("./api/Connft");
const ethers=require('ethers')

const  Mutex = require('async-mutex').Mutex;
const dotenv=require('dotenv');
dotenv.config();

const PROCESSING_INTERVAL = 500; // Processing interval (milliseconds)



class DaoApi {
        
    // Process events in the queue
    processQueue=async()=> {

        const _this=this;
        if (this.isProcessing || this.eventQueue.length === 0) {
            return;
        }

        this.isProcessing = true;
        const eventData = this.eventQueue.shift();

        try {
            await this.processSingleEvent(eventData);
        } catch (error) {
            console.error("Error processing event:", error);
        } finally {
            // Set a timer to process the next event after 1 second
            setTimeout(() => {
                _this.isProcessing = false;
                _this.processQueue();
            }, PROCESSING_INTERVAL);
        }
    }

     processSingleEvent=async(_data)=> {
            await _data.fn(_data.data);       
    }
 
    get ethersProvider()
    {
    
        if(!this.ethersProvider_obj)
        this.ethersProvider_obj= new ethers.JsonRpcProvider(process.env.NEXT_PUBLIC_HTTPS_URL.replace('${NEXT_PUBLIC_BLOCKCHAIN_NETWORK}',process.env.NEXT_PUBLIC_BLOCKCHAIN_NETWORK))
        return this.ethersProvider_obj
    }
 
 
    get ArtistRegistry() { 
        if (!this.dao_uToken_obj) this.dao_uToken_obj =new ArtistRegistry(this.web3, process.env.NEXT_PUBLIC_ArtistRegistry,this); 
        return this.dao_uToken_obj; 
    }
  
    
    get MusicFactory() { 
        if (!this.dao_MusicFactory_obj) this.dao_MusicFactory_obj =new MusicFactory(this.web3, process.env.NEXT_PUBLIC_MusicFactory,this); 
        return this.dao_MusicFactory_obj; 
    }

    get FractionalVaultV3() { 
        if (!this.dao_aa_obj) this.dao_aa_obj =new FractionalVaultV3(this.web3, process.env.NEXT_PUBLIC_FractionalVaultV3,this); 
        return this.dao_aa_obj; 
    }

    
    get VestingVault() { 
        if (!this.dao_a1a_obj) this.dao_a1a_obj =new VestingVault(this.web3, process.env.NEXT_PUBLIC_VestingVault,this); 
        return this.dao_a1a_obj; 
    }
  
     
    get Ognft() { 
        if (!this.dao_Ognft_obj) this.dao_Ognft_obj =new Ognft(this.web3, process.env.NEXT_PUBLIC_OGNFT,this); 
        return this.dao_Ognft_obj; 
    }

    get Earlynft() { 
        if (!this.dao_Earlynft_obj) this.dao_Earlynft_obj =new Earlynft(this.web3, process.env.NEXT_PUBLIC_EarlyBirdNFT,this); 
        return this.dao_Earlynft_obj; 
    }

    get Connft() { 
        if (!this.dao_Connft_obj) this.dao_Connft_obj =new Connft(this.web3, process.env.NEXT_PUBLIC_ContributorNFT,this); 
        return this.dao_Connft_obj; 
    }


    constructor(_web3, _account) {
        this.web3 = _web3;
        this.account = _account;
        this.queueMutex = new Mutex(); 
        this.eventQueue = []; 
        this.isProcessing = false;

    }
}

module.exports = DaoApi

