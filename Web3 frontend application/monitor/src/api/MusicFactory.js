const abi=require('../abi/MusicFactory_abi.json');
const utils = require("../utils");
const BlockScanner=require('../BlockScanner')
const { ethers } = require('ethers');

 class MusicFactory 
{
     

    constructor(_web3,_address,_parentThis) {
        this.web3=_web3; 
        this.address=_address;
        this.abi=abi
        this.parentThis=_parentThis;
    }
  
    SongCreated(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner1 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
               cname:'SongCreated',
            step: 500 
        });

        this.scanner1.on("SongCreated", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "series_address": data.returnValues.seriesAddr, 
                        "token_id": data.returnValues.tokenId, 
                        "ipfsCid": data.returnValues.ipfsCid, 
                        "memetoken_address": data.returnValues.memeToken, 
                        "start_time": data.returnValues.time, 
                        "song_id":data.returnValues.songId, 
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner1.start();
    }   
      
       
   
    Subscribed(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner2 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
            cname:'Subscribed',
            step: 500 
        });

        this.scanner2.on("Subscribed", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "series_address": data.returnValues.seriesAddr, 
                        "token_id": data.returnValues.tokenId, 
                        "sub_id": data.returnValues.subId, 
                        "user_address": data.returnValues.user, 
                        "sub_amount":ethers.formatEther(data.returnValues.amountBNB), 
                        "meme_token":ethers.formatEther(data.returnValues.tokenAmount), 
                        "sub_seconds": data.returnValues.secondsBought,
                        "start_second": data.returnValues.startSec, 
                        "song_id":data.returnValues.songId, 
                        "time":data.returnValues.time, 
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner2.start();
    }   
      
       
    WhitelistAdded(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner3 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
             cname:'WhitelistAdded',
            step: 500 
        });

        this.scanner3.on("WhitelistAdded", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "series_address": data.returnValues.seriesAddr, 
                        "token_id": data.returnValues.tokenId, 
                        "accounts": data.returnValues.accounts, 
                        "song_id": data.returnValues.songId,
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner3.start();
    }   
       
        
    Finalized(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner4 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
              cname:'Finalized',
            step: 500 
        });

        this.scanner4.on("Finalized", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "series_address": data.returnValues.seriesAddr, 
                        "token_id": data.returnValues.tokenId, 
                        "liquidity":ethers.formatEther(data.returnValues.liquidity), 
                        "song_id":data.returnValues.songId, 
                        "lp_token_id":data.returnValues.lpTokenId, 
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner4.start();
    }   
        
    MusicPre(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner5 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
             cname:'MusicPre',
            step: 500 
        });

        this.scanner5.on("MusicPre", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "user_address": data.returnValues.artist, 
                        "songPreId": data.returnValues.songPreId,
                        "intervalTime":data.returnValues.intervalTime, 
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner5.start();
    }

     unBind(){
        if(this.scanner1) this.scanner1.stop();
        if(this.scanner2) this.scanner2.stop();
        if(this.scanner3) this.scanner3.stop();
        if(this.scanner4) this.scanner4.stop();
        if(this.scanner5) this.scanner5.stop();
    }
  
  }
  
  module.exports=MusicFactory