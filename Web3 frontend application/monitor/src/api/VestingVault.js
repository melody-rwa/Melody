const abi=require('../abi/VestingVault_abi.json');
const utils = require("../utils");
const BlockScanner=require('../BlockScanner')
const { ethers } = require('ethers');

 class VestingVault 
{
     
    constructor(_web3,_address,_parentThis) {
        this.web3=_web3; 
        this.address=_address;
        this.abi=abi
        this.parentThis=_parentThis;

    }

      
    LPRedeemed(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner1 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
             cname:'LPRedeemed',
            step: 500 
        });

        this.scanner1.on("LPRedeemed", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                            "song_id": data.returnValues.songId, 
                            "user_address": data.returnValues.artist,
                            "lpUnits":ethers.formatEther(data.returnValues.lpUnits),
                            "token0":data.returnValues.token0,
                            "token1":data.returnValues.token1,
                            "amount0":ethers.formatEther(data.returnValues.amount0),
                            "amount1":ethers.formatEther(data.returnValues.amount1),
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner1.start();
    }

       
    FeesClaimed(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner2 = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
             cname:'FeesClaimed',
            step: 500 
        });

        this.scanner2.on("FeesClaimed", async (data) => {
            const release = await _this.parentThis.queueMutex.acquire();
            try {
                
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                            "song_id": data.returnValues.songId, 
                            "user_address": data.returnValues.artist,
                            "token0":data.returnValues.token0,
                            "token1":data.returnValues.token1,
                            "amount0":ethers.formatEther(data.returnValues.amount0),
                            "amount1":ethers.formatEther(data.returnValues.amount1)
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner2.start();
    }
 
  
        unBind(){
        if(this.scanner1) this.scanner1.stop();
        if(this.scanner2) this.scanner2.stop();
    }
 
 
  }
  
  module.exports=VestingVault