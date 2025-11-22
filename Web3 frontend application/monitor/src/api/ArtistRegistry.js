const abi=require('../abi/ArtistRegistry_abi.json');
const utils = require("../utils");
const BlockScanner=require('../BlockScanner')

class ArtistRegistry{
     

    constructor(_web3,_address,_parentThis) {
        this.web3=_web3; 
        this.address=_address;
        this.abi=abi
        this.parentThis=_parentThis;
    }
  

    ArtistRegistered(maxBlockNumber, callbackFun) {
        if (!this.contract) this.contract = new this.web3.eth.Contract(this.abi, this.address);
        const _this = this;
        this.scanner = new BlockScanner({
            web3:this.web3,
            contract: this.contract,
            storage: {
                get: async () => maxBlockNumber.value,
                set: async (v) => maxBlockNumber.value=v
            },
            cname:'ArtistRegistered',
            step: 500 
        });

        this.scanner.on("ArtistRegistered", async (data) => {
            console.log(data)
            const release = await _this.parentThis.queueMutex.acquire();
            try {
              
                _this.parentThis.eventQueue.push(
                    {
                        fn:callbackFun,
                        data:utils.valueFactory(data,{
                        "user_address": data.returnValues.artist
                        })
                    });
                    _this.parentThis.processQueue();
            } finally {
                release();
            } 
        });
        this.scanner.start();
    }

    unBind(){
        if(this.scanner)  this.scanner.stop();
    }


  }
  
  module.exports=ArtistRegistry