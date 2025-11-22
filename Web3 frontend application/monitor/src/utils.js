const ethers=require('ethers')
class Utils{


    async getCheckEvent(ethersProvider,eventIface,eventName,transactionHash)
    {
      let res=await ethersProvider.getTransactionReceipt(transactionHash)
      let lok=false
      for(let i=0;i<res.logs.length;i++)
      {
        try{
             let findLog=eventIface.decodeEventLog(eventName, res.logs[i].data, res.logs[i].topics)
            if(findLog) {lok=true; break;}
        } catch(e){}
      }
      return {gasUsed:res.gasUsed,isOk:lok};
    }

    
    async getCheckEvent_logs(ethersProvider,transactionHash)
    {
      let res=await ethersProvider.getTransactionReceipt(transactionHash)
      return res
    }

    
    getCheckEvent_ex(res,eventIface,eventName)
    {
        let lok=false
      for(let i=0;i<res.logs.length;i++)
      {
        try{
             let findLog=eventIface.decodeEventLog(eventName, res.logs[i].data, res.logs[i].topics)
            if(findLog) {lok=true; break;}
        } catch(e){}
      }
      return {gasUsed:res.gasUsed,isOk:lok};
    }


   async getTime(web3,blockNumber)
    {
        let _timestamp=(new Date()).getTime().toString().substring(0,10)
        if(!blockNumber) return _timestamp
        return _timestamp;
    }

    async gas(web3,transactionHash)
    {
       let obj=await  web3.eth.getTransactionReceipt(transactionHash)
       return obj.gasUsed
    }

    valueFactory(obj,data)
    {
        const {address,blockHash,blockNumber,transactionHash,transactionIndex,event}=obj
        const a= {address,blockHash,blockNumber,transactionHash,transactionIndex,data,event};
        return a;
    }

    log(str,err) {
        var myDate = new Date();
        console.log(`${myDate.getFullYear()}-${(myDate.getMonth() + 1)}-${myDate.getDate()} ${myDate.getHours()}:${myDate.getMinutes()}:${myDate.getSeconds()}-->${str}`,err)
    }

}

module.exports= new Utils();    