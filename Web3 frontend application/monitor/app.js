const mysql = require('mysql2/promise'); 
const dotenv=require('dotenv');
const fs = require('fs');
const serriseAbi=require('./src/abi/MusicSeries_abi.json')
const factoryAbi=require('./src/abi/MusicFactory_abi.json');
const vestvalut=require('./src/abi/VestingVault_abi.json');
const fractvault3=require('./src/abi/FractionalVaultV3_abi.json');
const artistregist=require('./src/abi/ArtistRegistry_abi.json');
const { ethers, Interface, keccak256, toUtf8Bytes } = require("ethers");
let saveBlock=Number(require('./maxblock/data.json'));
dotenv.config();


const abi = [
  "event FeesClaimed(uint256 indexed songId, address indexed artist, address token0, address token1, uint256 amount0, uint256 amount1)",
  "event LPRedeemed(uint256 indexed songId, address indexed artist, uint256 lpUnits, address token0, address token1, uint256 amount0, uint256 amount1)",
  "event Claimed(address indexed user, uint256 indexed songId, uint256 amount0, uint256 amount1,address token0,address token1)",
  "event Redeemed(address indexed user, uint256 indexed songId, uint128 liqRemoved, uint256 amount0, uint256 amount1,address token0,address token1)",
  "event ArtistRegistered(address indexed artist, string profileCid)",
  "event MusicPre(address indexed artist,uint256 songPreId,uint256 now,uint256 intervalTime)",
  "event SongCreated(address indexed seriesAddr,uint256 indexed tokenId, string ipfsCid, uint256 targetBNB, uint256 durationSec, address memeToken,uint256 time,uint256 songId)",
  "event Subscribed(address indexed seriesAddr,uint256 indexed tokenId, uint256 indexed subId, address user, uint256 amountBNB, uint256 tokenAmount, uint256 secondsBought, uint256 startSec,uint256 songId,uint256 time)",
  "event Finalized(address indexed seriesAddr,uint256 indexed tokenId, uint256 lpTokenId, uint128 liquidity,uint256 songId)",
  "event WhitelistAdded(address indexed seriesAddr,uint256 indexed tokenId, address[] accounts,uint256 songId)"
];

const iface = new Interface(abi);
const topics = iface.fragments
  .filter(f => f.type === "event")
  .map(f => {
    const types = f.inputs.map(i => i.type).join(",");
    const signature = `${f.name}(${types})`;
    return keccak256(toUtf8Bytes(signature));
  });

const CONFIRMATIONS = 15; // Delay 15 blocks to prevent rollback

const provider = new ethers.JsonRpcProvider(process.env.NEXT_PUBLIC_HTTPS_URL);
let lastSyncedBlock = 0; //starting block

const wallet = new ethers.Wallet("f8b731a244fd1a9e5c291fc9f5c6f70ea9571d53581561cd61874bd55a4614fd", provider);

const ADDRS = [
 process.env.NEXT_PUBLIC_MusicFactory.toLowerCase(),
 process.env.NEXT_PUBLIC_FractionalVaultV3.toLowerCase(),
 process.env.NEXT_PUBLIC_VestingVault.toLowerCase(),
 process.env.NEXT_PUBLIC_ArtistRegistry.toLowerCase(),
];

const handlers = {
  [ADDRS[0]]: {
    contract: new ethers.Contract(process.env.NEXT_PUBLIC_MusicFactory, factoryAbi, wallet),
    events: {
      SongCreated:  async (args) =>await SongCreatedBusi(args),
      Subscribed: async (args) =>await SubscribedBusi(args),
      Finalized:  async (args) =>await FinalizedBusi(args),
      WhitelistAdded: async (args) =>await WhitelistAddedBusi(args),
      MusicPre:  async (args) =>await MusicPreBusi(args),
    }
  },

  [ADDRS[1]]: {
    contract: new ethers.Contract(process.env.NEXT_PUBLIC_FractionalVaultV3, fractvault3, wallet),
    events: {
      Claimed:  async (args) =>await ClaimedBusi(args),
      Redeemed:  async (args) =>await userRedeemedBusi(args),
    }
  },

  [ADDRS[2]]: {
    contract: new ethers.Contract(process.env.NEXT_PUBLIC_VestingVault, vestvalut, wallet),
    events: {
      FeesClaimed: async (args) =>await FeesClaimedBusi(args),
      LPRedeemed: async(args) =>await LPRedeemedBusi(args),
    }
  },

  [ADDRS[3]]: {
    contract: new ethers.Contract(process.env.NEXT_PUBLIC_ArtistRegistry, artistregist, wallet),
    events: {
      ArtistRegistered:async (args) =>await ArtistRegisteredBusi(args)
      
    }
  },
};

async function handle(log) {
  const addr = log.address.toLowerCase();
  const entry = handlers[addr];
  if (!entry) return; 

  try {
    const parsed = entry.contract.interface.parseLog(log);
    const fn = entry.events[parsed.name];
    if (fn) {

      const para={
        event:parsed.name,
        address:addr,
        blockHash:log.blockHash,
        blockNumber:log.blockNumber,
        transactionHash:log.transactionHash,
        data:parsed.args
      };

     await fn.call(this,para);   // Execute event handling

    } else {
      console.log("Unknown event:", parsed.name, parsed.args);
    }

  } catch (err) {
    // parse
  }
}


const promisePool = mysql.createPool({
   host: process.env.MYSQL_HOST,
   user: process.env.MYSQL_USER,
   password: process.env.MYSQL_PASSWORD,
   database: process.env.MYSQL_DATABASE,
   port: process.env.MYSQL_PORT,
   waitForConnections: true,
   connectionLimit: 1,  
   queueLimit: 0,      
   enableKeepAlive: true,
   keepAliveInitialDelay: 0
 });

 promisePool.on('error', (err) => {
   console.error('Database connection error:', err);
 });

 async function finalize(item){
    const contract = new ethers.Contract(item.series_address, serriseAbi, wallet);
    try {
      const tx =  await contract.finalize(item.song_id);
      console.log("transaction hash:", tx.hash);
      await tx.wait(); 
      console.log(`finalize(${item.song_id}) Transaction confirmed`);
     } catch (error) {
       console.log(geneErr(error?.message.toString()))
    }
 }

 function geneErr(errStr){

    if(errStr.startsWith('execution reverted'))
    {
      const m = errStr.match(/execution reverted:\s*"([^"]+)"/);
      const mess= m ? m[1] : '';
      return mess;
    } 
    else {
      return 'On chain request error';
    }
 }
const searchSql='SELECT song_id,series_address FROM t_music a WHERE song_id>0 AND is_end=0 AND EXISTS(SELECT 1 FROM t_music_sub s WHERE s.song_id = a.song_id) AND UNIX_TIMESTAMP()-start_time >(8*60)';
const createSongSql='SELECT song_pre_id FROM t_music WHERE song_id=0 AND song_pre_id>0 AND confirm_time<UNIX_TIMESTAMP()';

async function requestBlock() {
  // p('ookk loop')
  const [rows] = await promisePool.query(searchSql, []);
  for (const item of rows) {
    await finalize(item);      // Process in order
  }

  const [songrows] = await promisePool.query(createSongSql, []);
  for (const item of songrows) { 
    try {
        const tx = await  handlers[process.env.NEXT_PUBLIC_MusicFactory.toLowerCase()].contract.musicConfirm(item.song_pre_id);
        console.log("transaction hash:", tx.hash);
        await tx.wait(); 
        console.log(`musicConfirm(${item.song_id}) Transaction confirmed`);
     } catch (error) {
      console.log(geneErr(error?.message.toString()))
    }
  }
}



async function startMultiPolling() {
   async function loop() {
    await requestBlock();     
    setTimeout(loop, 5000);     
    }
    loop(); 

    lastSyncedBlock =Number(await provider.getBlockNumber());
    //Restore the last parsed block. If it is less than 1000, continue with the previous block. 
    // Otherwise, record the missed block segments
    if(lastSyncedBlock-saveBlock<1000) lastSyncedBlock=saveBlock;
    else {
      write(new Date().getTime(),`${saveBlock}--${lastSyncedBlock}`)
    }
    // lastSyncedBlock=74022364

    while (true) {
        try {
            const currentBlock = await provider.getBlockNumber();
            const safeBlock = currentBlock - CONFIRMATIONS;
            if (safeBlock > lastSyncedBlock) {
                //Calculate the range of this pull, with a maximum of 2000 blocks
                const endBlock = Math.min(safeBlock, lastSyncedBlock + 2000);
                
                console.log(` ${lastSyncedBlock + 1} -> ${endBlock}====>${endBlock-lastSyncedBlock-1}`);
                    const logs = await provider.getLogs({
                        fromBlock: lastSyncedBlock + 1,
                        toBlock: endBlock,
                        address: ADDRS, 
                        topics: [topics] // If you want to listen for specific event signatures, you can add them here. Not adding them means listening for all events
                    });
  
                for (const log of logs) { await handle(log) }
                lastSyncedBlock = endBlock;
                if(endBlock-saveBlock>300){
                    write('data',endBlock.toString());
                    saveBlock=endBlock;
                }
            } else {

                await new Promise(r => setTimeout(r, 3000));
                console.log("Wait a moment.")
            }
        } catch (err) {
            console.error("Polling error:", err);
            await new Promise(r => setTimeout(r, 5000)); 
        }
        
        const currentBlock = await provider.getBlockNumber();
        if (currentBlock - lastSyncedBlock < 5) {
             await new Promise(r => setTimeout(r, 3000));
        }
        await new Promise(r => setTimeout(r, 2000));
      
    }
}

startMultiPolling();


async function executeSql(addSql, addSqlParams) {
   if(process.env.IS_DEBUGGER==='1') console.info(addSql,addSqlParams.join(','))
   await promisePool.execute(addSql,addSqlParams)
}

function p(str) {
  let myDate = new Date();
  console.info(myDate.getFullYear() + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate() + ' ' + myDate.getHours() + ":" + myDate.getMinutes() + ":" + myDate.getSeconds() + "-->" + str)
}
//----------------------------------------------------------------------------

async function SongCreatedBusi(obj)
{
      const [series_address,token_id,ipfsCid,,,memetoken_address,start_time,song_id]=obj.data;        
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const musicId=ipfsCid.substring(ipfsCid.lastIndexOf("/") + 1);
      const sql ="INSERT IGNORE INTO w_music(block_num,music_id,series_address,memetoken_address,token_id,start_time,song_id) VALUES(?,?,?,?,?,?,?)";
      const params=[obj.blockNumber,musicId,series_address,memetoken_address,token_id,start_time,song_id];
      await executeSql(sql, params); 


}

async function ArtistRegisteredBusi(obj)
{
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [user_address]=obj.data;  
      const sql ="INSERT IGNORE INTO w_artist(block_num,user_address) VALUES(?,?)";
      const  params=[obj.blockNumber,user_address];
      await executeSql(sql, params); 
     
}

// function Ognftmined()
// {
//   server1.daoapi.Ognft.Mintd(maxData[10], async (obj) => {
//       if(process.env.IS_DEBUGGER==='1') console.info(obj)
//       const {data}=obj
//       const sql ="INSERT IGNORE INTO t_nftog(block_num,user_address,token_id) VALUES(?,?,?)";
//       // maxData[10] = obj.blockNumber+1n;
//       const  params=[obj.blockNumber,data.to,data.tokenId];
//       await executeSql(sql, params); 
//      });
// }

// function Earlymined()
// {
//   server1.daoapi.Earlynft.Mintd(maxData[11], async (obj) => {
//       if(process.env.IS_DEBUGGER==='1') console.info(obj)
//       const {data}=obj
//       const sql ="INSERT IGNORE INTO t_nftearly(block_num,user_address,token_id) VALUES(?,?,?)";
//       // maxData[11] = obj.blockNumber+1n;
//       const  params=[obj.blockNumber,data.to,data.tokenId];
//       await executeSql(sql, params); 
//      });
// }

// function Conmined()
// {
//   server1.daoapi.Connft.Mintd(maxData[12], async (obj) => {
//       if(process.env.IS_DEBUGGER==='1') console.info(obj)
//       const {data}=obj
//       const sql ="INSERT IGNORE INTO t_nftcon(block_num,user_address,token_id) VALUES(?,?,?)";
//       // maxData[12] = obj.blockNumber+1n;
//       const  params=[obj.blockNumber,data.to,data.tokenId];
//       await executeSql(sql, params); 
//      });
// }



async function FinalizedBusi(obj)
{


      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [series_address,token_id,liquidity,song_id,lp_token_id]=obj.data;  
      const sql ="INSERT IGNORE INTO w_finalized(token_id,block_num,series_address,song_id,lp_token_id,liquidity) VALUES(?,?,?,?,?,?)";
      const params=[token_id,obj.blockNumber,series_address,song_id,lp_token_id,ethers.formatEther(liquidity)];
      await executeSql(sql, params); 

}


async function SubscribedBusi(obj)
{

      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [series_address,token_id,sub_id,user_address,sub_amount,meme_token,sub_seconds,start_second,song_id,ticme]=obj.data;  
      const sql ="INSERT IGNORE INTO t_music_sub(sub_id,block_num,token_id,user_address,start_second,sub_seconds,sub_type,sub_amount,series_address,meme_token,song_id,hash_time,tx_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
      const params=[sub_id,obj.blockNumber,token_id,user_address,start_second,sub_seconds,0,ethers.formatEther(sub_amount),series_address,ethers.formatEther(meme_token),song_id,ticme,obj.transactionHash];
      await executeSql(sql, params); 

}

async function WhitelistAddedBusi(obj)
{ 
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [series_address,token_id,accounts,song_id]=obj.data; 
      const sql ="INSERT IGNORE INTO t_whitelist(token_id,user_address,block_num,series_address,song_id) VALUES(?,?,?,?,?)";
      accounts.forEach( async userAddress=>{
        let params=[token_id,userAddress.toLowerCase(),obj.blockNumber,series_address,song_id];
        await executeSql(sql, params); 
     })

}


async function ClaimedBusi(obj)
{
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [song_id,user_address,amount0,amount1,token0,token1]=obj.data; 
      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows,]=await promisePool.query(sql,[song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===token0.toLowerCase()){
        meme_token=amount0;bnb_token=amount1;
      } else {
      meme_token=amount1;bnb_token=amount0;
      }

      sql ="INSERT IGNORE INTO t_claim(block_num,song_id,user_address,bnb_token,meme_token) VALUES(?,?,?,?,?)";
      let params=[obj.blockNumber,song_id,user_address.toLowerCase(),ethers.formatEther(bnb_token),ethers.formatEther(meme_token)];
      await executeSql(sql, params); 
}



async function userRedeemedBusi(obj)
{
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [user_address,song_id,liqRemoved,amount0,amount1,token0,token1]=obj.data; 

      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows]=await promisePool.query(sql,[song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===token0.toLowerCase()){
        meme_token=amount0;bnb_token=amount1;
      } else {
       meme_token=amount1;bnb_token=amount0;
      }

      sql ="INSERT IGNORE INTO t_user_redeem(block_num,song_id,user_address,bnb_token,meme_token,liqRemoved) VALUES(?,?,?,?,?,?)";     
      let params=[obj.blockNumber,song_id,user_address.toLowerCase(),ethers.formatEther(bnb_token),ethers.formatEther(meme_token),ethers.formatEther(liqRemoved)];
      await executeSql(sql, params); 

}


async function LPRedeemedBusi(obj)
{
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [song_id,user_address,lpUnits,token0,token1,amount0,amount1]=obj.data; 
      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows,]=await promisePool.query(sql,[song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===token0.toLowerCase()){
        meme_token=amount0;bnb_token=amount1;
      } else {
       meme_token=amount1;bnb_token=amount0;
      }

      sql ="INSERT IGNORE INTO t_artist_redeem(block_num,song_id,user_address,bnb_token,meme_token,lpUnits) VALUES(?,?,?,?,?,?)";
      let params=[obj.blockNumber,song_id,user_address.toLowerCase(),ethers.formatEther(bnb_token),ethers.formatEther(meme_token),ethers.formatEther(lpUnits)];
      await executeSql(sql, params); 


}



async function FeesClaimedBusi()
{
  
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [song_id,user_address,token0,token1,amount0,amount1]=obj.data; 

      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows]=await promisePool.query(sql,[song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===token0.toLowerCase()){
        meme_token=amount0;bnb_token=amount1;
      } else {
       meme_token=amount1;bnb_token=amount0;
      }

      sql ="INSERT IGNORE INTO t_claim_artist (block_num,song_id,user_address,bnb_token,meme_token) VALUES(?,?,?,?,?)";
      let params=[obj.blockNumber,song_id,user_address.toLowerCase(),ethers.formatEther(bnb_token),ethers.formatEther(meme_token)];
      await executeSql(sql, params); 

}


async function MusicPreBusi(obj)
{

      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const [user_address,songPreId,,intervalTime]=obj.data; 
      const re=await handlers[process.env.NEXT_PUBLIC_MusicFactory.toLowerCase()].contract.musicPre(songPreId);

      const musicId=re[1].substring(re[1].lastIndexOf("/") + 1);
      const confirmTime=re[6];

      let sql="INSERT INTO w_musc_pre(block_num,music_id,user_address,song_pre_id,planned_sec,confirm_time) VALUES(?,?,?,?,?,?)";
      let params=[obj.blockNumber,musicId,user_address.toLowerCase(),songPreId,intervalTime,confirmTime];
      await executeSql(sql, params); 


}

 function write(fileName,text){
    fs.writeFileSync(`./maxblock/${fileName}.json`,text,'utf-8');
    console.log(`${text}--> write success！`);
 }

async function gracefulShutdown() {
   console.info('Shutting down gracefully...');
   if (promisePool) {
     await promisePool.end();
     console.info('Database connection pool closed.');
   }
   process.exit(0);
 }
 
 process.on('SIGINT', gracefulShutdown); // Handle Ctrl+C
 process.on('SIGTERM', gracefulShutdown); // Handle kill command
 
