
const Server = require('./src/server');
const schedule =require("node-schedule");
const mysql = require('mysql2/promise'); 
const dotenv=require('dotenv');
const MultiRpcClient=require('./src/MultiRpcClient')
const serriseAbi=require('./src/abi/MusicSeries_abi.json')
const factoryAbi=require('./src/abi/MusicFactory_abi.json');

dotenv.config();

const rpcProviders = [
  'https://bsc-testnet.publicnode.com',
  'https://bsc-testnet.infura.io/v3/5c746c888c834f15b16c32e426c67abc', 
];
console.log(rpcProviders)
const rpcClient = new MultiRpcClient(rpcProviders);

let start_block=73097151n; 
var monitor = 0; //Restart every 10 minutes 
var server1=new Server();
var maxData = []; // Record the maximum block number that has been listened to

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

async function daoListenStart() {
  p('daoListenStart----------------->')
   monitor = 0;
   server1.daoapi.ArtistRegistry.unBind();
   server1.daoapi.FractionalVaultV3.unBind();
   server1.daoapi.MusicFactory.unBind();
   server1.daoapi.VestingVault.unBind();
   await server1.restart();
   daoListen();
}

if (server1.web3 && server1.web3.currentProvider && server1.web3.currentProvider.on) {
   server1.web3.currentProvider.on('error', async (err) => {
     console.error('[WebSocket] error:', err.message);
     console.log('[Server] Attempting automatic restart due to WS error...');
     try {
       await server1.restart();
       console.log('[Server] Automatic restart successful');
     } catch (err) {
       console.error('[Server] Automatic restart failed:', err.message);
     }
   });
 }

async function hand() {
    //Obtain the maximum block number that needs to be monitored from the database
   let sql = 'SELECT IFNULL(MAX(block_num),0)+1 s FROM w_music'  //0 
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM w_artist'  //1 
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM w_finalized' //2 
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_music_sub'  //3 
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_whitelist'  //4
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_claim'  //5
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_user_redeem'  //6
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_artist_redeem'  //7
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_claim_artist'  //8
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM w_musc_pre'  //9
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_nftog'  //10
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_nftearly'  //11
        + ' UNION ALL SELECT IFNULL(MAX(block_num),0) FROM t_nftcon';  //12

   const rows=await promisePool.query(sql,[]);
   rows[0].forEach(element => {maxData.push({value: BigInt(element.s)>start_block?BigInt(element.s):start_block})});
   console.info(maxData)
   
   await server1.start();
   daoListen();
   schedule.scheduleJob('*/5 * * * * *', () => {
    server1.daoapi.processQueue();
    requestBlock();
    if (monitor > 12*10 && server1.daoapi.eventQueue.length===0 && !server1.daoapi.isProcessing) daoListenStart();
    monitor++;
  });
  requestBlock();
}



const searchSql='SELECT song_id,series_address FROM t_music a WHERE song_id>0 AND is_end=0 AND EXISTS(SELECT 1 FROM t_music_sub s WHERE s.song_id = a.song_id) AND UNIX_TIMESTAMP()-start_time >(8*60)';
const createSongSql='SELECT song_pre_id FROM t_music WHERE song_id=0 AND song_pre_id>0 AND confirm_time<UNIX_TIMESTAMP()';

async function requestBlock(){
    
  const [rows,]=await promisePool.query(searchSql,[]);
  if(Array.isArray(rows)){
    rows.forEach((item, index) => {
      rpcClient.sendContract(item.series_address,serriseAbi,'finalize',[item.song_id])
    });
  }

  const [songrows,]=await promisePool.query(createSongSql,[]);
  if(Array.isArray(songrows)){
    songrows.forEach((item, index) => {
      rpcClient.sendContract(process.env.NEXT_PUBLIC_MusicFactory,factoryAbi,'musicConfirm',[item.song_pre_id])
    });
  }

}

async function daoListen() {
   p("start...........")
  
   const subscribeMethods = [
     { method: ArtistRegistered, name: 'ArtistRegistered', delay: true },
     { method: WhitelistAdded, name: 'WhitelistAdded', delay: true },
     { method: SongCreated, name: 'SongCreated', delay: true },
     { method: Finalized, name: 'Finalized', delay: true },
     { method: Subscribed, name: 'Subscribed', delay: true },
     { method: Claimed, name: 'Claimed', delay: true },
     { method: userRedeemed, name: 'userRedeemed', delay: true },
     { method: LPRedeemed, name: 'LPRedeemed', delay: true },
     { method: FeesClaimed, name: 'FeesClaimed', delay: true },
     { method: MusicPre, name: 'MusicPre', delay: true },
   ];
 
   // Sequential execution with delay
   for (let i = 0; i < subscribeMethods.length; i++) {
     const { method, name, delay } = subscribeMethods[i];
     
     try {
       method(); // Execute subscription method

       // If it's not the last method, add a delay
       if (delay && i < subscribeMethods.length - 1) {
         const delayMs = Math.floor(Math.random() * 2000) + 1000; //1-3 second random delay
       
         await new Promise(resolve => setTimeout(resolve, delayMs));
       }
       
     } catch (error) {
       console.error(`Error executing subscription ${name}:`, error);
       // When an error occurs, wait for a period of time before continuing
       const errorDelay = 2000; // Wait for 2 seconds when there is an error
       console.log(`Error occurred, waiting$ {errorDelay}ms Continue afterwards ..`);
       await new Promise(resolve => setTimeout(resolve, errorDelay));
     }
   }
   

 }

// Start monitoring
hand();


async function executeSql(addSql, addSqlParams) {
   if(process.env.IS_DEBUGGER==='1') console.info(addSql,addSqlParams.join(','))
   await promisePool.execute(addSql,addSqlParams)
}


function p(str) {
  let myDate = new Date();
  console.info(myDate.getFullYear() + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate() + ' ' + myDate.getHours() + ":" + myDate.getMinutes() + ":" + myDate.getSeconds() + "-->" + str)
}
//----------------------------------------------------------------------------

function SongCreated()
{
  server1.daoapi.MusicFactory.SongCreated(maxData[0], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const musicId=data.ipfsCid.substring(data.ipfsCid.lastIndexOf("/") + 1);
      const sql ="INSERT IGNORE INTO w_music(block_num,music_id,series_address,memetoken_address,token_id,start_time,song_id) VALUES(?,?,?,?,?,?,?)";
      const params=[obj.blockNumber,musicId,data.series_address,data.memetoken_address,data.token_id,data.start_time,data.song_id];
      // maxData[0] = obj.blockNumber+1n;
      await executeSql(sql, params); 
     });

}

function ArtistRegistered()
{
  server1.daoapi.ArtistRegistry.ArtistRegistered(maxData[1], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const sql ="INSERT IGNORE INTO w_artist(block_num,user_address) VALUES(?,?)";
      // maxData[1] = obj.blockNumber+1n;
      const  params=[obj.blockNumber,data.user_address];
      await executeSql(sql, params); 
     });
}

function Finalized()
{
  server1.daoapi.MusicFactory.Finalized(maxData[2], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const sql ="INSERT IGNORE INTO w_finalized(token_id,block_num,series_address,song_id,lp_token_id,liquidity) VALUES(?,?,?,?,?,?)";
      const params=[data.token_id,obj.blockNumber,data.series_address,data.song_id,data.lp_token_id,data.liquidity];
      // maxData[2] = obj.blockNumber+1n;
      await executeSql(sql, params); 
     });
}


function Subscribed()
{
  server1.daoapi.MusicFactory.Subscribed(maxData[3], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const sql ="INSERT IGNORE INTO t_music_sub(sub_id,block_num,token_id,user_address,start_second,sub_seconds,sub_type,sub_amount,series_address,meme_token,song_id,hash_time,tx_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
      const params=[data.sub_id,obj.blockNumber,data.token_id,data.user_address,data.start_second,data.sub_seconds,0,data.sub_amount,data.series_address,data.meme_token,data.song_id,data.time,obj.transactionHash];
      // maxData[3] = obj.blockNumber+1n;
      await executeSql(sql, params); 
     });
}

function WhitelistAdded()
{
  server1.daoapi.MusicFactory.WhitelistAdded(maxData[4], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const sql ="INSERT IGNORE INTO t_whitelist(token_id,user_address,block_num,series_address,song_id) VALUES(?,?,?,?,?)";
      // maxData[4] = obj.blockNumber+1n;
      data.accounts.forEach( async userAddress=>{
        let params=[data.token_id,userAddress.toLowerCase(),obj.blockNumber,data.series_address,data.song_id];
        await executeSql(sql, params); 
     })
  });
}


function Claimed()
{
  server1.daoapi.FractionalVaultV3.Claimed(maxData[5], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
    
         let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
         const [rows,]=await promisePool.query(sql,[data.song_id]);
         let meme_token=0;
         let bnb_token=0;
         if(rows[0].memetoken_address.toLowerCase()===data.token0.toLowerCase()){
           meme_token=data.amount0;bnb_token=data.amount1;
         } else {
          meme_token=data.amount1;bnb_token=data.amount0;
         }

      sql ="INSERT IGNORE INTO t_claim(block_num,song_id,user_address,bnb_token,meme_token) VALUES(?,?,?,?,?)";
      // maxData[5] = obj.blockNumber+1n;
     
      let params=[obj.blockNumber,data.song_id,data.user_address.toLowerCase(),bnb_token,meme_token];
      await executeSql(sql, params); 
  });

}



function userRedeemed()
{
  server1.daoapi.FractionalVaultV3.Redeemed(maxData[6], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj

      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows,]=await promisePool.query(sql,[data.song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===data.token0.toLowerCase()){
        meme_token=data.amount0;bnb_token=data.amount1;
      } else {
       meme_token=data.amount1;bnb_token=data.amount0;
      }

      sql ="INSERT IGNORE INTO t_user_redeem(block_num,song_id,user_address,bnb_token,meme_token,liqRemoved) VALUES(?,?,?,?,?,?)";
      // maxData[6] = obj.blockNumber+1n;
     
      let params=[obj.blockNumber,data.song_id,data.user_address.toLowerCase(),bnb_token,meme_token,data.liqRemoved];
      await executeSql(sql, params); 
  });
}


function LPRedeemed()
{
  server1.daoapi.VestingVault.LPRedeemed(maxData[7], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows,]=await promisePool.query(sql,[data.song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===data.token0.toLowerCase()){
        meme_token=data.amount0;bnb_token=data.amount1;
      } else {
       meme_token=data.amount1;bnb_token=data.amount0;
      }

      sql ="INSERT IGNORE INTO t_artist_redeem(block_num,song_id,user_address,bnb_token,meme_token,lpUnits) VALUES(?,?,?,?,?,?)";
      // maxData[7] = obj.blockNumber+1n;
      let params=[obj.blockNumber,data.song_id,data.user_address.toLowerCase(),bnb_token,meme_token,data.lpUnits];
      await executeSql(sql, params); 
  });

}



function FeesClaimed()
{
  server1.daoapi.VestingVault.FeesClaimed(maxData[8], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj

      let sql="SELECT memetoken_address FROM w_music WHERE song_id=?";
      const [rows,]=await promisePool.query(sql,[data.song_id]);
      let meme_token=0;
      let bnb_token=0;
      if(rows[0].memetoken_address.toLowerCase()===data.token0.toLowerCase()){
        meme_token=data.amount0;bnb_token=data.amount1;
      } else {
       meme_token=data.amount1;bnb_token=data.amount0;
      }

      sql ="INSERT IGNORE INTO t_claim_artist (block_num,song_id,user_address,bnb_token,meme_token) VALUES(?,?,?,?,?)";
      // maxData[8] = obj.blockNumber+1n;
      let params=[obj.blockNumber,data.song_id,data.user_address.toLowerCase(),bnb_token,meme_token];
      await executeSql(sql, params); 
  });
}


function MusicPre()
{
  server1.daoapi.MusicFactory.MusicPre(maxData[9], async (obj) => {
      if(process.env.IS_DEBUGGER==='1') console.info(obj)
      const {data}=obj
      const re=await rpcClient.callContract(process.env.NEXT_PUBLIC_MusicFactory,factoryAbi,'musicPre',[data.songPreId]);

      const musicId=re[1].substring(re[1].lastIndexOf("/") + 1);
      const confirmTime=re[6];

      let sql="INSERT INTO w_musc_pre(block_num,music_id,user_address,song_pre_id,planned_sec,confirm_time) VALUES(?,?,?,?,?,?)";
      // maxData[9] = obj.blockNumber+1n;
      let params=[obj.blockNumber,musicId,data.user_address.toLowerCase(),data.songPreId,data.intervalTime,confirmTime];
      await executeSql(sql, params); 
  });

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
 
