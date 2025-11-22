import { getRequestCount, incrementRequestCount } from "../requestCounter";
import { getData,execute, getPageData } from "./common";

export async function geneUser({ did }:any): Promise<UserInfo[]> {
    const _address=did.toLowerCase();
    let re: UserInfo[] = await getData(
        'select * FROM t_user WHERE user_address=?',[_address]
    );

    if(re.length===0){
        const randInt = Math.floor(Math.random() * 1000) + 600;
        incrementRequestCount();
        const sid=getRequestCount()%10;
        await  execute("INSERT IGNORE INTO t_user(user_address,user_name,user_avatar) VALUES(?,?,?)",
            [_address,`user${randInt}`,`https://${process.env.NEXT_PUBLIC_DOMAIN}/u${sid}.svg`]);
        re=await getData(
            'select * FROM t_user WHERE  user_address=?',
            [_address]
        );
    }
    return re;
  }

export async function getUser({ did }:any): Promise<UserInfo[]> {
  const _address=did.toLowerCase();
  const re: UserInfo[] = await getData('select * FROM t_user WHERE user_address=?',[_address]);
  return re;
}
  
export  interface bnbType{
    bnb_amount:number;
  }


export async function getBnb(): Promise<bnbType[]> {
    const re=await getData("select bnb_amount from t_bnb");
    return re;
}

export async function checkNameAndSymbol({name,symbol}:any): Promise<any[]> {
    const re=await getData("select music_id from t_music where (start_time>0 or song_pre_id>0) and (token_name=? or token_symbol=?)",[name,symbol]);
    return re;
}


export async function getWhiteListById({songId}:any): Promise<any[]> {
  const re=await getData("select user_address from t_whitelist where song_id=?",[songId]);
  return re;
}

export async function getSubAmountById({songId}:any): Promise<any[]> {
  const re=await getData("SELECT IFNULL(SUM(sub_amount),0) t FROM t_music_sub WHERE song_id=? UNION ALL SELECT total_raise FROM t_music WHERE song_id=?",[songId,songId]);
  return re;
}


export async function getSubMusicAvatar({songId}:any): Promise<any[]> {
  const re=await getData("SELECT a.start_second,b.user_avatar FROM t_music_sub a JOIN t_user b ON a.user_address=b.user_address WHERE a.song_id=?",[songId]);
  return re;
}


export async function getWhitelistSub({songId}:any): Promise<any[]> {
  const re=await getData("SELECT a.user_address,IFNULL(b.sub,0) AS sub FROM t_whitelist a LEFT JOIN (SELECT user_address,SUM(sub_amount) sub FROM t_music_sub WHERE song_id=? GROUP BY user_address) b ON a.user_address=b.user_address WHERE a.song_id=?",[songId,songId]);

  return re;
}


export interface ArtistTotalType{
  music_count:number;
  sub_sum:number;
}

export async function getArtistTotal({did}:any): Promise<ArtistTotalType[]> {
  
  const re=await getData("SELECT COUNT(DISTINCT CASE WHEN m.start_time >0 AND s.song_id IS NOT NULL THEN m.song_id END) AS music_count,IFNULL(SUM(s.sub_amount),0) AS sub_sum FROM t_music m LEFT JOIN t_music_sub s ON m.song_id=s.song_id WHERE m.user_address=?",[did]);
  
  return re;
}


export async function getProfileTotal({did}:any): Promise<ArtistTotalType[]> {
  const re=await getData("SELECT  COUNT(DISTINCT song_id) AS music_count,IFNULL(SUM(sub_amount),0) AS sub_sum FROM t_music_sub WHERE user_address=?",[did]);
  return re;
}


export async function getPlayAll(): Promise<MusicType[]> {
  const re=await getData("SELECT * FROM v_play_all ORDER BY create_time DESC LIMIT 10",[]);
  return re;
}

export async function getPlayUser({did}:any): Promise<MusicType[]> {
  const re=await getData("SELECT * FROM v_play_user where user_address=? ORDER BY create_time DESC LIMIT 10",[did]);
  return re;
}


export async function getWhitePay({did,songId}:any): Promise<MusicType[]> {
  const re=await getData("SELECT IFNULL(SUM(sub_amount),0) t FROM t_music_sub WHERE song_id=? AND user_address=? UNION ALL SELECT IFNULL(SUM(sub_amount),0) t FROM t_music_sub WHERE song_id=?",[songId,did,songId]);
  return re;
}


export async function delMusic({did,musicId}:any): Promise<number> {
   return await execute("delete from t_play_user where user_address=? and music_id=?",[did,musicId]);
 
}

export interface SlatType{
  id:number;
  slat:string
}
export type QueryResult = [SlatType[], any];

export async function getSlat(): Promise<QueryResult> {
  return await getData("call get_and_use_slat()",[]);

}

;


export interface ListArtistType{
  user_address:string;
  artist_name:string;
  artist_avatar:string;
  twitter:string;
  facebook:string;
  tg:string;
  instgram:string;
  
}

export async function getPlayArtist(): Promise<any[]> {
  const re=await getData("SELECT a.user_address,a.artist_name,a.artist_avatar,a.twitter,a.facebook,a.tg,a.instgram FROM t_user a JOIN (SELECT user_address, COUNT(*) FROM t_music WHERE is_end=1 GROUP BY user_address ORDER BY COUNT(*) DESC LIMIT 6) b ON a.user_address=b.user_address",[]);
  return re;
}

export interface SalesType{
  user_address:string;
  sub_amount:number;
  hash_time:number;
  tx_hash:string;
}

export async function getSales({songId,did}:any): Promise<SalesType[]> {
  const re=await getData("SELECT user_address,sub_amount,hash_time,tx_hash from t_music_sub where user_address=? and song_id=?",[did,songId]);
  return re;
}


export async function getClaimed({songId,did}:any): Promise<SalesType[]> {
  const re=await getData("SELECT user_address,sub_amount,hash_time,tx_hash from t_music_sub where user_address=? and song_id=?",[did,songId]);
  return re;
}




const checkAddress = (v: string): boolean => /^0x[A-Fa-f0-9]{40}$/.test(v);
//
export async function messagePageData(params: any): Promise<any> {
  
  const { pi, type, w,account } = params;

  let sql='';
  let sqltotal='';

  switch(type) {

    case "In Progress":
      if(checkAddress(account)){ 
        sql=`select a.*,ifnull(b.sub_amount,0) sub_amount,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim from v_music a
        left join (SELECT song_id,SUM(sub_amount) AS sub_amount FROM t_music_sub WHERE user_address='${account}' GROUP BY song_id) b on a.song_id=b.song_id
        where a.song_id>0 and a.is_end=0 and (unix_timestamp()-a.start_time<${process.env.NEXT_PUBLIC_PUBLIC!}*60)`;
      }else 
      {
        sql=`select a.*,0 sub_amount,0 is_claim from v_music a where a.song_id>0 and a.is_end=0 and (unix_timestamp()-a.start_time<${process.env.NEXT_PUBLIC_PUBLIC!}*60)`;
      }
      sqltotal=`select count(*) c from v_music a where a.song_id>0 and a.is_end=0 and (unix_timestamp()-a.start_time<${process.env.NEXT_PUBLIC_PUBLIC!}*60)`;
      break;

    case "Ended":
      if(checkAddress(account)){ 
        sql=`select a.*,ifnull(b.sub_amount,0) sub_amount,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim from v_music a
        left join (SELECT song_id,SUM(sub_amount) AS sub_amount FROM t_music_sub WHERE user_address='${account}' GROUP BY song_id) b on a.song_id=b.song_id
        where a.is_end=1`;
      }else 
      {
        sql='select a.*,0 sub_amount,0 is_claim from v_music a where a.is_end=1';
      }
      sqltotal="select count(*) c from v_music a where  a.is_end=1";
      break;

    case "artist-In Progress":
      sql=`select a.*,0 sub_amount,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim from v_music a
      where a.user_address='${account}' and a.song_id>0 and a.is_end=0 and (unix_timestamp()-a.start_time<${process.env.NEXT_PUBLIC_PUBLIC!}*60)`;
      sqltotal=`select count(*) c from v_music a where a.user_address='${account}' and a.song_id>0 and a.is_end=0 and (unix_timestamp()-a.start_time<${process.env.NEXT_PUBLIC_PUBLIC!}*60)`;
      break;

      
    case "artist-Ended":
      sql=`select a.*,0 sub_amount,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim from v_music a
      where a.user_address='${account}' and a.is_end=1`;
      sqltotal=`select count(*) c from v_music a where a.user_address='${account}' and a.is_end=1`;
      break;
    case "artist-Upcoming":
        sql=`select a.*,0 sub_amount,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim from v_music a
        where a.user_address='${account}' and a.song_id=0`;
        sqltotal=`select count(*) c from v_music a where a.user_address='${account}' and a.song_id=0`;
        break;

      case "user-In Progress":
      
      sql=`SELECT b.*,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim,
a.token_id,a.user_address, a.music_id,a.music_name,a.music_url,a.music_seconds,a.token_name,a.token_symbol,a.token_logo,a.start_time,a.user_name,a.user_avatar,a.artist_name,a.artist_avatar,UNIX_TIMESTAMP() AS now_time 
FROM (SELECT song_id,SUM(sub_amount) sub_amount FROM t_music_sub  WHERE user_address='${account}' GROUP BY song_id) b 
JOIN ( SELECT b1.token_id,b1.user_address,b1.song_id, b1.music_id,b1.music_name,b1.music_url,b1.music_seconds,b1.token_name,b1.token_symbol,b1.token_logo,b1.start_time,b2.user_name,b2.user_avatar,b2.artist_name,b2.artist_avatar FROM t_music b1 
JOIN t_user b2 ON b1.user_address=b2.user_address WHERE b1.is_end=0) a ON b.song_id=a.song_id`;
sqltotal=`SELECT COUNT(*) AS c FROM t_music_sub s JOIN t_music m ON s.song_id = m.song_id WHERE s.user_address='${account}' AND m.is_end=0`;
      
      break;
      case "user-Ended":
        
        sql=`SELECT b.*,a.memetoken_address,CASE WHEN EXISTS (SELECT 1 FROM t_user_redeem WHERE t_user_redeem.song_id = a.song_id) THEN 1 ELSE 0 END AS is_claim,
  a.token_id,a.user_address, a.music_id,a.music_name,a.music_url,a.music_seconds,a.token_name,a.token_symbol,a.token_logo,a.start_time,a.user_name,a.user_avatar,a.artist_name,a.artist_avatar,UNIX_TIMESTAMP() AS now_time 
  FROM (SELECT song_id,SUM(sub_amount) sub_amount FROM t_music_sub  WHERE user_address='${account}' GROUP BY song_id) b 
  JOIN ( SELECT b1.token_id,b1.user_address,b1.song_id, b1.music_id,b1.music_name,b1.music_url,b1.music_seconds,b1.token_name,b1.token_symbol,b1.token_logo,b1.start_time,b2.user_name,b2.user_avatar,b2.artist_name,b2.artist_avatar,b1.memetoken_address FROM t_music b1 
  JOIN t_user b2 ON b1.user_address=b2.user_address WHERE b1.is_end=1) a ON b.song_id=a.song_id`;
  sqltotal=`SELECT COUNT(*) AS c FROM t_music_sub s JOIN t_music m ON s.song_id = m.song_id WHERE s.user_address='${account}' AND m.is_end=1`;
        
        break;

    default:  //upcoming
        sql=`select a.*,0 is_claim from v_music a where a.song_id=0`;
        sqltotal=`select count(1) c from v_music a where a.song_id=0`;
      break;
  }
  const ps=10;
   const str= w?`a.token_name LIKE '%${w}%' OR a.token_symbol LIKE '%${w}%'`:''
   const rows= await getData(`${sql}${str?' and '+str:''} order by music_id desc limit ${pi*ps},${ps}`, []);
   const re= await getData(`${sqltotal}${str?' and '+str:''}`, [],true);
   const total=re.c;

  return { rows,total,pages:Math.ceil(total/ps)};
}

  
export async function getMusicData({ ps, pi, searchText,states,userAddress }: any): Promise<any> {
    searchText = searchText?.replaceAll(' ', '');
    const re = await getPageData(
      'music',
      ps,
      pi,
      'music_id',
      'desc' ,
      geneW(states,searchText,userAddress)
    );
    return re;
  }

  const geneW=(states:string,searchText:string,userAddress:string)=>{
    const str= searchText?`token_name LIKE '%${searchText}%' OR token_symbol LIKE '%${searchText}%'`:''
    let stateStr=''
    switch(states){
        case "In Progress":
            stateStr='song_id>0 and is_end=0';
            break;
        case "Ended":
            stateStr='is_end=1';
            break;
        case "artist":
          stateStr=`user_address='${userAddress}'`;
          break;
        default:
            stateStr='song_id=0 and is_end=0';
            break; 
    }

    return str?`${stateStr} and ${str}`:stateStr;
  }

