
export {};

declare global {

  interface Window {
    ethereum?: any;
  }

  
interface UserInfo{
  user_address:string;
  user_type:number;
  user_name:string;
  user_avatar:string,
  user_desc:string;
  user_link:string;
  twitter:string;
  tg:string;
  facebook:string;
  instgram:string;
  artist_avatar:string;
  artist_name:string;
  artist_desc:string;
  artist_link:string;
}



  interface MeloUserInfo extends UserInfo {
    connected: number;
    networkName: string;
    chainId: number;
  }

 

interface WalletProviderInfoType {
    uuid: string;
    name: string;
    icon: string;
    rdns: string;
  }

interface WalletProviderType {
    info: WalletProviderInfoType;
    provider: any; 
  }

  interface WhiteType{
    user_address: string;
  }
  
  

  interface MusicType{
    user_address:string; 
    user_name:string; 
    user_avatar:string; 
    music_id:number;  
    song_id:number; 
    music_name:string; 
    music_url:string; 
    music_seconds:number;
    token_id:number; 
    token_name:string;
    token_symbol:string;
    token_logo:string;
    now_time:number; 
    create_time:number; 
    start_time:number; 

    artist_name:string;
    artist_avatar:string;
    artist_desc:string;


    total_raise?:number;
    token_desc?:string;
    website?:string;
    twitter?:string;
    telegram?:string;
    series_address?:string; 
    memetoken_address?:string; 
    is_end?:number;  
    total_sub_amount?:number; 
    sub_amount?:number;
    is_claim?:number; 
    

  }

}

