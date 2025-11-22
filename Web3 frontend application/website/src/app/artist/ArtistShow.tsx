

"use client";
import React from "react";
import Left from "./Left";
import Right from "./Right";

interface PropsType{
  user:UserInfo;
  setSt?:(v:string)=>void;
  isEdit?:boolean;
}

const ArtistShow=({user,setSt,isEdit=true}:PropsType)=> {
  return (

    <div style={{display:'flex',flexDirection:'column',height:'100%'}} > 
      <div className="top-text  p-3 artist-ratio " >Artist Profile</div>
      <div  style={{  flex: 1, display: 'flex'}} >
        <div className="mt-2 p-3" style={{width:'248px',background:'#111',color:'#FFF',overflowY:'auto',flexShrink:0}} >
          <Left user={user} setSt={setSt}  />
        </div>
        <div className="ms-3 p-3 mt-1"  style={{background:'#111',color:'#FFF',borderRadius:'4px', flex:1,overflowX:'auto'}} >
          <Right userAddress={(user.user_address).toLowerCase()} isEdit={isEdit} />
        </div>
      </div>
    </div>
  );
}
 

ArtistShow.displayName = "ArtistShow";
export default React.memo(ArtistShow);