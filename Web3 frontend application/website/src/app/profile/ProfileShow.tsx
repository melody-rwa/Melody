

import React from "react";
import Left from "./Left";
import Right from "./Right";

const ProfileShow=({user,setSt}:{user:MeloUserInfo,setSt:(v:string)=>void})=> {

  return (

    <div style={{display:'flex',flexDirection:'column',height:'100%'}} > 
      <div className="mole-profile-top top-text pt-4 pb-3 px-4 mb-1 " >Personal Proflie</div>
      <div  style={{  flex: 1, display: 'flex'}} >
        <div className="mt-1 p-3" style={{width:'248px',background:'#111',color:'#FFF',overflowY:'auto',flexShrink:0}} >
        <Left user={user} setSt={setSt} />
        </div>
        <div className="ms-3 p-3 mt-1"  style={{height:'100%', background:'#111',color:'#FFF',borderRadius:'4px', flex:1,overflowX:'auto'}} >
          <Right user={user} />
        </div>
      </div>
    </div>
  );
}

ProfileShow.displayName = "ProfileShow";
export default React.memo(ProfileShow);