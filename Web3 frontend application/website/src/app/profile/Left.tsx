
import { Button } from "react-bootstrap";
import React from "react";
import ImageWithFallback from "@/components/ImageWithFallback";
import { ProfileEdit } from "@/lib/jssvg/melosvg";

const Left=({user,setSt}:{user:MeloUserInfo,setSt:(v:string)=>void})=> {

    return (
    <>
        <div style={{display:'flex',flexDirection:'column',alignItems:'center',gap:'4px',position:'relative'}} >
            <ImageWithFallback src={user.user_avatar} alt=""  width={100} height={100} style={{borderRadius:'50%'}} />
            <div style={{fontSize:'18px',color:'#FFF',fontWeight:700}} >{user.user_name}</div>
            <div className="one-line-clamp"  style={{fontSize:'12px',color:'#718096',width:'100%'}} >{user.user_link}</div>
           
            <Button  onClick={()=>setSt('edit')}  variant="dark" style={{borderRadius:'50%',position:'absolute',top:'60px',right:'50px'}} >
                <ProfileEdit />
            </Button>
        </div>
        <div className="mt-2 mb-1" style={{color:'#323B4D',fontSize:'12px',fontWeight:700}}>Personal Introduction：</div>
        <div className="mt-2" style={{color:'#FFF',opacity:0.8,fontSize:'14px',fontWeight:400}} >{user.user_desc}</div>    
    </>
  );
}

Left.displayName = "Left";
export default React.memo(Left);