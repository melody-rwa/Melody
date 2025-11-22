
import { Button } from "react-bootstrap";
import './styles/left.css'
import React from "react";
import ArtistLink from "@/components/ArtistLink";
import ImageWithFallback from "@/components/ImageWithFallback";
import { ProfileEdit } from "@/lib/jssvg/melosvg";

interface PropsType{
  user:UserInfo;
  setSt?:(v:string)=>void;
  
}

const Left=({user,setSt}:PropsType)=> {

    return (
    <>
        <div style={{display:'flex',flexDirection:'column',alignItems:'center',gap:'4px',position:'relative'}} >
            <ImageWithFallback src={user.artist_avatar} alt=""  width={100} height={100} style={{borderRadius:'50%'}} />
            <div style={{fontSize:'18px',color:'#FFF',fontWeight:700}} >{user.artist_name}</div>
            <div className="one-line-clamp" style={{fontSize:'12px',color:'#718096',width:'100%'}} >{user.artist_link}</div>
            <div className="mt-2 mb-2" style={{display:'flex',alignItems:'center',gap:'20px',justifyContent:'space-between',width:'100%'}} >
              <ArtistLink url={user.twitter} index={0} />
              <ArtistLink url={user.facebook} index={1} />
              <ArtistLink url={user.tg} index={2} />
              <ArtistLink url={user.instgram} index={3} />
            </div>
           {setSt && <Button  onClick={()=>setSt('edit')}  variant="dark" style={{borderRadius:'50%',position:'absolute',top:'60px',right:'50px'}} >
                <ProfileEdit />
            </Button>
            }
        </div>
        <div className="mt-2" style={{color:'#FFF',opacity:0.8,fontSize:'12px',fontWeight:700}} >{user.artist_desc}</div>
    </>
  );
}

Left.displayName = "Left";
export default React.memo(Left);

