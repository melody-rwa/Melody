"use client";
import PlayItem from "@/components/sidemenu/PlayItem";
import { useLayout } from "@/contexts/LayoutContext";

export default function Profile() {
  
    const {musicList}=useLayout();
 
  return (
    <div style={{display:'flex',flexDirection:'column',height:'100%'}} > 
    <div className="mole-profile-top top-text mt-0 mb-1 px-3" >Playlist</div>
    <div  style={{  flex: 1, display: 'flex',flexDirection:'column', flexWrap:'wrap', overflow: 'hidden',gap:'20px',paddingLeft:'20px' }} >
        {
          musicList.map((item,idx)=>(
            <PlayItem  key={`more_side_${idx}`} item={item} idx={idx} width="320px" />
           
          ))
        }
    </div>
  </div>
    
  );
}
