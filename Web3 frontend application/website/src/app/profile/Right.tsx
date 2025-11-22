

import React, { useState } from "react";
import { useFetch } from "@/hooks/useFetch";
import { ArtistTotalType } from "@/lib/mysql/message";
import '../artist/styles/right.css'
import MusicParent from "@/components/home/MusicParent";
import { trimTrailingZeros } from "@/lib/utils/js";
import SelectState from "../home/SelectState";

const Right=({user}:{user:MeloUserInfo})=> {
    const { data} = useFetch<ArtistTotalType[]>(`/api/getData?did=${user.user_address}`,'getProfileTotal',[]);
    const [st, setSt] = useState<string>('In Progress'); 
    const [t,setT]=useState(0) ;// t Timestamp, for refreshing

    return (
    <> 
    <div className="mt-2 mb-3" style={{fontSize:'18px',color:'#FFF',fontWeight:700}} >Participation  Details</div>
    {data && Array.isArray(data) && data.length>0 &&
        <div  className="mb-2" style={{display:'flex',alignItems:'center',justifyContent:'space-between',gap:'20px'}}>
            <div className="mole-artist-right mole-artist-right1 p-3">
                <div style={{color:'#FAFAFA',fontSize:'22px',fontWeight:600}} >{trimTrailingZeros(data[0].music_count)}</div>
                <div style={{color:'#FAFAFA',fontSize:'12px',fontWeight:400}}>Total Participated</div>
            </div>
            <div  className="mole-artist-right mole-artist-right2 p-3">
                <div style={{color:'#FAFAFA',fontSize:'22px',fontWeight:600}} >{trimTrailingZeros(data[0].sub_sum)} BNB</div>
                <div style={{color:'#FAFAFA',fontSize:'12px',fontWeight:400}}>Total Invested</div>
            </div>
            <div  className="mole-artist-right mole-artist-right3 p-3">
                <div style={{color:'#FAFAFA',fontSize:'22px',fontWeight:600}} >{trimTrailingZeros((data[0].sub_sum*100).toFixed(2))} Points</div>
                <div style={{color:'#FAFAFA',fontSize:'12px',fontWeight:400}}>B Points</div>
            </div>

         
        </div>
    
    }
    <SelectState st={st} setSt={setSt}  setT={setT} stateAr={["In Progress","Ended"]} />
    <MusicParent states={`user-${st}`} t={t}  />
    
      
    </>
  );
}

Right.displayName = "Right";
export default React.memo(Right);