"use client";
import React, {  useState } from "react";
import { useFetch } from "@/hooks/useFetch";
import { ArtistTotalType } from "@/lib/mysql/message";
import './styles/right.css'
import MusicParent from "@/components/home/MusicParent";
import SelectState from "../home/SelectState";
import { trimTrailingZeros } from "@/lib/utils/js";

interface PropsType{
  userAddress:string;
  isEdit?:boolean;
}

const Right=({userAddress,isEdit=true}:PropsType)=> {
  
    const { data, } = useFetch<ArtistTotalType[]>(`/api/getData?did=${userAddress}`,'getArtistTotal',[]);
    const [st, setSt] = useState<string>('In Progress'); 
    const [t,setT]=useState(0) ;// t Timestamp, for refreshing
  
    return (
    <> 
    <div className="mt-0 mb-3" style={{fontSize:'18px',color:'#FFF',fontWeight:700}} >Launched Details</div>
    {data && Array.isArray(data) && data.length>0 &&
        <div className="mb-2" style={{display:'flex',alignItems:'center',justifyContent:'space-between',gap:'20px'}}>
            <div  className="mole-artist-right mole-artist-right1 p-3">
                <div style={{color:'#FAFAFA',fontSize:'22px',fontWeight:600}} >{data[0].music_count}</div>
                <div style={{color:'#FAFAFA',fontSize:'12px',fontWeight:400}}>Tokenized</div>
            </div>
            <div  className="mole-artist-right mole-artist-right2 p-3">
                <div style={{color:'#FAFAFA',fontSize:'22px',fontWeight:600}} >{trimTrailingZeros(data[0].sub_sum.toString())} BNB</div>
                <div style={{color:'#FAFAFA',fontSize:'12px',fontWeight:400}}>Total Raised</div>
            </div>
        </div>
    
    }
    <SelectState st={st} setSt={setSt} setT={setT} stateAr={["In Progress","Upcoming","Ended"]}  />
    <MusicParent states={`artist-${st}`} t={t} isEdit={isEdit} userAddress={userAddress} />
      
    </>
  );
}

Right.displayName = "Right";
export default React.memo(Right);