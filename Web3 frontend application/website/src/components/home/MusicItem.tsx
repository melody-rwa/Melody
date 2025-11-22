"use client";

import ImageWithFallback from "../ImageWithFallback";
import Deposit from "./Deposit";
import WhiteAdd from "./WhiteAdd";
import Claim from "./Claim";
import { trimTrailingZeros } from "@/lib/utils/js";
import Lp from "./Lp";
import { useDispatch } from "react-redux";
import { setErrText, setMessageText, setTipText } from "@/store/store";
import Fees from "./Fees";
import CopyWorld from "../CopyWorld";
import React from "react";
import Info from "./Info";
import Image from "next/image";
import { useLayout } from "@/contexts/LayoutContext";
import Link from "next/link";

interface SelectableRowProps {
  musicObj:MusicType;
  index: number;
  onSelect: (index: number) => void;
  states:string;
  user:MeloUserInfo;
  isEdit?:boolean;
  refetch:()=>void;
  
}

const MusicItem: React.FC<SelectableRowProps> = ({ musicObj, index, onSelect,states,user,refetch,isEdit=true }) => {

    const { selectedRow,setMusicAndpush } = useLayout(); 
    const dispatch = useDispatch();
    const showError = (str: string) => dispatch(setErrText(str));
    const showTip = (str: string) => dispatch(setTipText(str));
    const closeTip = () => dispatch(setTipText(''));
    const showMessage= (str: string) => dispatch(setMessageText(str));
    const {canSelect } = useLayout(); 

    const trSelect=()=>{
      if(canSelect) { 
        onSelect(index);
        setMusicAndpush(musicObj)
      // setMusicObj(musicObj);
    }
    }

    
    const geneEndTime=()=>{
      if(musicObj.start_time && musicObj.song_id>0) 
        return new Date(new Date(musicObj.start_time*1000).getTime() + (Number(process.env.NEXT_PUBLIC_PUBLIC)) * 60 * 60 * 1000).toLocaleDateString("en-US", {month: "long",day:"numeric",year: "numeric"});
      else  return "upcoming"
    }
   
    const geneStartTime=()=>{   
      if(musicObj.start_time && musicObj.song_id>0) 
        return new Date(musicObj.start_time*1000).toLocaleDateString("en-US", {month: "long",day:"numeric",year: "numeric"});
      else  return "upcoming"
    }
    
  
  return (
    <tr  className={`custom-row ${selectedRow===index ? "selected" : ""}`} onClick={trSelect}>
      {/* music */}
        <td >
            <div className="melo-flex-start gap-2" style={{width:'160px',flexShrink:0}}  >
                <ImageWithFallback src={musicObj.token_logo} alt=""  width={40} height={40}  style={{borderRadius:'10px'}} />
                <div  className="melo-grid-text text-ellipsis-2" >{musicObj.token_name}</div>
            </div>
    
        </td>

        {/* token symbol */}
        <td>
          <div style={{width:'100px',flexShrink:0,overflow:'hidden'}}>
          <CopyWorld copyStr= {musicObj?.memetoken_address||musicObj.token_symbol} showStr={musicObj.token_symbol} />
          </div>
          </td>

        {/* start time */}
        {states.startsWith('artist') && <td >
         
          <div style={{minWidth:'100px',width:'100%',textAlign:'center',flexShrink:0,overflow:'hidden',paddingLeft:'10px',paddingRight:'10px'}} >{geneStartTime()}</div>
          </td>}

        {/* artist */}
       {!states.startsWith('artist') && <td>
          <div className="melo-flex-start gap-1" style={{width:'100px',flexShrink:0,overflow:'hidden'}} >
            <ImageWithFallback src={musicObj.artist_avatar} alt="" width={32} height={32} style={{borderRadius:'50%'}} />
            <div className="melo-grid-text text-ellipsis-2" >{musicObj.artist_name}</div>
        </div>
        </td>
      }

        {/* total meme token */}
        {(!states.startsWith('artist') &&!states.startsWith('user')) &&  <td>
          <div style={{minWidth:'100px',width:'100%',flexShrink:0,overflow:'hidden',textAlign:'center'}} >
         {musicObj.music_seconds} M
        </div></td>
        }

         {/* Task completion volume */}
         {!states.startsWith('user') &&  <td>
         <div  style={{minWidth:'100px',width:'100%',flexShrink:0,overflow:'hidden'}}>
            <div style={{textAlign:'center',width:'100%'}}  >{`${(parseFloat((musicObj?.total_sub_amount??0).toString())/(parseFloat((musicObj?.total_raise??1).toString()))*100).toFixed(2)}%`}</div>
            <div  style={{textAlign:'center',fontSize:'0.8em',flexShrink:0,width:'100%'}} >({`${trimTrailingZeros(Number(musicObj?.total_sub_amount??0).toFixed(2))}BNB/${trimTrailingZeros(musicObj?.total_raise??0)}BNB`})</div>
            </div>
           
        </td>}

        {/* end time */}
        {!states.startsWith('artist') && !states.startsWith('user')  && <td  >
          <div style={{minWidth:'100px',width:'100%',textAlign:'center',flexShrink:0,overflow:'hidden',paddingLeft:'10px',paddingRight:'10px'}} >{geneEndTime()}</div>
        
        </td>}
          {/* Invested */}
        {states.startsWith('user') && <td>
          <div  style={{minWidth:'100px',width:'100%',flexShrink:0,overflow:'hidden',textAlign:'center'}}>
            {trimTrailingZeros(musicObj?.sub_amount??0)} BNB
            </div></td>}

        {/* user Allocation */}
        {states.startsWith('user') && <td>
            <div style={{width:'100%',minWidth:'100px',flexShrink:0,display:'flex',justifyContent:'center',alignContent:'center'}}   >
              {/* <div style={{minWidth:'100px',width:'100%',flexShrink:0, textAlign:'center'}} >{trimTrailingZeros(Number(musicObj?.bnb_token??0).toFixed(2))} BNB</div>
              <div style={{minWidth:'100px',width:'100%',flexShrink:0, textAlign:'center'}}>{trimTrailingZeros(Number(musicObj?.meme_token??0).toFixed(2))} {musicObj.token_symbol}</div> */}
             {musicObj.is_claim===1? <Link href={process.env.NEXT_PUBLIC_BSC_URL!} target="_blank" >
             <Image src="/httpto.svg" width={24} height={24} alt=""  /> 
             </Link>:"/"}
            </div>
          </td>}


        {/* Participate  */}
        {!states.startsWith('artist') && !states.startsWith('user') && <td  >
          {/* {trimTrailingZeros(musicObj?.sub_amount??0)} */}
          <div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}>
          <Deposit musicObj={musicObj}  user={user} showError={showError} showMessage={showMessage} showTip={showTip} closeTip={closeTip} refetch={refetch} /> 
         </div>
          </td>
        }
       

        {/* action   {/*  LP */}
        {!states.startsWith('artist') && <td  >
        <div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}> <Claim musicObj={musicObj} refetch={refetch}  user={user} showError={showError} showMessage={showMessage} showTip={showTip} closeTip={closeTip} /> 
        </div>
        </td>}

        {states.startsWith('user') && <td >
        <div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}> <Info songId={musicObj.song_id} user_address={user.user_address} />
        </div>
          </td>}

        {states.startsWith('artist') && isEdit &&
          <> 
          <td><div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}><WhiteAdd musicObj={musicObj}  showError={showError} showMessage={showMessage} showTip={showTip} closeTip={closeTip} /></div></td>
          <td><div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}><Lp musicObj={musicObj} user={user} showError={showError} showMessage={showMessage} showTip={showTip} closeTip={closeTip} /></div></td>
          <td><div style={{width:'100%',minWidth:'100px',flexShrink:0,textAlign:'center'}}><Fees musicObj={musicObj} user={user} showError={showError} showMessage={showMessage} showTip={showTip} closeTip={closeTip} /></div></td>
          </>
        }


    </tr>
  )
};




MusicItem.displayName="MusicItem";
export default React.memo(MusicItem);

