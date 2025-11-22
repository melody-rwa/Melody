"use client";

import {Button} from "react-bootstrap";
import Image from "next/image";
import { useLayout } from "@/contexts/LayoutContext";
import { stopEvent } from "@/lib/utils/eventUtils";

interface PropType{
    item:MusicType;
    idx:number;
    width?:string;
}

export default function PlayItem({item,idx,width='100%'}:PropType ) {
  const {currentIndex,audioRef,musicList, isPlaying,setMusicList,setSelectedRow,setSelectedLeftRow,musicObj}=useLayout();

 const delMusic=(idx:number)=>{

    fetch("/api/postwithsession", {
        method: 'POST',
        headers: { 'x-method': 'delMusic' },
        body: JSON.stringify({did:musicList[idx].user_address,musicId:musicList[idx].music_id})
    });

    setMusicList([...musicList.slice(0, idx), ...musicList.slice(idx + 1)]);
 }
 const trSelect=(idx:number)=>{
    setSelectedLeftRow(null);
    setSelectedRow(null); 
    if(currentIndex===idx && isPlaying){
       audioRef.current?.stopPlay()
    } else {
        audioRef.current?.playIndex(idx);
    }
    
 }

  return ( 
 
    <div className='mt-2 melo-play-item' onClick={()=>trSelect(idx)} style={{width}} >
        <div className="d-flex justify-content-start align-items-center gap-1">
            <Image alt='' src="/playlist.svg" width={40} height={40} />
            <div  className="one-line-clamp melo-playlist-text"  >{item.token_name}</div>
        </div>
        {musicObj?.music_id===musicList[idx].music_id && <Image alt='' src={isPlaying?"/m_pause.svg":"/m_play.svg"} width={24} height={24} />}
        <div>
        <Button style={{background:'transparent',border:0}}  variant='dark'  onClick={(e:any)=>{stopEvent(e); delMusic(idx);}}  >
            <Image alt='' src="/clear.svg" width={24} height={24} />
        </Button>
        </div>
    </div>
       
  );
}
