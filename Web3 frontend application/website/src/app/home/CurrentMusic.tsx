"use client";

import Image from "next/image";
import React, { RefObject } from "react";
import { useEffect, useRef, useState } from "react";
import './styles/currentmusic.css'
import NoteBar from "./NoteBar";
import NoteBarBegin from "./NoteBarBegin";
import StateBar from "./StateBar";
import ImageWithFallback from "@/components/ImageWithFallback";
import { Button } from "react-bootstrap";
import { MusicPlayerHandle } from "@/components/footer/MusicPlayer";


interface MusicAvatarType{
  start_second:number;
  user_avatar:string;
}

interface PropType{
  musicObj:MusicType|null;
  isPlaying:boolean;
  audioRef: RefObject<MusicPlayerHandle|null>;
  currentTime:number;

}

const CurrentMusic=({musicObj,isPlaying,audioRef,currentTime}:PropType)=> {
  const [width, setWidth] = useState(0); //area width
  const [avatar,setAvartar]=useState<MusicAvatarType[]>([]) //Subscriber avatar
  const [bars, setBars] = useState<number[]>([]);
  const timerRef   = useRef<NodeJS.Timeout | null>(null);
  const divRef = useRef<HTMLDivElement>(null);   //calculate width
  const duration=musicObj?.music_seconds??0;
  const progressPercent = duration ? (currentTime / duration) * 100 : 0;

  useEffect(() => {
    if (divRef.current) {
      setWidth(divRef.current.offsetWidth); // Get the current width of the div in pixels
      const len=Math.ceil(divRef.current.offsetWidth/4)+1;
      setBars(Array.from({ length: len }, (_, i) => i));
      timerRef.current = setInterval(() => {
        setBars(prev => [...prev,performance.now()].slice(-len));
      }, 500);
    }

    return () => {  
      if(timerRef.current)  clearInterval(timerRef.current);
    };
  }, []);

  useEffect(() => {
    if (!musicObj || !musicObj.music_id) return;
    const controller = new AbortController();
    const fetchData = async (songId: number) => {
      try {
        const res = await fetch(`/api/getData?songId=${songId}`, {
          signal: controller.signal,
          headers: { "x-method": "getSubMusicAvatar" },
        });

        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);

        const data: MusicAvatarType[] = await res.json();
        setAvartar(data);
      } catch (err: any) {
        if (err.name !== "AbortError") {
          console.error("Fetch error:", err?.message|| err);
        }
      }
    };

    fetchData(musicObj.song_id);

    return () => controller.abort();
  }, [musicObj]);

  return ( <>
    <div ref={divRef}  className="mt-3 mole-bg-music" style={{display:'flex',alignItems:'center',justifyContent:'space-between'}}  >
      {/* left */}
      <div style={{position:'relative',height:'75px'}} >
      <Image style={{position:'absolute',borderRadius:'50%',left:30}}  src={musicObj?.token_logo??'/favicon.ico'} width={75} height={75} alt="" />

        <div className="melo-home-info" style={{marginTop:'8px'}} >
            <div style={{paddingTop:'8px'}} className="melo-home-info-1" >{musicObj?.token_name}</div>
            <div className="melo-home-info-2" >{musicObj?.artist_name}</div>
        </div>

      </div>

      {/* center */}
      {musicObj &&  
        <div>
          {/* <span style={{fontSize:'1.2rem',color:'#FFF',marginRight:'12px'}} >{formatTime(current)}</span> */}
          <Button style={{background:'transparent',border:0}} onClick={()=>audioRef.current?.togglePlay()}>
          <Image alt='' src={isPlaying?"/m_pause.svg":"/m_play.svg"} width={48} height={48} />
          </Button>
          {/* <span style={{fontSize:'1.2rem',color:'#FFF',marginLeft:'12px'}} >{formatTime(duration)}</span> */}
        </div>
      }

      {/* right */}
      {musicObj && <StateBar musicObj={musicObj} />}
    
      </div>

      <div style={{width:'100%',height:'70px',position:'relative'}} >
          <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            height: "40px",
            width: `${progressPercent}%`,
            background:'rgba(255, 255, 255, 0.10)', // 
            transition: "width 0.1s linear",
          }}
          />
     
          <div   className="d-flex align-items-end  w-100" 
            style={{height:'40px',overflowX:'hidden',gap:'1px', whiteSpace: "nowrap",background:'rgba(255, 255, 255, 0.20)'}}>
            {bars.map((t, i) => (
              <div key={i}>
              {isPlaying? <NoteBar  birthTime={t} />:<NoteBarBegin />}
              </div>
            ))}
          </div>

          {/* Display subscription avatar */}
          {avatar.map((item,idx)=>(
           <div style={{ position:'absolute',top:20,left:(width/(musicObj?.music_seconds??200)*(item.start_second/10000))}}  
            key={`avvatar${idx}`}>
             {/* <Image style={{borderRadius:'50%'}} width={30} height={30} alt="" src={item.user_avatar} />  */}
             <ImageWithFallback src={item.user_avatar} alt="" fallback="/user.svg" width={30} height={30} style={{borderRadius:'50%'}} />
           </div>))
          }
      </div>
   
      {/* <audio ref={audioRef} src={musicObj?.music_url??'/'} onTimeUpdate={updateTime}  onLoadedMetadata={handleLoadedMetadata} onEnded={withEnd} /> */}
    </>
  );
}
CurrentMusic.displayName="CurrentMusic";
export default React.memo(CurrentMusic);

// 

