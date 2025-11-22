"use client";
import React, { useState, useRef, forwardRef, useImperativeHandle } from 'react';
import { Form } from 'react-bootstrap';
import Image from 'next/image';
import MusicDetail from './MusicDetail';
import { formatTime } from '@/lib/utils/js';
import { useLayout } from '@/contexts/LayoutContext';
import { useSelector } from 'react-redux';
import { RootState } from '@/store/store';

export interface MusicPlayerHandle {
  togglePlay: () => void;
  playNext: () => void;
  playPrev: () => void;
  stopPlay:()=>void;
  setCurrentIndex:(v:number)=>void;
  playIndex: (index: number) => void;  
}

const MusicPlayer = forwardRef<MusicPlayerHandle>((_,ref) => {
    const audioRef = useRef<HTMLAudioElement | null>(null);
    const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
    const [isMuted, setIsMuted] = useState(false);
    const [volume, setVolume] = useState(1);
    const [isRandom, setIsRandom] = useState(false);
    const [isLoop, setIsLoop] = useState(false);
    const [pressed, setPressed] = useState(false);
    const [pressed1, setPressed1] = useState(false);
  
    const {currentTime, setCurrentTime,musicList,isPlaying,setMusicObj, setIsPlaying,currentIndex,setCurrentIndex,setSelectedLeftRow,setSelectedRow}=useLayout();

    const saveMusic=(index:number)=>{
      if(user.connected===1 &&  musicList[index] && musicList[index].music_id)
        fetch("/api/allplaylist", {
          method: "POST",
          headers: {
            "Content-Type": "application/json", 
          },
          body: JSON.stringify({
            did:user.user_address,   
            musicId: musicList[index].music_id,
          }),
          })
    }
 
    const togglePlay = () => {
      if (!audioRef.current) return;
      if (isPlaying) {
        setIsPlaying(false);
        audioRef.current.pause();
      } else {
        setIsPlaying(true);
        setTimeout(() => {
          audioRef.current?.play().catch(err => console.warn("Play error:", err));
          saveMusic(currentIndex);
        }, 100);
      
      }
      
    };

    const stopPlay = () => {
      if (!audioRef.current) return;
      if (isPlaying) {
        audioRef.current.pause();
      } 
      setIsPlaying(false);
    };
  
    const playEnd=()=>{
    
      if (isRandom) playNext();
      else  setIsPlaying(false);
      
    }
    const playNext = () => {
      if(musicList.length < 2) return;
      let nextIndex = currentIndex;
      // if (isRandom) {
      //   nextIndex = Math.floor(Math.random() * musicList.length);
      // } else {
        nextIndex = (currentIndex + 1) % musicList.length;
      // }
      setCurrentIndex(nextIndex);
      setIsPlaying(true);
      setMusicObj(musicList[nextIndex]);
      saveMusic(nextIndex);
      setSelectedRow(null);
      setSelectedLeftRow(null);
    };
  
    const playPrev = () => {
      if(musicList.length < 2) return;
      let prevIndex = currentIndex;
      // if (isRandom) {
      //   prevIndex = Math.floor(Math.random() * musicList.length);
      // } else {
        prevIndex = (currentIndex - 1 + musicList.length) % musicList.length;
      // }
      setCurrentIndex(prevIndex);
      setIsPlaying(true);
      setMusicObj(musicList[prevIndex]);
      saveMusic(prevIndex);
      setSelectedRow(null);
      setSelectedLeftRow(null);
      
    };
  
    const toggleMute = () => {
      if (!audioRef.current) return;
      audioRef.current.muted = !isMuted;
      setIsMuted(!isMuted);
    };
  
    const handleVolumeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      const vol = parseFloat(e.target.value);
      if (audioRef.current) audioRef.current.volume = vol;
      setVolume(vol);
      if (vol === 0) setIsMuted(true);
      else setIsMuted(false);
    };
  
    const toggleRandom = () => {
      setIsRandom(prev=>(!prev));
      if(!isRandom){
        setIsLoop(false);
      }

    }
    const toggleLoop = () => {
      if (!audioRef.current) return;
      audioRef.current.loop = !isLoop;
      setIsLoop(!isLoop);

      if(!isLoop) {
        if(isRandom) setIsRandom(false);
      }
      
    };
  
    const handleTimeUpdate = () => {
      if (!audioRef.current) return;
      setCurrentTime(audioRef.current.currentTime);
    };

     const playIndex = (index: number) => {
      if (index < 0 || index >= musicList.length) return;
      setCurrentIndex(index);
      setIsPlaying(true);
      setTimeout(() => {
        audioRef.current?.play().catch((err) =>
          console.warn("PlayIndex play error:", err)
        );
        setMusicObj(musicList[index]);
      }, 0);
    };

    
    useImperativeHandle(ref, () => ({
      togglePlay,
      playNext,
      playPrev,
      playIndex,stopPlay,setCurrentIndex
    }));

  const handleSeek = (e: React.ChangeEvent<HTMLInputElement>) => {
    const time = Number(e.target.value);
    if (audioRef.current) {
      audioRef.current.currentTime = time;
      setCurrentTime(time);
    }
  };

  return (
  <>
        <MusicDetail  musicObj={musicList[currentIndex]||null}/>

        <div className="melo-flex-start" >
        <button disabled={musicList.length<2}  onClick={toggleRandom}  className="melo-btn">
        <Image alt='' src={isRandom? "/shuffle.svg":"m_repeatl.svg"} width={24} height={24} />
        </button>
        <button className="melo-btn btn-prev"  onClick={playPrev}>
          <Image className="icon normal" alt="" src="/m_backward.svg" width={24} height={24} />
          <Image className="icon pressed" alt="" src="/m_backward1.svg" width={24} height={24} />
        </button>

        {/* <button disabled={musicList.length<2} onClick={playPrev} className="melo-btn"   onPointerDown={() => setPressed(true)}
      onPointerUp={() => setPressed(false)}
      onPointerLeave={() => setPressed(false)}>
        <Image alt='' src={pressed?"/m_backward1.svg":"/m_backward.svg"} width={24} height={24} />
        </button> */}
        <button  disabled={musicList.length===0} onClick={togglePlay}  className="melo-btn"  >
        <Image alt='' src={isPlaying?"/m_pause.svg":"/m_play.svg"} width={48} height={48} />
        </button>
        {/* <button  disabled={musicList.length<2} onClick={playNext}  className="melo-btn"  
        onPointerDown={() => setPressed1(true)}
        onPointerUp={() => setPressed1(false)}
        onPointerLeave={() => setPressed1(false)}> 
        <Image alt='' src={pressed1?"/m_forward1.svg":"/m_forward.svg"} width={24} height={24} />
        </button> */}
        <button className="melo-btn btn-prev"  onClick={playNext}>
          <Image className="icon normal" alt="" src="/m_forward.svg" width={24} height={24} />
          <Image className="icon pressed" alt="" src="/m_forward1.svg" width={24} height={24} />
        </button>

        <button  disabled={musicList.length===0}  onClick={toggleLoop}  className="melo-btn" >   
        <Image alt='' src={isLoop?"repeate-one.svg": "/m_repeatr.svg"} width={24} height={24} />
        </button>

        <input
            type="range"
            min={0}
            max={musicList[currentIndex]?.music_seconds || 0}
            value={currentTime}
            step={1}
            onChange={handleSeek}
            style={{
                background: `linear-gradient(to right, #ff8c4c ${(currentTime / (musicList[currentIndex]?.music_seconds || 1)) * 100}%, #343A40 ${(currentTime / (musicList[currentIndex]?.music_seconds || 1)) * 100}%)`,
                height: "6px",
                borderRadius: "3px",
                appearance: "none",
                width:'120px',
                marginLeft:'18px'
              }}
          />

        </div>

     
        
        <div className="melo-flex-start" >
           <div className="melo-vo-text" >{formatTime(currentTime)}</div>
           <div className="melo-vo-text px-2">/</div>
            <div className="melo-vo-text" >{formatTime(musicList[currentIndex]?.music_seconds??0)}</div>
            <button  onClick={toggleMute} className="melo-btn" >
                <Image alt='' src={isMuted?"/mr_volume0.svg":(isPlaying?"/playing.gif":"/mr_volume.svg")} width={24} height={24} />
            </button>
            <Form.Range  value={volume} min={0} max={1} step={0.01} onChange={handleVolumeChange} 
            className="custom-range ms-3 me-3 align-self-center" 
            style={{ ["--progress" as any]: `${volume * 100}%`, width: "100px" }}
             /> 
           
            <Image alt='' src="/mr_filter.svg" width={24} height={24} />
        </div>
        

   {musicList.length>0 && <audio ref={audioRef} src={musicList[currentIndex].music_url||'/'} onEnded={playEnd} onTimeUpdate={handleTimeUpdate}
        onLoadedMetadata={() => {
          if (audioRef.current) { setCurrentTime(0); }
          if (isPlaying) {
            audioRef.current?.play().catch(err => console.warn("Autoplay failed:", err));
          }
        }}
      />}
    </>
  );
});

MusicPlayer.displayName = "MusicPlayer"; 
export default MusicPlayer;
