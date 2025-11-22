'use client';

import React, { useRef, useState, useEffect } from 'react';
import './styles/musicupload.css';
import { Music_upload } from '@/lib/jssvg/melosvg';
import NoteBar from '../home/NoteBar';
import NoteBarBegin from '../home/NoteBarBegin';
import Image from 'next/image';
import { Button } from 'react-bootstrap';
import { useLayout } from "@/contexts/LayoutContext";

interface MusicUploaderProps {
  onUpload?: (file: File|null, duration: number) => void; 
  showError:(v:string)=>void;
  musicURL:string|null;
  setMusicURL:(v:string|null)=>void;
}

const MusicUpload=({musicURL,setMusicURL, onUpload,showError }: MusicUploaderProps)=> {

  const [musicFile, setMusicFile] = useState<File | null>(null);
  const [bars, setBars] = useState<number[]>([]);
  
  const audioRefup = useRef<HTMLAudioElement | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [dragActive, setDragActive] = useState<boolean>(false);
  
  const timerRef   = useRef<NodeJS.Timeout | null>(null);
  const divRef = useRef<HTMLDivElement>(null);   
  const {audioRef}=useLayout();
  
  useEffect(() => {
    if (divRef.current) {
      const len=Math.floor(divRef.current.offsetWidth/4+20)
      // setBarAmount(len);
      setBars(Array.from({ length: len }, (_, i) => i));
      timerRef.current = setInterval(() => {
        setBars(prev => [...prev,performance.now()].slice(-len));
      }, 500);
    }

    return () => { 
      if(timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

const handleFile = (selectedFile: File) => {
  if (!selectedFile) return;
  if (!selectedFile.type.startsWith('audio/')) {
    showError('Launch&$&You must select an audio file.');
    handleClear(); 
    return;
  }
  if (selectedFile.size > 100 * 1024 * 1024) {
    showError('Launch&$&File size exceeds 100MB');
    return;
  }

  setMusicFile(selectedFile);

  if (audioRefup.current) {
    audioRefup.current.pause();
    audioRefup.current.currentTime = 0;
  }

  if (musicURL) URL.revokeObjectURL(musicURL);

  const url = URL.createObjectURL(selectedFile);
  setMusicURL(url);

};

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0] ?? null;
    if (!file) return;

    if (!file.type.startsWith('audio/')) {
      showError('Launch&$&You must select an audio file.');
      e.target.value = ''; 
      return;
    }

    if (file.size > 100 * 1024 * 1024) {
      showError('Launch&$&File size exceeds 100MB');
      return;
    }

    setMusicFile(file);

    if (audioRefup.current) {
      audioRefup.current.pause();
      audioRefup.current.currentTime = 0;
    }

    if (musicURL) URL.revokeObjectURL(musicURL);

    const url = URL.createObjectURL(file);
    setMusicURL(url);
    
  };

  const handleClear = () => {
    setMusicFile(null);

    if (audioRefup.current) {
      audioRefup.current.pause();
      audioRefup.current.currentTime = 0;
    }

    if (musicURL) URL.revokeObjectURL(musicURL);

 
    setMusicURL(null);
    
    if (onUpload) {
      onUpload(null, 0);
    }
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(true);
  };
  const handleDragLeave = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(false);
  };
  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const handleLoadedMetadata = (e: React.SyntheticEvent<HTMLAudioElement, Event>) => {
    if(!e.currentTarget) return;
    const target = e.currentTarget;
    const duration = Math.floor(target.duration);

    if (musicFile && onUpload) {
      onUpload(musicFile, duration);
    }
    audioRef.current?.stopPlay();
    setIsPlaying(true);
    target.play().catch((err) => console.warn('Auto-play failed:', err));
  };

  useEffect(() => {
    return () => {
      if (musicURL) URL.revokeObjectURL(musicURL);
    };
  }, [musicURL]);

  
   
  const togglePlay = () => {
    if (!audioRefup.current) return;
    if (isPlaying) {
      audioRefup.current.pause();
    } else {
      audioRefup.current.play().catch(err => console.warn("Play error:", err));
    }
    setIsPlaying(prev=>!prev);
  };
   
const withEnd=()=>{setIsPlaying(false)}
  return (
    <div ref={divRef}
      className={`upload-box ${dragActive ? 'drag-active' : ''}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}

      
    >
     {!musicFile && <input type="file" id="musicFile"  accept="audio/*" style={{ display: 'none' }} onChange={handleChange} />}
   
      <label htmlFor="musicFile" className="upload-label">
        {musicURL ? (<div style={{height:'100%', display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'space-between'}} >
          <div className='mt-2' style={{color:'#FFF'}} >{musicFile?.name}</div>
          <div className='mt-2' > 
            <Button style={{background:'transparent',border:0}} onClick={togglePlay} >
            <Image src={isPlaying?"/m_pause.svg":"/m_play.svg"} width={40} height={40} alt="" />
            </Button>
            </div>
    
         <div className="d-flex align-items-end flex-row-reverse w-100" 
            style={{height:'60px',overflowX:'hidden', whiteSpace: "nowrap",gap:'2px'}}>
            {bars.map((t, i) => (
              <div key={i}>
              {isPlaying? <NoteBar  birthTime={t} height={36} />:<NoteBarBegin height={36} />}
              </div>
            ))}
          </div>
        </div>
         
        ) : (<div className='music-show-select'  >
            <Music_upload />
            <div style={{color:'#FFF'}} ><span>Drag your file or</span>{' '}
            <span style={{color:'#30C5F7'}} >browse</span></div>
            <div style={{color:'#6D6D6D'}} >Max size: 100 MB</div>
            </div>
        )}
      </label>

      {musicURL && (
        <button type="button" className="clear-btn" onClick={handleClear}>
          ×
        </button>
      )}
      <audio ref={audioRefup} src={musicURL??'/'}  onLoadedMetadata={handleLoadedMetadata} onEnded={withEnd} />
    </div>
  );
}
MusicUpload.displayName = "MusicUpload";
export default React.memo(MusicUpload);