'use client';

import { RootState } from '@/store/store';
import { createContext, useContext, useState, ReactNode, useEffect, useRef, RefObject } from 'react';
import { useSelector } from 'react-redux';
import { type MusicPlayerHandle } from '@/components/footer/MusicPlayer';

interface LayoutContextType {
  isShowBtn: boolean;
  setIsShowBtn: (value: boolean) => void;
  musicObj: MusicType|null;
  setMusicObj: (value: MusicType|null) => void;
  isPlaying: boolean;
  setIsPlaying: (value: boolean) => void;
  musicList:(MusicType)[];
  setMusicList:(v:(MusicType)[])=>void;
  audioRef: RefObject<MusicPlayerHandle|null>;
  canSelect:boolean;
  setCanSelect:(value: boolean) => void;
  selectedRow:number | null;
  setSelectedRow:(value: number | null) => void;
  selectedLeftRow:number | null;
  setSelectedLeftRow:(value: number | null) => void;
  currentIndex:number;
  setCurrentIndex:(v:number)=>void;
  setMusicAndpush:(v:MusicType)=>void;
  currentTime:number;
  setCurrentTime:(v:number)=>void;
}

const LayoutContext = createContext<LayoutContextType | undefined>(undefined);

export function LayoutProvider({ children }: { children: ReactNode }) {
  const [isShowBtn, setIsShowBtn] = useState(false);
  const [musicObj,setMusicObj]=useState<MusicType|null>(null); 
  const [isPlaying, setIsPlaying] = useState(false);
  const [canSelect, setCanSelect] = useState(true); 
  const [selectedRow, setSelectedRow] = useState<number | null>(null); 
  const [selectedLeftRow, setSelectedLeftRow] = useState<number | null>(null); 
  const [musicList, setMusicList] = useState<(MusicType)[]>([]); 
  const audioRef = useRef<MusicPlayerHandle | null>(null);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [currentTime, setCurrentTime] = useState(0);
  
  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;

  const setMusicAndpush=(obj:MusicType)=>{
    setMusicObj(obj);
    const arr= musicList.filter(item => item.music_id !== obj.music_id);
    arr.unshift(obj);
    setMusicList(arr);
    setCurrentIndex(0);
    setIsPlaying(false);

  }

 useEffect(()=>{
  if(user.connected!==1){
    setIsPlaying(false);
    setMusicList([]);
  }
 },[user.connected])

  return (
    <LayoutContext.Provider value={{isShowBtn, setIsShowBtn,musicObj,setMusicObj, isPlaying,setIsPlaying,musicList,
    setMusicList,audioRef,canSelect,setCanSelect,selectedRow,setSelectedRow,selectedLeftRow, setSelectedLeftRow,setMusicAndpush,
    currentIndex, setCurrentIndex,currentTime, setCurrentTime}}>
      {children}
    </LayoutContext.Provider>
  );
}

export function useLayout() {
  const context = useContext(LayoutContext);
  if (context === undefined) {
    throw new Error('useLayout must be used within a LayoutProvider');
  }
  return context;
}