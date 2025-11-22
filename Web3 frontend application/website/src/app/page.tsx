"use client";
import CurrentMusic from "./home/CurrentMusic";
import { useState } from "react";
import SelectState from "./home/SelectState";
import MusicParent from "@/components/home/MusicParent";
import { useLayout } from "@/contexts/LayoutContext";
import HomeRight from "./home/HomeRight";
import BannerCarousel from "./home/BannerCarousel";
import Loading from "@/components/Loadding";

export default function Home() {
  const {musicObj,isPlaying,audioRef,currentTime,isShowBtn}=useLayout();
  const [st, setSt] = useState<string>('In Progress'); 
  const [t,setT]=useState(0) ;// t  Timestamp, for refreshing

  return (
  <>
    {isShowBtn ?
    <div className="melo-home-container">
      {/* Left content area */}
      <div className="melo-home-left">
        <div style={{borderRadius:'12px',position:'relative'}} >
          <BannerCarousel  />
        </div>
        <CurrentMusic musicObj={musicObj} isPlaying={isPlaying}  audioRef={audioRef} currentTime={currentTime} />
        <SelectState st={st} setSt={setSt}  setT={setT} stateAr={["In Progress","Upcoming","Ended"]} />
        <MusicParent states={st} t={t} />
      
      </div>
      {/* Right-side content area */}
      <HomeRight />   
    </div>
    :
    <Loading />
    } 
  </>
  );
}
