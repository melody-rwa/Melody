import React, { useEffect, useRef, useState, useMemo } from "react";
import { generateMusicState, generateRemaining } from "@/lib/utils/js";

interface StateBarProps {
  musicObj: MusicType;
}

const StateBar: React.FC<StateBarProps> = ({ musicObj }) => {
    const [state,setState] = useState(-1);
    const [seconds, setSeconds] = useState(0);
    const timerRef = useRef<NodeJS.Timeout | null>(null);
    const secondsRef=useRef<number>(0);
    const stateRef=useRef<number>(0);
    const startSecondsRef=useRef<number>(0);

    const startRef=useRef<number>(0);
    const nowRef=useRef<number>(0);

  useEffect(()=>{
    startRef.current=musicObj.start_time;
    nowRef.current=musicObj.now_time;
    const _st=generateMusicState(musicObj)
    setState(_st);
    stateRef.current=_st;
    secondsRef.current=generateRemaining(startRef.current,nowRef.current,_st,0);
    startSecondsRef.current=secondsRef.current; 
    setSeconds(secondsRef.current);
   
  },[musicObj])

  // Update remaining time every second
  useEffect(() => {
    timerRef.current = setInterval(() => {
        if(secondsRef.current>0) { secondsRef.current=secondsRef.current-1; setSeconds(secondsRef.current);}
        if(secondsRef.current===0){
             if(stateRef.current<4) {
                stateRef.current=stateRef.current+1;
                setState(stateRef.current);
                //"Including the total time"
                secondsRef.current=generateRemaining(startRef.current,nowRef.current,stateRef.current,startSecondsRef.current);
                startSecondsRef.current=startSecondsRef.current+secondsRef.current;
            }
        }
    }, 1000);

    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);


  const formattedTime = useMemo(() => {
    const h = Math.floor(secondsRef.current / 3600);
    const m = Math.floor((secondsRef.current % 3600) / 60);
    const s = secondsRef.current % 60;
    return {
      h: String(h).padStart(2, "0"),
      m: String(m).padStart(2, "0"),
      s: String(s).padStart(2, "0"),
    };
  }, [seconds]);

  const states = useMemo(
    () => [
      "Collection End in",
      "Whitelist End in",
      "Interval End in",
      "Public End in",
      "Ended",
    ],
    []
  );

  const showCountdown = state >= 0 && state < 4;

  return (
    <div className="melo-home-state-p">
      <div className="melo-home-state">
        {/* status text */}
        <div className="melo-home-state-text1">
          {state > -1 ? states[state] : "Upcoming"}
        </div>

        {/* count down */}
        {showCountdown && (
          <div>
            <span className="melo-home-state-text2">{formattedTime.h}</span>:
            <span className="melo-home-state-text2">{formattedTime.m}</span>:
            <span className="melo-home-state-text2">{formattedTime.s}</span>
          </div>
        )}
      </div>
    </div>
  );
};

StateBar.displayName = "StateBar";
export default React.memo(StateBar);
