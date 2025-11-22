import React from "react";
import { useEffect, useState } from "react";

 /* ---------- column component ---------- */
 const NoteBar = ({ birthTime,height=26 }: { birthTime: number,height?:number }) => {
    const [age, setAge] = useState(0);
    useEffect(() => {
      let raf = 0;
      const loop = () => {
        setAge(performance.now() - birthTime);
        raf = requestAnimationFrame(loop);
      };
      raf = requestAnimationFrame(loop);
      return () => cancelAnimationFrame(raf);
    }, [birthTime]);

    const scale = 0.3 + 0.7 * Math.abs(Math.sin(age * 0.008));
    const num = Math.floor(Math.random() * height) + 1;
    return (
      <div
        style={{
          width: 4,
          height: num,
          background: "linear-gradient(to top, #9333ea, #ec4899)",
          borderRadius: "4px 4px 0 0",
          transform: `scaleY(${scale})`,
          transformOrigin: "bottom",
        }}
      />
    );
  };

  NoteBar.displayName="NoteBar";
export default React.memo(NoteBar);