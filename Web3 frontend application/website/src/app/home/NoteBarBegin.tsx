import React from "react";

const NoteBarBegin = ({height=26}:{height?:number}) => {
    const scale = 0.8;
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


  NoteBarBegin.displayName="NoteBarBegin";
  export default React.memo(NoteBarBegin);