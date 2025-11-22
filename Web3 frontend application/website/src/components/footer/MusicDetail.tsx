"use client";
import ImageWithFallback from "../ImageWithFallback";

export default function MusicDetail({musicObj}:{musicObj:MusicType|null}) {

  return (
    <div className="melo-flex-start" style={{gap:'10px'}} >
      
          <ImageWithFallback src={musicObj?.token_logo} alt=""  width={56} height={56}  style={{borderRadius:'10px'}} />
       
        <div>
            <div style={{width:'100px',flexShrink:0}}  className="one-line-clamp"  >{musicObj?.token_name}</div>
            <div style={{width:'100px',flexShrink:0}}  className="one-line-clamp"  >{musicObj?.artist_name}</div>
        </div>
  </div>
      
  );
}
