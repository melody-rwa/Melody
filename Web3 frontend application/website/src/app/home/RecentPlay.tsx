import ImageWithFallback from "@/components/ImageWithFallback";
import { useLayout } from "@/contexts/LayoutContext";
import { useFetch } from "@/hooks/useFetch";
import { formatSeconds } from "@/lib/utils/js";
import React, { useEffect } from "react";

const RecentPlay = () => {
    const {setSelectedRow,selectedLeftRow,setMusicAndpush, setSelectedLeftRow,musicObj}=useLayout();

    const { data } = useFetch<MusicType[]>(`/api/getData`,'getPlayAll',[]);


    useEffect(()=>{
        if(data && Array.isArray(data) && data.length>0 && !musicObj){
            setMusicAndpush(data[0]);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    },[data])

    return (
        <>
            {/* {
            status==='loading'?<Loadding />
            :(status==='failed' || !data)? <ShowErrorBar errStr={error ?? ''} />
            :data.length===0? <ShowErrorBar errStr='non' />
            :<> */}
            <div className="mb-3 w-100 mt-3" style={{fontWeight:500}} > Recent Played    </div>
            <div className="hide-scrollbar-vertical" style={{flex:1,width:'100%'}} >
                {data && data.map((item,idx)=>(
                        <div  className={`mb-3 custom-row ${selectedLeftRow===idx ? "selected" : ""}`} onClick={()=>{setMusicAndpush(item);setSelectedLeftRow(idx);setSelectedRow(null); }} key={idx} 
                            style={{width:'100%', display:'flex',alignItems:'center',justifyContent:'space-between'}} >
                            <div className="d-flex justify-content-start align-items-center gap-3">
                                <ImageWithFallback src={item.token_logo} alt=""  width={40} height={40} style={{borderRadius:'10px'}} />
                                <div >
                                    <div  className="one-line-clamp melo-recent-play2">{item.token_name}</div>
                                    <div  className="one-line-clamp mt-1 melo-recent-play2">{item.artist_name}</div>
                                </div>
                            </div>
                            <div style={{fontSize:'12px',fontWeight:400,opacity:0.5,color:'#FFF',flexShrink:0}} >{formatSeconds(item.now_time-item.create_time)}</div>
                        </div>
                    ))
                }
            </div>
            {/* </>
            } */}

      
        </>
    );
  };


  RecentPlay.displayName="RecentPlay";
  export default React.memo(RecentPlay);