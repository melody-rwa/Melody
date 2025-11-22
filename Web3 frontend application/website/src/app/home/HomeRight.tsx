
import { useFetch } from "@/hooks/useFetch";
import React from "react";
import { type ListArtistType } from "@/lib/mysql/message";
import Loadding from "@/components/Loadding";
import ShowErrorBar from "@/components/ShowErrorBar";
import AutoGallery from "./AutoGallery";
import RecentPlay from "./RecentPlay";
import Non from "@/components/Non";

const HomeRight = () => {  
    const { data, status, error } = useFetch<ListArtistType[]>(`/api/getData`,'getPlayArtist',[]);
    return (
        <div className="melo-home-right d-flex flex-column align-items-center ms-3 px-3 py-2  "  >          
            {
            status==='loading'?<Loadding />
            :(status==='failed' || !data)? <ShowErrorBar errStr={error ?? ''} />
            :data && data.length===0? <div style={{width:'100%',height:'200px'}} > <Non /> </div>
            : <div className="mb-3 w-100" style={{display:'flex',flexDirection:'column',alignItems:'center'}} >
                  <div style={{fontWeight:500}} className="w-100 mb-2" >Fans Also Like</div>
                {data && data.length>0 && <AutoGallery items={data} interval={10*1000}/>  }
              </div>
            }

           <RecentPlay />
      
      </div>
    );
  };


  HomeRight.displayName="HomeRight";
  export default React.memo(HomeRight);