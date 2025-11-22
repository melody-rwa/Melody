"use client";

import { usePageFetch } from "@/hooks/usePageFetch";
import { useEffect, useState } from "react";
import Loadding from "../Loadding";
import ShowErrorBar from "../ShowErrorBar";
import MusicGrid from "./MusicGrid";
import PageItem from "../PageItem";
import { useSelector } from "react-redux";
import { RootState } from "@/store/store";
import React from "react";
import Non from "../Non";
import { useLayout } from "@/contexts/LayoutContext";


interface PropsType{
  isEdit?:boolean;
  userAddress?:string;  
  states:string;
  t:number; 

}

const MusicParent=({states,t,userAddress,isEdit=true}:PropsType)=> {
    
  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
  const where = useSelector((state: RootState) => state.valueData.where) as string;
  const [currentPageNum, setCurrentPageNum] = useState(1) 
      
  const {setCanSelect}=useLayout();
  
  const { rows, total, pages, status, error,refetch } = usePageFetch<MusicType[]>(
    `/api/getData?pi=${currentPageNum-1}&w=${where}&type=${states}&account=${userAddress || user.user_address.toLowerCase()}&t=${t}`,
     'messagePageData',[],
     null,
     {
       autoRefreshOnVisible: false,       // When the page is switched back to the foreground, it will automatically refresh
       autoRefreshOnRouteChange: true,   // Route switching automatically refreshes
       useCache: true,                   
     }
    );

    useEffect(()=>{
      if(rows && Array.isArray(rows) && rows.length>0){
        setCanSelect(true);
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    },[rows])
    
  
  return (
    <>
      
     {status === 'loading'?<div style={{width:'100%',height:'200px'}} ><Loadding /></div>
          :(status === 'failed')?<ShowErrorBar errStr={error || ''} />
          :rows.length===0?<div style={{width:'100%',height:'300px'}} > <Non /> </div>
          :
          <>
        
          <MusicGrid rows={rows} states={states} user={user} isEdit={isEdit} refetch={refetch} />
           
            <PageItem
              records={total}
              pages={pages}
              currentPageNum={currentPageNum}
              setCurrentPageNum={setCurrentPageNum}
              postStatus={status}
            />
          </>
      }
    </>
  );
}

MusicParent.displayName="MusicParent";
export default React.memo(MusicParent);