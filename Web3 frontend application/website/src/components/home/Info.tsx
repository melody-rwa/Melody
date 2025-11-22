'use client';
import React from "react";
import Loadding from "../Loadding";
import ShowErrorBar from "../ShowErrorBar";
import { Dropdown, DropdownButton } from "react-bootstrap";
import { SalesType } from "@/lib/mysql/message";
import { trimTrailingZeros } from "@/lib/utils/js";
import CopyWorld from "../CopyWorld";
import { stopEvent } from "@/lib/utils/eventUtils";
import { useLayout } from "@/contexts/LayoutContext";
import { useLazyFetch } from "@/hooks/useLazyFetch";

interface MusicPlayerProps {
    songId:number;
    user_address:string;
}

const Info=({songId,user_address}: MusicPlayerProps)=> {
    const { setCanSelect } = useLayout();
 
    const { data, status, error, fetchData } = useLazyFetch<SalesType[]>(
      `Info_${songId}`,
      async () => {
        const res = await fetch(`/api/getData?did=${user_address}&songId=${songId}`, {
          headers: { "x-method": "getSales" }
        });
        if (!res.ok) throw new Error('Fetch failed');
        const json: SalesType[] = await res.json();
        return json ?? [];
      },
      { autoRefreshOnVisible: false, useCache: true }
    );

    const handleToggle =async (nextShow: boolean) => {

        setCanSelect(!nextShow) 

        if(nextShow){
          await fetchData(true);
        }
   
    };
    
    const geneStartTime=(createTime:number)=>{   
      
          return new Date(createTime * 1000).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false, 
          });
       
      }
 
    
  return (
    <DropdownButton variant="dark" drop="start"    onClick={(e:any) => { stopEvent(e);}}
    //  className="btn-aa"
       title="Open"
     onToggle={handleToggle}
    >
   {
        status==='loading'?<Loadding />
        :(status==='failed' || !data)? <ShowErrorBar errStr={error ?? ''} />
        :data && data.length===0? <ShowErrorBar errStr='non' />
        :data && data.length>0 && data.map((item,idx)=>( <Dropdown.Item key={idx}>
            <span style={{width:'140px',display:'inline-block',}} >{geneStartTime(item.hash_time)}</span>
            <span className=" text-end"  style={{width:'120px',display:'inline-block',}} >{trimTrailingZeros(Number(item.sub_amount).toFixed(2))} BNB</span>
            <span className=" text-end"  style={{width:'120px',display:'inline-block',}} ><CopyWorld copyStr= {item.tx_hash} showStr="hash" /></span>
        </Dropdown.Item>))
    }

    </DropdownButton>


);
}


Info.displayName="Info";
export default React.memo(Info);

