"use client";

import Loading from "@/components/Loadding";
import { useLayout } from "@/contexts/LayoutContext";
import { RootState } from "@/store/store";
import { useSelector } from "react-redux";
import { useState } from "react";
import ArtistShow from "./ArtistShow";
import Edit from "./Edit";
import ShowErrorBar from "@/components/ShowErrorBar";

export default function ClientPage() {
  const {isShowBtn}=useLayout();
  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
  const [st,setSt]=useState('artistshow');

  return (
  
  <>
    {isShowBtn ?
      <>
       {user.connected<1?
            <div style={{width:'100%',height:'100%'}} >
                <ShowErrorBar errStr='Wallet not connected'></ShowErrorBar>
            </div>
            : st==='artistshow' && user.user_type===1?
             <ArtistShow user={user} setSt={setSt} />
            :<Edit user={user} setSt={setSt} />
        }
      </>
      :<Loading />
      }
  </>
    
  );
}
