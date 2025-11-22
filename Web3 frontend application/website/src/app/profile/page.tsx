"use client";
import Loading from "@/components/Loadding";
import ShowErrorBar from "@/components/ShowErrorBar";
import { useLayout } from "@/contexts/LayoutContext";
import { RootState } from "@/store/store";
import { useSelector } from "react-redux";
import Show from "./Show";

export default function Profile() {
  const {isShowBtn}=useLayout();
  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;

  return (
  
  <>
    {isShowBtn ?
      <>
        {user.connected<1?
            <div style={{width:'100%',height:'100%'}} >
                <ShowErrorBar errStr='Wallet not connected'></ShowErrorBar>
            </div>
            :<Show user={user} />
          }
        </>
        :<Loading />
      }
  </>
    
  );
}
