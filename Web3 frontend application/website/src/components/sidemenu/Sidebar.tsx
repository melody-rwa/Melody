"use client";

import { usePathname } from "next/navigation";
import Link from "next/link";
import Image from "next/image";
import { useLayout } from "@/contexts/LayoutContext";
import { useFetch } from "@/hooks/useFetch";
import { useSelector } from "react-redux";
import { RootState } from "@/store/store";
import { useEffect } from "react";
import PlayItem from "./PlayItem";

type LinkType ={
  str:string;
  href:string;
}

const URLS=[
  {str:'Home',href:'/'},
  {str:'Launch',href:'/launch'},
  {str:'Artist',href:'/artist'},
  {str:'Profile',href:'/profile'},
] as LinkType[];


export default function Sidebar() {
  const pathname = usePathname();
  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
  const { data } = useFetch<MusicType[]>(`/api/getData?did=${user.user_address}`,'getPlayUser',[]);
  
  const {musicList, setMusicList}=useLayout();

 useEffect(()=>{
    if(data && data.length>0){
      setMusicList(data);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
 },[data])


  return ( 
  <div style={{display:'flex',flexDirection:'column',height:'100%'}} >
    <div className="px-3 pt-2 mb-3" >
    <Image src='/logo.png' width={170} height={40} alt='' />
    </div>
     
    <div className="mb-3" >
      {
        URLS.map((obj,idx)=>(
          <div className="m-2 mb-2" key={idx} >
          <Link className="melo-a" href={obj.href}>
              <div className="melo-flex-start" >
              {pathname===obj.href?
              <span className="melo-nav-active"  ></span>
              :<span className="melo-nav-active1" ></span>
              }
              <Image alt="" width={40} height={40} src={pathname===obj.href?`/${obj.str.toLowerCase()}_1.svg`:`/${obj.str.toLowerCase()}.svg`} />
              <span  className="melo-menu-text"> {obj.str} </span>
              </div>
          </Link>
          </div>
        ))
      }
    </div>
 

    <div className="p-2" style={{flex:1, display:'flex',flexDirection:'column',justifyContent:'flex-end'}} >
      <div style={{display:'flex',alignContent:'center',justifyContent:'space-between',width:'100%'}} >
          <div style={{color:'#7D7D7D',fontSize:'16px',fontWeight:400}} >PlayList</div>
          <Link className="melo-a" href='/moreplay' >
          <Image alt="" width={24} height={18} src="/more.svg" />
          </Link>
      
      </div>
     
        {
          musicList.slice(0,3).map((item,idx)=>(
            <PlayItem  key={`side_${idx}`} item={item} idx={idx} />
          ))
        }
      </div>
  </div>
  );
}
