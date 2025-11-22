"use client";

import { Button } from "react-bootstrap";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import Wallet from "../wallet/Index";
import TopSearch from "../TopSearch";

export default function TopMenu() {

  const router = useRouter();
  const pathname = usePathname();

  const getPath=()=>{
    let path='Home';
    switch(pathname){
      case "/":
        path="Home";
        break;
      case "/launch":
      path="Launch";
      break;
      case "/artist":
        path="Artist";
        break;
      case "/profile":
      path="Profile";
      break;
      default:
        path='Home';
        break;
    }
    return path;
  }


  return (

       
          <div style={{margin:0}} className=" melo-top-menudiv d-flex align-items-center justify-content-between gap-3  w-100" >
              <div className="flex-grow-1 d-flex align-items-center justify-content-start" >
                <Button  onClick={() => router.back()} className="melo-btn" style={{borderRadius: '12px',background: '#111'}} >
                  <Image src="/back.svg" width={24} height={24} alt="" />
                </Button>
                <span className="melo-menu-text" style={{display:'inline-block',padding:'0 80px 0 10px'}} >{getPath()}</span>
                <TopSearch />

              </div>
              <Wallet />
            
          </div>
        

      
  );
}
