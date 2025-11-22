"use client";

import {Nav} from "react-bootstrap";
import { Launchmenu,Launchmenu_upload,Launchmenu_basic } from "@/lib/jssvg/melosvg";
import React from "react";
 interface PropsType{
    st:string;
    setSt:(v:string)=>void;
 }

const Left=({st,setSt}:PropsType)=> {

    const selectMenu=(e:any)=>{
        setSt(e.toString());
     
    }

  return (
        <Nav className="flex-column" onSelect={selectMenu} activeKey={st}>
   
            <Nav.Item >
                <Nav.Link  eventKey='upload'>
                    <div style={{display:'flex',alignItems:'center',justifyContent:'space-around'}} >
                       <Launchmenu_upload color={st==='upload'? "#FFF":"#98A1B3"} />
                        <div style={{paddingLeft:'6px',flex:1,color:st==='upload'? "#FFF":"#98A1B3"}}  > 
                            Upload File
                        </div>
                        <div>
                           <Launchmenu color={st==='upload'? "#FFF":"#98A1B3"} />
                        </div>
                    </div>
                </Nav.Link>
            </Nav.Item>

            <Nav.Item >
                <Nav.Link  eventKey='basic'>
                    <div style={{display:'flex',alignItems:'center',justifyContent:'space-around'}} >
                       <Launchmenu_basic color={st==='basic'? "#FFF":"#98A1B3"} />
                        <div style={{paddingLeft:'6px', flex:1,color:st==='basic'? "#FFF":"#98A1B3"}}  > 
                            Basic setting
                        </div>
                        <div>
                           <Launchmenu color={st==='basic'? "#FFF":"#98A1B3"} />
                        </div>
                    </div>
                </Nav.Link>
            </Nav.Item>
        </Nav>
  )
}

Left.displayName = "Left";
export default React.memo(Left);