"use client";
import React from 'react';
import './styles/selectstate.css'
import { Button, Nav } from "react-bootstrap";
import Image from 'next/image'

interface PropType{
    setIsPlaying?:(v:boolean)=>void;
    setSt:(v:string)=>void;
    st:string;
    setT:(v:number)=>void;
    stateAr:string[];
}

const SelectState=({st,setSt,setT,stateAr}:PropType)=> {
 
    const selectMenu=(e:any)=>{
        setSt(e.toString());
    }

  return ( 
   <div className='px-3' style={{display:'flex',flexShrink:0,alignItems:'center',justifyContent:'space-between',background:'#191919'}} >
   <Nav onSelect={selectMenu} activeKey={st}>
    {
        stateAr.map((stateKey,idx)=>(
            <Nav.Item key={idx} >
                <Nav.Link className="melo-nav-link" eventKey={stateKey}>
                   <div style={{width:'100px',textAlign:'center'}} >{stateKey}</div> 
                    {st===stateKey && <div className="melo-home-nav-active" ></div>}
                </Nav.Link>
                
            </Nav.Item>
        ))
    }
   
   </Nav>

   <Button variant='dark'  onClick={()=>setT(new Date().getTime())} >
    <Image alt='' width={20} height={20} src="/refresh.svg" /></Button>
   </div>
  );
}

SelectState.displayName="SelectState";
export default React.memo(SelectState);

 