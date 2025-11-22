'use client';

import { useFetch } from "@/hooks/useFetch";
import { bnbType } from "@/lib/mysql/message";
import React, { useEffect, useState } from "react";
import { Button, Form } from "react-bootstrap";

interface PropsType{
    totalRaise:number;
    setTotalRaise:(v:number)=>void;
    musicDuration:number;
    showError:(v:string)=>void;
  }

const RaiseButton=({totalRaise,setTotalRaise,musicDuration,showError}:PropsType)=> {

    const {data} = useFetch<bnbType[]>(`/api/getData`,'getBnb',[]);
    const [v1,setV1]=useState('');
    const [v2,setV2]=useState('');
    const [v3,setV3]=useState('');

    const Click=(v:number)=>{
      if(musicDuration===0) {showError("Launch&$&You haven’t selected a music file.");return;}
      setTotalRaise(v);
    
    }
 
    useEffect(()=>{
      if(totalRaise>0){
        setV1((musicDuration*1000000).toString());
        setV2((totalRaise/0.1).toString());
        setV3('0.1');
      }

    },[totalRaise,musicDuration])

  return (<>
   
    
        <div className="d-flex gap-2">
          {data && data.map(v => (
            <Button
              key={v.bnb_amount} 
              variant={totalRaise === v.bnb_amount  ? '' : 'outline-secondary'}
              className={totalRaise === v.bnb_amount  ? "btn-custom":""}
              onClick={()=>Click(v.bnb_amount)}
            >
            {v.bnb_amount}{' '} BNB
            </Button>
          ))}
        </div>
        <Form.Group  className="mt-2 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Max Supply</Form.Label>
        <Form.Control defaultValue={v1} readOnly={true} placeholder="Auto-fill" className="melo-input" />
       </Form.Group>

       <Form.Group  className="mt-2 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Whitelist Spots</Form.Label>
        <Form.Control defaultValue={v2} readOnly={true} placeholder="Auto-fill"  className="melo-input" />
       </Form.Group>

       <Form.Group  className="mt-2 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Invest Limit Per Spot</Form.Label>
        <Form.Control defaultValue={v3} readOnly={true} placeholder="Auto-fill" className="melo-input" />
       </Form.Group>
       {/* { totalRaise>0 && <div className="mt-1" style={{color:'#FFF',opacity:'0.8'}} >
          Max Supply:{totalRaise*1000000} <br/>
          Whitelist Spots:{totalRaise*10}<br/>
          Invest Limit Per Spot:{totalRaise*10000}
        </div>
        } */}
    
    </>
  );
}

RaiseButton.displayName = "RaiseButton";
export default React.memo(RaiseButton);