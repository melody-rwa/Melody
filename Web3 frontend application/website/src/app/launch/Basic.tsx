"use client";

import { useEffect, useState } from "react";
import { Button, Col,Row, Form } from "react-bootstrap";
import RaiseButton from "./RaiseButton";
import DateTimePicker from "./DateTimePicker";
import React from "react";
import ValidatedCheckbox from "@/components/ValidatedCheckbox";

interface PropsType{
  totalRaise:number;
  setTotalRaise:(v:number)=>void;
  startTime:string;
  setStartTime:(v:string)=>void;
  twitter:string;
  setTwitter:(v:string)=>void;
  website:string;
  setWebsite:(v:string)=>void;
  tg:string;
  setTg:(v:string)=>void;
  check1:boolean|null;
  setCheck1:(v:boolean|null)=>void
  check2:boolean|null;
  setCheck2:(v:boolean|null)=>void 
  check3:boolean|null;
  setCheck3:(v:boolean|null)=>void;
  setSt:(v:string)=>void;
  handleSubmit:()=>void;
  showError:(v:string)=>void;
  musicDuration:number;
  isOk:boolean;

}


const Basic=({
  totalRaise,setTotalRaise,
  startTime,setStartTime,
  twitter,setTwitter,
  website,setWebsite,
  tg,setTg,
  check1,setCheck1,
  check2,setCheck2, 
  check3,setCheck3,
  setSt,isOk,
  handleSubmit,
  showError,musicDuration,
  
}:PropsType)=> {
  
  const [dateTime, setDateTime] = useState<Date | null>(startTime?new Date(startTime):null);
  const [isCheck,setIsCheck]=useState(false);

  
  useEffect(()=>{
    if(dateTime){
      setStartTime(toDateTimeValue(dateTime));
    } else 
    {
      setStartTime('');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  },[dateTime])


  function toDateTimeValue(date:Date) {
    if (!date) return "";
    const pad = (n:number) => String(n).padStart(2, "0");
    const yyyy = date.getFullYear();
    const MM = pad(date.getMonth() + 1);
    const dd = pad(date.getDate());
    const hh = pad(date.getHours());
    const mm = pad(date.getMinutes());
    const ss = pad(date.getSeconds() || 0); 
  
    return `${yyyy}-${MM}-${dd} ${hh}:${mm}:${ss}`;
  }


  
  return (
  <div className="mt-2 mb-5" style={{width:'600px', marginLeft: '-300px'}} > 
  <div className="p-2" style={{border:'1px solid #1F263366'}}> 
      <div>
            <span className="must-text" >*</span>
            <span style={{color:'#FFF',fontSize:'18px',fontWeight:'700'}} >Total raise</span>
      </div>
        
      <div className="px-3 mt-1 mb-3">
      <RaiseButton totalRaise={totalRaise} setTotalRaise={setTotalRaise} musicDuration={musicDuration} showError={showError} />
      </div>

      <div className="px-3 d-flex gap-5 ">
        <span style={{color:'#FFF',fontSize:'18px',fontWeight:'700'}}  >  Start  Time</span>
       
       
         <Form.Check  checked={isCheck} onChange={(e:any)=>{setIsCheck(e.target.checked)}}
        // type="switch"
        id="custom-switch1"
        label="Schedule a release"
      />
        </div>
    
     
  
     { isCheck && <div className="px-3" >
        <DateTimePicker   value={dateTime} onChange={setDateTime} />
      </div>}
    

      <div className="px-3 mt-3" style={{color:'#FFF',fontSize:'18px',fontWeight:'700'}}  > Contacts </div>
      <Row className="px-3 mt-2 mb-2" >
      <Form.Group as={Col} md="6" >
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Website(optional)</Form.Label>
        <Form.Control  value={website} onChange={(e:any)=>setWebsite(e.target.value)} placeholder="Add URL" className="melo-input" />
      </Form.Group>
      <Form.Group as={Col} md="6" className="px-3">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Twitter(optional)</Form.Label>
        <Form.Control value={twitter} onChange={(e:any)=>setTwitter(e.target.value)}   placeholder="Add URL" className="melo-input" />
      </Form.Group>
      </Row>
      <Form.Group  className="px-3 mt-2 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Telegram(optional)</Form.Label>
        <Form.Control value={tg} onChange={(e:any)=>setTg(e.target.value)} placeholder="Add URL" className="melo-input" />
      </Form.Group>

      <div className="px-3" >
      <ValidatedCheckbox
        name="melo-c1"
        label="Disclaimer"
        checked={check1||false}
        onChange={(e) => setCheck1(e.target.checked)}
        isInvalid={check1===null?false:!check1}
        feedback="You must agree Disclaimer before submitting."
      />
      <ValidatedCheckbox
        name="melo-c2"
        label={
          <>
            <span className="melo-check-label" >Check to agree</span>
            <span  className="melo-check-y" >《Copyright Income Agreement Statement》</span>

          </>
        }
        checked={check2||false}
        onChange={(e) => setCheck2(e.target.checked)}
        isInvalid={check2===null?false:!check2}
        feedback="You must agree 《Copyright Income Agreement Statement》 before submitting."
      />
      <ValidatedCheckbox
        name="melo-c3"
        label="Signing of Copyright Income Agreement for Music Work"
        checked={check3||false}
        onChange={(e) => setCheck3(e.target.checked)}
        isInvalid={check3===null?false:!check3}
        feedback="You must signing of Copyright Income Agreement for Music Work."
      />

        {/* <Form.Check id="melo-c2"  isInvalid={!check2}  checked={check2} onChange={(e:any)=>setCheck2(e.target.checked)}   className="melo-check-label" 
          label={
            <>
              <span className="melo-check-label" >Check to agree</span>
              <span  className="melo-check-y" >《Copyright Income Agreement Statement》</span>

            </>
          }
        /> */}
        {/* <Form.Check id="melo-c3"  checked={check3} onChange={(e:any)=>setCheck3(e.target.checked)}   className="melo-check-label" label="Signing of Copyright Income Agreement for Music Work" /> */}
      </div>
      </div>
      <div className="mt-3 mb-5 px-3" >
          <Button className="btn-custom"  onClick={()=>setSt('upload')}>Go back</Button>{' '}
          <Button className="btn-custom"  onClick={handleSubmit}   style={{opacity:isOk?1:0.7}}>Submit</Button>
      </div>

     
  </div>
  );
}


Basic.displayName = "Basic";
export default React.memo(Basic);