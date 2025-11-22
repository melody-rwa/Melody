'use client';
import { getDaismContract } from "@/lib/globalStore";
import { FormEvent, useEffect, useState } from "react";
import { Button, Form, FormControl, Modal } from "react-bootstrap";
import { chainErr, generateMusicState, trimTrailingZeros } from "@/lib/utils/js";
import React from "react";
import { stopEvent } from "@/lib/utils/eventUtils";
import { useLayout } from "@/contexts/LayoutContext";
import Image from "next/image";

interface MusicPlayerProps {
  musicObj:MusicType;
  user:MeloUserInfo;
  showError:(v:string)=>void;
  showTip :(v:string)=>void;
  closeTip:()=>void;
  showMessage:(v:string)=>void;
  refetch:()=>void;
}

const Deposit=({musicObj,user,showError,showTip,closeTip,showMessage,refetch }: MusicPlayerProps)=> {
  const [show,setShow]=useState(false);
  const [errorBnb,setErrorBnb]=useState(false);
  const [inputErr,setInputErr]=useState('');
  const state = generateMusicState(musicObj);

  const [whitelist, setWhitelist] = useState<string[]>([]);

  useEffect(() => {
    if (user.connected !== 1 ) return;  
    const controller = new AbortController();
    const fetchData = async (songId: number) => {
      try {
        const res = await fetch(`/api/getData?songId=${songId}`, {
          signal: controller.signal,
          headers: { "x-method": "getWhiteListById" },
        });


        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);

        const data: WhiteType[] = await res.json();

        const result: string[] = data.map((e) => e.user_address);

        setWhitelist(result);
      } catch (err: any) {
        if (err.name !== "AbortError") {
          console.error("Fetch error:", err?.message|| err);
        }
      }
    };

    if(musicObj){
      const _state=generateMusicState(musicObj);
      if(_state!==1) return;
      fetchData(musicObj.song_id);
    }

  
    return () => controller.abort();
  }, [user,musicObj]);


    const submit= async(event: FormEvent<HTMLFormElement>) =>{
      setCanSelect(true)
      event.preventDefault(); event.stopPropagation();
      const form = event.currentTarget;    

      const _v=parseFloat((form.elements.namedItem('bnbAmount') as HTMLInputElement).value.trim());

      if(isNaN(_v) || _v<0.01) {
         setErrorBnb(true);
        setInputErr("The subscription amount cannot be less than 0.01 BNB");
        return;
      }
      showTip('Deposit&$&Submitting Request...')
      const res=await fetch(`/api/getData?songId=${musicObj.song_id}&did=${user.user_address}`, {headers: { "x-method": "getWhitePay" }})
    
      if(!res.ok){
        closeTip();
        showError('Deposit&$&Data processing failed');
        return;
      }

      const resJson:Record<string, string>[] =await res.json();
      
      if(state===1 &&  parseFloat(resJson[0].t.toString())+_v>0.1) {
        setErrorBnb(true);
        closeTip();
        setInputErr(`The maximum limit is 0.1 BNB, but for this round you can only subscribe ${trimTrailingZeros(0.1-parseFloat(resJson[0].t.toString()))} BNB.`);
        return
      }
      

      if(parseFloat(resJson[1].t.toString())+_v>parseFloat(musicObj.total_raise?.toString()??'0')){
        setErrorBnb(true);
        closeTip();
        setInputErr(`The maximum limit is ${trimTrailingZeros((musicObj.total_raise??0).toString())} BNB, but for this round you can only subscribe ${trimTrailingZeros(parseFloat((musicObj.total_raise??0).toString())-parseFloat(resJson[0].t.toString()))} BNB.`);
        return;
      }
      
      const daismObj=getDaismContract();
      showTip('Deposit&$&Submitting Request...')
      setShow(false);

      try {
        const receipt = await daismObj?.MusicSeries.subscribe(musicObj?.series_address??'',musicObj.song_id,_v);  
        if (!receipt) {throw new Error("Transaction receipt is null");}
        showMessage(`Deposit&$&Already Depositd ${trimTrailingZeros(_v)} BNB`);
        setTimeout(() => {
          refetch();  
        }, 500);
     
      
      } catch (err:any) {
        chainErr(err,showError,'Deposit');
      }
      finally{
          closeTip(); 
      }
    }
 
    
  const {setCanSelect}=useLayout();
  return (
  <>{
    (musicObj.is_end===0 && user.connected===1 && (state===3 || (state===1 && whitelist.includes(user.user_address))))?
    <> 
    <Button variant="dark" className="melo-btn-desposit"   onClick={(e:any)=>{stopEvent(e);setShow(true);setCanSelect(false);}} >Deposit</Button>
    
      <Modal  centered contentClassName="melo-dailog-parent" backdrop="static" keyboard={false}
       show={show} onHide={() => {setShow(false);setCanSelect(true);}}>
       
        <Modal.Body  className='melo-dailog' >
        <div  className='melo-dailog-inner'   >
        <div className='melo-flex-between mb-3' >
                   <span className='melo-dailog-title' >Deposit </span>
                   <div className='melo-dailog-title-close-btn' onClick={()=>{setShow(false);setCanSelect(true);}} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
                </div>
        <Form onSubmit={submit}>
          <Form.Group className="mb-5" controlId="bnbAmount" style={{position:'relative'}} >
            <Form.Label style={{color:'rgba(255, 255, 255, 0.60)',fontSize:'12px',fontWeight: 400,lineHeight:'16px'}} >Deposit amount</Form.Label>
            <FormControl style={{paddingRight:'70px'}} className="melo-dailog-input" isInvalid={errorBnb} defaultValue={0.1} type="text" placeholder="Enter the amount" 
             onFocus={() => { setErrorBnb(false) }}  />
             {!errorBnb && <span style={{position:'absolute',right:'10px',top:'38px'}} >BNB</span>}
              <Form.Control.Feedback type="invalid">{inputErr}</Form.Control.Feedback>
          </Form.Group>

          <div className="mt-5 mb-3" style={{ display:'flex',alignItems:'center',justifyContent:'center',gap:'10px'}}>
            <Button variant="dark"  className="melo-btn-dailog melo-dailog-cancel" onClick={()=>{setShow(false);setCanSelect(true);}} >Cancel</Button>{' '}
            
              <Button variant="dark" className="melo-btn-dailog melo-dailog-submit"  type="submit" >
                Submit
              </Button>
          </div>
        </Form>
        </div>
        </Modal.Body>
      </Modal>
      
    </>
    : <Button variant="dark"  className="melo-btn-desposit-disable"  disabled={true} >Deposit</Button>
    }
  </>
);
}



Deposit.displayName="Deposit";
export default React.memo(Deposit);