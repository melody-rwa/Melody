"use client";

import React  from "react";
import LogoUploader from "./LogoUploader";
import MusicUpload from "./MusicUpload";
import { useEffect, useState } from "react";
import { Button, Form, InputGroup } from "react-bootstrap";

interface PropsType{
    code:string;
    setCode:(v:string)=>void;
    logoFile:File|null;
    setLogoFile:(v:File|null)=>void; 
    tokenName:string;
    setTokenName:(v:string)=>void;
    tokenSymbol:string;
    setTokenSymbol:(v:string)=>void;
    tokenDesc:string;
    setTokenDesc:(v:string)=>void;
    musicFile:File|null;
    setMusicFile:(v:File|null)=>void; 
    setMusicDuration:(v:number)=>void; 
    showError:(v:string)=>void;
    setSt:(v:string)=>void;
    checkInvite:(flag?:boolean)=>Promise<boolean>;
    isValid:boolean | null;
    setIsValid:(v:boolean|null)=>void;
    showInvit:boolean;
    setShowInvit:(v:boolean)=>void;
    invitReadOnly:boolean;
    setInvitReadOnly:(v:boolean)=>void;

    validtokenName:boolean | null;
    setValidtokenName:(v:boolean|null)=>void;

    validtokenSymbol:boolean | null;
    setValidtokenSymbol:(v:boolean|null)=>void;
}


const Upload=({
    code,setCode,
    logoFile, setLogoFile, 
    tokenName,setTokenName,
    tokenSymbol, setTokenSymbol,
    tokenDesc, setTokenDesc,
    musicFile,setMusicFile,setMusicDuration,
    showError,
    setSt,
    checkInvite,
    isValid, setIsValid,
    showInvit, setShowInvit,
    invitReadOnly, setInvitReadOnly,
    validtokenName,setValidtokenName,
    validtokenSymbol,setValidtokenSymbol
    
}:PropsType)=> {

    const [musicURL, setMusicURL] = useState<string | null>(null);
    const initialUrl=logoFile? URL.createObjectURL(logoFile):'';
    

    useEffect(()=>{
        if(musicFile){
            const url = URL.createObjectURL(musicFile);
            setMusicURL(url);
        }
    },[musicFile])
    
    const check= async ()=>{

        if(!code) setIsValid(false)
        else {
            const isok=await checkInvite(true);
            setIsValid(isok);
            if(isok) {
                setShowInvit(false);
                setInvitReadOnly(true);
            } 
            
        }
    }
  
  return (
  <div className="mt-2 mb-5" style={{width:'600px', marginLeft: '-300px'}}  > 
    <div className="p-2" style={{border:'1px solid #1F263366'}}> 
        <div>
            <span className="must-text" >*</span>
            <span style={{color:'#FFF',opacity:'0.8'}} >Invitation Code</span>
        </div> 
        <InputGroup hasValidation className="mb-3 mt-1 px-3">
          <Form.Control className='melo-input'
            type="text"
            readOnly={invitReadOnly}
            value={code} 
            onChange={(e:any)=>setCode(e.target.value)} 
            placeholder="Fill in the Invitation Code"
            isValid={isValid === true}
            isInvalid={isValid === false}
          />
         
           {showInvit && <Button onClick={check} className="melo-btn" style={{color:'#F8742E'}}  >Verify </Button>}
        </InputGroup>

        <div>
            <span className="must-text" >*</span>
            <span style={{color:'#FFF',fontSize:'18px',fontWeight:'700'}} >Upload logo</span>
        </div>
        <div className="px-3" style={{color:'#6D6D6D'}} > PNG-JPEG-WEBP-GIF, Max Size: 5MB </div>
        <div className="px-3 mt-1 d-flex align-items-center gap-3 ">
            <LogoUploader maxSizeKB={5*1024} initialUrl={initialUrl} onChange={(file) => setLogoFile(file)}  /> 
            <div style={{flex:1}} >
                <div style={{color:'#FFF',opacity:'0.8',fontSize:'14px'}}>Token Name</div>
                <InputGroup hasValidation>
                    <Form.Control isValid={validtokenName === true} isInvalid={validtokenName === false} 
                    className="melo-input" value={tokenName} onChange={(e:any)=>{
                        const str=e.target.value;
                        setTokenName(str);
                        if(!str || str.length>128) setValidtokenName(false);
                        else setValidtokenName(true);

                        setTokenName(e.target.value);
                    
                    }} placeholder="Name your coin" />
                </InputGroup>
                <div style={{color:'#FFF',opacity:'0.8',fontSize:'14px'}} className="mt-1" >Ticker Symbol</div>
                <InputGroup hasValidation>
                    <Form.Control isValid={validtokenSymbol === true} isInvalid={validtokenSymbol === false} 
                    className="melo-input" value={tokenSymbol} onChange={(e:any)=>{
                        const str=e.target.value;
                        setTokenSymbol(str);
                        if(!str || str.length>128) setValidtokenSymbol(false);
                        else setValidtokenSymbol(true);

                        }}  placeholder="Add a coin ticker (e.g. DOGE)" />
                </InputGroup>

            </div>
        </div>
        <Form.Group  className="px-3 mt-2 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Description</Form.Label>
        <Form.Control  value={tokenDesc} onChange={(e:any)=>setTokenDesc(e.target.value)} className="melo-input" as="textarea" rows={3} />
        </Form.Group>
      
        <div>
            <span className="must-text" >*</span>
            <span style={{color:'#FFF',fontSize:'18px',fontWeight:'700'}} >Music Upload</span>
        </div>
        <div className="px-3" style={{color:'#6D6D6D'}} >
            Upload your music file here
            <MusicUpload musicURL={musicURL} setMusicURL={setMusicURL} onUpload={(file, duration) => {{/*Music upload */}    
            setMusicFile(file);
            setMusicDuration(duration);
            }} showError={showError} />
        </div>
        </div>
        <div className="mt-3 mb-5 px-3" >
            <Button  className="btn-custom"   onClick={()=>setSt('basic')}>Basic setting</Button>{' '}
            {/* <Button onClick={handleSubmit} className="melo-btn" style={{borderRadius:'5px',background:'#F9752D'}}>Submit</Button> */}
        </div>

  </div>
  );
}
Upload.displayName = "Upload";
export default React.memo(Upload);