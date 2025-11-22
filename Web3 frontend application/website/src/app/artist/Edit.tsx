
import { Button, Form, InputGroup } from "react-bootstrap";
import { useDispatch } from "react-redux";
import { setErrText, setTipText, setUser } from "@/store/store";
import React, { useEffect, useState } from "react";
import LogoUploader from "../launch/LogoUploader";
import './styles/edit.css'
import { getDaismContract } from "@/lib/globalStore";
import { chainErr } from "@/lib/utils/js";
import { ArtistLeft1, ArtistLeft2, ArtistLeft3, ArtistLeft4 } from "@/lib/jssvg/melosvg";

const Edit=({user,setSt}:{user:MeloUserInfo,setSt:(v:string)=>void})=> {

  const [logoFile, setLogoFile] = useState<File | null>(null);
  const [userName,setUserName]=useState(user.artist_name||'');
  const [userLink,setUserLink]=useState<string>(user.artist_link||'');
  const [bio,setBio]=useState(user.artist_desc||'');
  const [twitter,settwitter]=useState(user.twitter||'');
  const [facebook,setFacebook]=useState(user.facebook||'');
  const [tg,setTg]=useState(user.tg||'');
  const [instgram,setInstgram]=useState(user.instgram||'');
  const [isModified, setIsModified] = useState<boolean>(false); 

  const[ok,setOk]=useState(false);

  const dispatch = useDispatch();
  const showError = (str: string) => dispatch(setErrText(str));
  const showTip = (str: string) => dispatch(setTipText(str));
  const closeTip = () => dispatch(setTipText(''));

  useEffect(()=>{
    if(isModified && !logoFile) setOk(false);
    else if(!user.artist_avatar && !logoFile)  setOk(false);
    else if(!userName)  setOk(false)
    else setOk(true);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  },[logoFile,userName,isModified])

  const post=async ()=>{
    const formData=new FormData();
    if(logoFile) formData.append('file', logoFile);
    formData.append('userName', userName);
    formData.append('userDesc', bio);
    formData.append('userLink', userLink);
    formData.append('twitter', twitter);
    formData.append('facebook', facebook);
    formData.append('tg', tg);
    formData.append('instgram', instgram);
    formData.append('did', user.user_address);
    formData.append('userType', '1');
    
    try {
      const response = await fetch('/api/profile', {
          method: 'POST',
          body: formData
      });
    
      if(!response.ok){
        showError(`Artist edit&$&Data processing failed!-->${response.statusText||''}(${response.status??500})`);
        return;
      }

      const re = await response.json();
      const baseData={ twitter,tg,facebook,instgram,artist_name:userName,artist_desc:bio,artist_link:userLink}
      dispatch(setUser({...user,user_type:1,...baseData,...(re.avatarPath ? { artist_avatar:re.avatarPath } : {})}));
      setSt("artistshow");
      
    } catch (error:any) 
    {
      closeTip();
      showError(`Artist edit&$&Data processing failed!-->${error.message||''}`);
    }
    finally
    {
      closeTip();
    }
  }

  const submit=async ()=>{

    if(isModified && !logoFile) { showError('Artist edit&$&No avatar selected.');return; }
    if(!userName)  { showError('Artist edit&$&Your Name cannot be empty or greater than 128 characters');return; }
    if(!user.artist_avatar && !logoFile) {showError('Artist edit&$&Your avatar not provided');return; }
    
  
    showTip('Artist edit&$&Submitting Request...')

    try {
    
      const daismObj=getDaismContract();
      const isArtist = await daismObj?.ArtistRegistry.isArtist(user.user_address);
  

       if(!isArtist){  //Not registered
        try {
          const daismObj=getDaismContract();
          await daismObj?.ArtistRegistry.register(`https://${process.env.NEXT_PUBLIC_DOMAIN}/artist/${user.user_address}`);   
          await post();
        } catch (err:any) {
          chainErr(err,showError,'Artist edit');
        }
      }
      else 
      {
        await post();
      }
    } catch (err: any) {
      if (err.name !== "AbortError") {
        showError(`Artist edit&$&Fetch error:${err?.message||''}`);
      }
    } finally{
      closeTip();
    }   
  }

    return (
    <div className="mb-5" style={{padding:'20px 0 0 40px', maxWidth:'800px'}} >
        <div className="mb-4" style={{fontSize:'39px',color:'#FFF'}} >Apply for Artist Status </div>
        <div style={{fontSize:'18px',fontWeight:700,color:'#FFF'}} >Basic Data</div>
        <div className="mb-1" style={{fontSize:'14px',fontWeight:400,color:'#6D6D6D'}} >Add your basic information  </div>

        <div className="mt-1 d-flex align-items-center gap-3 ">
            <LogoUploader maxSizeKB={2*1024} initialUrl={user.artist_avatar} onChange={(file) => {setLogoFile(file);setIsModified(true);}}  /> 
            <div style={{flex:1}} >
                <div style={{color:'#FFF',opacity:'0.8',fontSize:'12px'}}>Your Name</div>
                <InputGroup>
                    <Form.Control className="melo-input" value={userName} onChange={(e:any)=>setUserName(e.target.value)} placeholder="Ex. Kater john" />
                </InputGroup>
                <div style={{color:'#FFF',opacity:'0.8',fontSize:'12px'}} className="mt-1" >Related Links</div>
                <InputGroup>
                    <Form.Control className="melo-input" value={userLink} onChange={(e:any)=>setUserLink(e.target.value)}  placeholder="Add URL" />
                </InputGroup>

            </div>
        </div>
        <Form.Group  className="mt-4 mb-2">
        <Form.Label style={{color:'#FFF',opacity:'0.8'}} >Bio</Form.Label>
        <Form.Control  value={bio} onChange={(e:any)=>setBio(e.target.value)} placeholder="Add a detailed description..." className="melo-input" as="textarea" rows={3} />
        </Form.Group>

        <div style={{fontSize:'18px',fontWeight:700,color:'#FFF'}} >Link Your Social Account</div>
        <div className="mb-1" style={{fontSize:'14px',fontWeight:400,color:'#6D6D6D'}} >Add your social media information       </div>

     
        <div className="melo-artist-edit-box mt-3 mb-3" >
           <div style={{width:'50px'}} ><ArtistLeft1  /></div>
            <div style={{flex:1}} >
              <div style={{color:'#FFF',opacity:'0.8'}}>twitter</div>
              <InputGroup >
                <Form.Control className="melo-input" value={twitter} onChange={(e:any)=>settwitter(e.target.value)} placeholder="Add URL" />
                {/* <Button  className="melo-btn" style={{color:'#F8742E'}}  >Verify </Button> */}
              </InputGroup>
            </div>
        </div>
        <div className="melo-artist-edit-box mt-2 mb-3" >
            <div style={{width:'50px'}} ><ArtistLeft2  /></div>
            <div style={{flex:1}} >
              <div style={{color:'#FFF',opacity:'0.8'}}>Facebook</div>
              <InputGroup >
                <Form.Control className="melo-input" value={facebook} onChange={(e:any)=>setFacebook(e.target.value)} placeholder="Add URL" />
                {/* <Button  className="melo-btn" style={{color:'#F8742E'}}  >Verify </Button> */}
              </InputGroup>
            </div>
        </div>
        <div className="melo-artist-edit-box mt-2 mb-3" >
            <div style={{width:'50px'}} ><ArtistLeft3  /></div>
            <div style={{flex:1}} >
              <div style={{color:'#FFF',opacity:'0.8'}}>Telegram</div>
              <InputGroup >
                <Form.Control className="melo-input" value={tg} onChange={(e:any)=>setTg(e.target.value)} placeholder="Add URL" />
                {/* <Button  className="melo-btn" style={{color:'#F8742E'}}  >Verify </Button> */}
              </InputGroup>
            </div>
        </div>
        <div className="melo-artist-edit-box mt-2 mb-3" >
            <div style={{width:'50px'}} ><ArtistLeft4  /></div>
            <div style={{flex:1}} >
              <div style={{color:'#FFF',opacity:'0.8'}}>Instgram</div>
              <InputGroup >
                <Form.Control className="melo-input" value={instgram} onChange={(e:any)=>setInstgram(e.target.value)} placeholder="Add URL" />
                {/* <Button  className="melo-btn" style={{color:'#F8742E'}}  >Verify </Button> */}
              </InputGroup>
            </div>
        </div>

        <div className="mt-3 mb-5">

          <Button className="btn-custom px-2"  onClick={()=>setSt('artistshow')} >Go Back</Button>{' '}
          <Button  style={{opacity:ok?1:0.7}}  className="btn-custom px-2"  onClick={submit} >Submit</Button>

        </div>

    </div>
  );
}

Edit.displayName = "Edit";
export default React.memo(Edit);