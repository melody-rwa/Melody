
import { Button, Form, InputGroup } from "react-bootstrap";
import { useDispatch } from "react-redux";
import { setErrText, setTipText, setUser } from "@/store/store";
import React, { useState } from "react";
import LogoUploader from "../launch/LogoUploader";

const Edit=({user,setSt}:{user:MeloUserInfo,setSt:(v:string)=>void})=> {

  const [logoFile, setLogoFile] = useState<File | null>(null);
  const [userName,setUserName]=useState(user.user_name);
  const [userLink,setUserLink]=useState<string>(user.user_link||'');
  const [bio,setBio]=useState(user.user_desc||'');
  const [isModified, setIsModified] = useState<boolean>(false); 

  const dispatch = useDispatch();
  const showError = (str: string) => dispatch(setErrText(str));
  const showTip = (str: string) => dispatch(setTipText(str));
  const closeTip = () => dispatch(setTipText(''));

  const submit=async ()=>{
    if (isModified && !logoFile) { showError('Profile edit&$&No avatar selected.');return; }
    
    showTip('Profile edit&$&Submitting Request...')
    const formData=new FormData();
    if(logoFile) formData.append('file', logoFile);
    formData.append('userName', userName);
    formData.append('userDesc', bio);
    formData.append('userLink', userLink);
    formData.append('did', user.user_address);
    formData.append('userType', '0');
    
    try {
      const response = await fetch('/api/profile', {
          method: 'POST',
          body: formData
      });
    
      if(!response.ok){
        showError(`Profile edit&$&Data processing failed!-->${response.statusText||''}(${response.status??500})`);
        return;
      }

      const re = await response.json();
      const baseData={ user_name:userName,user_desc:bio,user_link:userLink}
      dispatch(setUser({...user,...baseData,...(re.avatarPath ? { user_avatar:re.avatarPath } : {})}));
      setSt("profileshow");
      
    } catch (error:any) 
    {
      closeTip();
      showError(`Profile edit&$&Data processing failed!\n${error.message||''}`);
    }
    finally
    {
      closeTip();
    }
  }

    return (
    <div className="mb-5"  style={{padding:'20px 0 0 40px',maxWidth:'800px'}} >
        <div className="mb-4" style={{fontSize:'39px',color:'#FFF'}} >Apply for Profile Status </div>
        <div style={{fontSize:'18px',fontWeight:700,color:'#FFF'}} >Basic Data</div>
        <div className="mb-1" style={{fontSize:'14px',fontWeight:400,color:'#6D6D6D'}} >Add your basic information  </div>

        <div className="mt-1 d-flex align-items-center gap-3 ">
            <LogoUploader maxSizeKB={2*1024} initialUrl={user.user_avatar} onChange={(file) => {setLogoFile(file);setIsModified(true);}}  /> 
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

        <div className="mb-5">

        <Button className="btn-custom px-2"  variant="dark" onClick={()=>setSt('profileshow')} >Back</Button>{' '}
            <Button variant="dark" className="btn-custom px-2"  onClick={submit} >Submit</Button>

        </div>

    </div>
  );
}

Edit.displayName = "Edit";
export default React.memo(Edit);