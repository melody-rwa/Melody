"use client";

import {  useEffect, useState } from "react";
import Left from "./Left";
import './styles/launch.css'
import Upload from "./Upload";
import { useLayout } from "@/contexts/LayoutContext";
import { useDispatch, useSelector } from "react-redux";
import { RootState, setErrText, setMessageText, setTipText } from "@/store/store";
import Loading from "@/components/Loadding";
import { getDaismContract } from "@/lib/globalStore";
import dynamic from "next/dynamic";
import ShowErrorBar from "@/components/ShowErrorBar";
import { chainErr } from "@/lib/utils/js";

const Basic = dynamic(() => import('./Basic'), { 
  ssr: false,
  loading: () => <Loading />, 
});


export default function Lanuch() {
  const {isShowBtn}=useLayout();
  const [code,setCode]=useState('');
  const [logoFile, setLogoFile] = useState<File | null>(null);
  const [tokenName,setTokenName]=useState('');
  const [tokenSymbol,setTokenSymbol]=useState('');
  const [tokenDesc,setTokenDesc] = useState('');
  const [musicFile, setMusicFile] = useState<File | null>(null);
  const [musicDuration, setMusicDuration] = useState<number>(0);
  const [totalRaise,setTotalRaise]=useState(0);
  const [startTime,setStartTime]=useState('');
  const [twitter,setTwitter]=useState('');
  const [website,setWebsite]=useState('');
  const [tg,setTg]=useState('');
  const [check1,setCheck1]= useState<boolean | null>(null); 
  const [check2,setCheck2]= useState<boolean | null>(null); 
  const [check3,setCheck3]= useState<boolean | null>(null); 
  const [isValid, setIsValid] = useState<boolean | null>(null); 
  const [validtokenName, setValidtokenName] = useState<boolean | null>(null); 
  const [validtokenSymbol, setValidtokenSymbol] = useState<boolean | null>(null); 
  const [showInvit, setShowInvit] = useState(true); 
  const [invitReadOnly, setInvitReadOnly] = useState(false); 

  const [isOk,setIsOk]=useState(false);

  useEffect(()=>{
    if(code && tokenName && tokenSymbol && musicDuration>0 && logoFile && totalRaise>0 && check1 && check2 && check3){
      setIsOk(true);
    }
    else 
    {
      setIsOk(false);
    }
  },[
    code,tokenName,tokenSymbol,musicDuration,logoFile,totalRaise,check1,check2,check3
  ])



  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
  const [st,setSt]=useState('upload');

  const clearValue=()=>{
    setCode('');
    setLogoFile(null);
    setTokenName('');
    setTokenSymbol('');
    setTokenDesc('');
    setMusicFile(null);
    setMusicDuration(0);
    setTotalRaise(0);
    setStartTime('');
    setTwitter('');
    setWebsite('');
    setTg('');
    setCheck1(true);
    setCheck2(true);
    setCheck3(true);
    setSt('upload');
    setIsValid(null);
    setShowInvit(true);
    setInvitReadOnly(false);
    setValidtokenName(null);
    setValidtokenSymbol(null);


  }

  const dispatch = useDispatch();
  const showError = (str: string) => dispatch(setErrText(str));
  const showMessage = (str: string) => dispatch(setMessageText(str));
  const showTip = (str: string) => dispatch(setTipText(str));
  const closeTip = () => dispatch(setTipText(''));

  const checkInvite=async (flag:boolean=false)=>{
    if(!code){
      showError('Check invitation&$&You must enter an invitation code.');
      return false;
    }
    if(flag)  showTip('Check invitation&$&Verifying invitation code...')
    const daismObj=getDaismContract()
    try {
      const re=await daismObj?.MusicFactory.isInviteValid(code);
      if(!re) {
        showError('Check invitation&$&Invalid invite code ')
        return false;
      }
      if(flag)  showMessage("Check invitation&$&Verification code is valid.");
      return true;
    } catch (error:any) {
      console.error(error)
      showError(`Check invitation&$&An error occurred while submitting the request. Please check and handle it in your wallet!\n ${error.message||''}`);
      return false;
    }
    finally{
     if(flag) closeTip()
    }

  }
  
  
  const checkForm=async ()=>{
    try {
  
      if(!musicFile){
        showError('Launch&$&No music selected');
        return false;
      }
      if(!logoFile){
        showError('Launch&$&Token logo not provided');
        return false;
      }

      if(musicDuration<30){
        showError('Launch&$&Music duration cannot be less than 30 seconds');
        return false
      }

     
      if(!tokenName || tokenName.length>128){
        showError('Launch&$&Token name cannot be empty or greater than 128 characters');
        setValidtokenName(false)
        return false;
      }
      setValidtokenName(true)

      if(!tokenSymbol || tokenSymbol.length>8){
        showError('Launch&$&Token symbol cannot be empty or greater than 8 characters');
        setValidtokenSymbol(false)
        return false;
      }
      setValidtokenSymbol(true)

    

      if(totalRaise===0){
        showError('Launch&$&Funding amount not selected');
        return false;
      }
      if(!check1){
        showError('Launch&$&You must agree Disclaimer before submitting.');
        return false;
      }
      if(!check2){
        showError('Launch&$&You must agree 《Copyright Income Agreement Statement》before submitting.');
        return false;
      }
      if(!check3){
        showError('Launch&$&You must signing of Copyright Income Agreement for Music Work before submitting.');
        return false;
      }
      if(!checkInvite()) return;
    
      return true;
    } catch (error) {
      console.error(error);
      return false;
     }
  }

  const handleSubmit = async () => {
    const lok=await checkForm();
    if(!lok) return;

    let plannedSec=0;
    if(startTime) plannedSec=Math.ceil(Math.ceil(new Date(startTime).getTime()/1000-new Date().getTime()/1000));

    showTip('Post music&$&Submitting Request...')
    try {
      const form = new FormData();
      form.append('user_address', user.user_address);
      if(musicFile) form.append('music_file', musicFile);
      form.append('music_seconds', musicDuration.toString());
      if(logoFile) form.append('logo', logoFile);
      form.append('token_name',tokenName);
      form.append('token_symbol', tokenSymbol);
      form.append('token_desc', tokenDesc);
      form.append('website', website);
      form.append('twitter', twitter);
      form.append('telegram', tg);
      form.append('total_raise', totalRaise.toString());


      const res = await fetch('/api/music', {method: 'POST', body: form});
      if(!res.ok){
        showError(`Post music&$&Data processing failed!-->${res.statusText||''}(${res.status??500})`);
        closeTip();
        return;
      }

      const result = await res.json();
      const {insrtID,slat}=result;
      handleInChain(code,tokenName,tokenSymbol,totalRaise,musicDuration,insrtID,plannedSec,slat)
    } 
    catch (err:any) {
      closeTip(); 
      showError(`Post music&$&Data processing failed!-->${err.message||''}`);
    }
   
  };


  const handleInChain=async (_code:string,_name:string,_symbol:string,totalRaise:number,musicDuration:number,insrtID:number,plannedSec:number,slat:string)=>{   
    const  daismObj=getDaismContract();
    try {
      await daismObj?.MusicFactory.createMusic(
        _code,
       `https://${process.env.NEXT_PUBLIC_DOMAIN}/api/v1/${insrtID}`,
        totalRaise,
        musicDuration,
        _name,
        _symbol,
        plannedSec,slat);   

        showMessage("Post music&$&Saved on-chain successfully.");
        closeTip(); 
        clearValue();
    
    } catch (err:any) {
      console.error(err)
      closeTip(); 
      chainErr(err,showError,'Post music');
    }
   
    
  }



  return (
  <> 
    {isShowBtn ?
      <>{user.connected<1?
      <div style={{width:'100%',height:'100%'}} ><ShowErrorBar errStr='Wallet not connected'></ShowErrorBar></div>
      :
      <>{user.user_type!==1?
          <div style={{width:'100%',height:'100%'}} ><ShowErrorBar errStr='You haven’t registered as an artist yet.'></ShowErrorBar></div>
          :
          <div style={{display:'flex',flexDirection:'column',height:'100%'}} > 
          <div className="top-text mt-3 mb-3 px-3" >Launch Your Tokens</div>
          <div  style={{  flex: 1, display: 'flex' }} >
            <div className="melo-launch-left" >
              <div className="top-menu-text mt-3 mb-3" >Launch steps </div>
              <Left st={st} setSt={setSt} />
            </div>
            <div className="melo-launch-right">
            {st==='upload'?
            <Upload 
              code={code} setCode={setCode}
              logoFile={logoFile} setLogoFile={setLogoFile} 
              tokenName={tokenName} setTokenName={setTokenName}
              tokenSymbol={tokenSymbol} setTokenSymbol={setTokenSymbol}
              tokenDesc={tokenDesc} setTokenDesc={setTokenDesc}
              musicFile={musicFile} setMusicFile={setMusicFile} setMusicDuration={setMusicDuration}
              showError={showError}
              setSt={setSt}
              checkInvite={checkInvite}
              isValid={isValid}
              setIsValid={setIsValid}
              showInvit={showInvit}
              setShowInvit={setShowInvit}
              invitReadOnly={invitReadOnly}
              setInvitReadOnly={setInvitReadOnly}
              validtokenName={validtokenName} setValidtokenName={setValidtokenName}
              validtokenSymbol={validtokenSymbol} setValidtokenSymbol={setValidtokenSymbol}

              />
              :<Basic 
              totalRaise={totalRaise} setTotalRaise={setTotalRaise}
              startTime={startTime} setStartTime={setStartTime}
              twitter={twitter} setTwitter={setTwitter}
              website={website} setWebsite={setWebsite}
              tg={tg} setTg={setTg} isOk={isOk}
              check1={check1} setCheck1={setCheck1} 
              check2={check2} setCheck2={setCheck2} 
              check3={check3} setCheck3={setCheck3} 
              handleSubmit={handleSubmit}
              setSt={setSt} showError={showError} musicDuration={musicDuration}
              />
            }
            </div>
        </div>
          </div>
        }
      </>
      }
      </> 
      :<Loading />
    }
  </>
  );
}
 