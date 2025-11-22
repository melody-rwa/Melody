
import { useEffect, useRef, useState } from 'react';
import MetmaskInstall from './MetmaskInstall';
import WalletInfo from './WalletInfo';
import User from './User';
import {useDispatch, useSelector} from 'react-redux';
import {type RootState, type AppDispatch, setUser,setEthBalance,setErrText} from '@/store/store';
import { ethers } from 'ethers';
import { setDaismContract } from "@/lib/globalStore";
import DaoApi from '@/lib/contract';
import { useLayout } from '@/contexts/LayoutContext';
import { useProviders } from '@/hooks/useProviders';
import Loading from '../Loadding';
import { useWalletManager } from '@/hooks/useWalletManager';
import SiweLogin, {type LoginButtonRef} from './SiweLogin';
import { chainErr } from '@/lib/utils/js';
import { Modal } from 'react-bootstrap';
import Image from 'next/image';
import Loader from '../Loader';

interface ReUserInfo{
  singed:boolean;
  data:UserInfo[];
}

// const TARGET_CHAIN = {
//   chainId: 11155111, // Sepolia
//   chainName: "Sepolia Testnet",
//   rpcUrls: [process.env.NEXT_PUBLIC_HTTPS_URL as string],
//   nativeCurrency: { name: "Sepolia ETH", symbol: "SEP", decimals: 18 },
//   blockExplorerUrls: ["https://sepolia.etherscan.io"],
// };

const TARGET_CHAIN = {
  chainId: 97,
  chainName: "BNB Chain Testnet",
  rpcUrls: ["https://bsc-testnet.publicnode.com"],
  nativeCurrency: { name: "Test BNB", symbol: "tBNB", decimals: 18 },
  blockExplorerUrls: ["https://testnet.bscscan.com"],
};

// const TARGET_CHAIN = {
//   chainId: 56,
//   chainName: "BNB Smart Chain Mainnet",
//   rpcUrls: ["https://bsc-dataseed.binance.org/"],
//   nativeCurrency: { name: "BNB", symbol: "BNB", decimals: 18 },
//   blockExplorerUrls: ["https://bscscan.com"],
// };


function Wallet() {
    const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
    const dispatch = useDispatch<AppDispatch>();
    const { setIsShowBtn } = useLayout(); 
    const showError=(str:string)=>{ dispatch(setErrText(str));}
    const [providers, loading] = useProviders();
    const loginRef = useRef<LoginButtonRef>(null);
    const [signering,setSignering]=useState(false);
    const [showModal, setShowModal] = useState(false);
    const currentSignerRef = useRef<any>(null);
    const chainIdRef = useRef<number>(0);

const connectWallet1 =async (account: string,provider: ethers.BrowserProvider) => {
    const signer = await provider.getSigner();
    const network = await provider.getNetwork();
    const chainId = Number(network.chainId);
    currentSignerRef.current=signer;
    chainIdRef.current=chainId;
   

    fetch("/api/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json", 
      },
      body: JSON.stringify({
        did:account,   
        time: Date.now(),
      }),
    })
    .then((res) => {
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        return res.json();
    })
    .then(async (re: ReUserInfo) => {
      const {data,singed}=re;
    

      if(!singed){ 
        setShowModal(true);
        if(signering) return;
        setSignering(true);
        const result = await loginRef.current?.siweLogin(signer,chainId);
 
        if(result?.reason!=='success'){
         
            setShowModal(false);
            setSignering(false);
            if(result?.err.message==='Signature verification failed'){
              showError(`Wallet signature&$&Signature verification failed`)
            } else    chainErr(result?.err,showError,'Wallet signature');

          return;
        }
        else 
        {
          setShowModal(false);
          setSignering(false);
        }
      }
      const daismObj=new DaoApi(signer,account);
      setDaismContract(daismObj); 

      dispatch(
        setUser({
          connected: 1,
          networkName: network.name,
          chainId,
          user_address:data[0].user_address,
          user_avatar: data[0].user_avatar,
          user_name: data[0].user_name,
          user_desc: data[0].user_desc,
          user_type: data[0].user_type,
          user_link:data[0].user_link,
          twitter:data[0].twitter,
          tg:data[0].tg,
          facebook:data[0].facebook,
          instgram:data[0].instgram,
          artist_avatar:data[0].artist_avatar,
          artist_name:data[0].artist_name,
          artist_desc:data[0].artist_desc,
          artist_link:data[0].artist_link
        })
      );
      setIsShowBtn(true);
    })
    .catch((err: any) => {
      setIsShowBtn(true);
      console.error(err);
    })
   

    provider.getBalance(account).then((balance:bigint) => { dispatch(setEthBalance(ethers.formatEther(balance))); });
    
  }

  const onDisconnect=async ()=> {
    dispatch(setEthBalance('0'))
    dispatch(setUser({connected:0,chainId:0,networkName:'',user_address:'',
      user_avatar:'',user_name:'',user_desc:'',user_type:0,user_link:'',
      twitter:'',tg:'',facebook:'',instgram:'',
      artist_name:'',artist_avatar:'',artist_desc:'',artist_link:'' }));
    setDaismContract(undefined);  
    await fetch('/api/logout', {
      method: 'POST',
      credentials: 'include',
    });
   
    
  };

  const {  connecting, connectWallet, disconnectWallet } = useWalletManager({
    targetChain: TARGET_CHAIN,
    debug: true,
    showError:(str:string)=>{ setShowModal(false); dispatch(setErrText(str));},
    setIsShowBtn,
    onConnected:connectWallet1,
    onDisconnected:onDisconnect
  });

  
  useEffect(()=>{
    if(!loading && providers.length===0){
      setIsShowBtn(true)
    }
//eslint-disable-next-line react-hooks/exhaustive-deps
  },[loading,providers])


  const stopSiwe = () => {
    // if (stopRequestedRef.current) return;
    // stopRequestedRef.current = true;
    // clearResources();
    setShowModal(false);
    // if (resultResolver.current)
    //   resultResolver.current({ success: false, reason:'error', err:{message: "User cancels the signature"}});
  };


  return (
    <>
      <div className='d-flex justify-content-start align-items-center' style={{ minWidth: '220px'}}> 
        <div style={{ marginTop: '6px', marginRight: '10px' }}>
          {user.connected > 0 ? 
         
            <User onDisconnect={disconnectWallet}/>
  
          :<>
            {loading?
              <Loading />
              :providers.length > 0 ? <WalletInfo providers={providers} connectWallet={connectWallet} connecting={connecting} /> 
              : <MetmaskInstall  />
            }
          </>
          }
        </div>
      <SiweLogin ref={loginRef} />
        {/* <LocaleSwitcher /> */}
      </div>   
      <Modal size='sm' contentClassName="melo-dailog-parent" show={showModal} onHide={stopSiwe} centered backdrop="static">
        <Modal.Body className="text-center melo-dailog">
        <div className='melo-dailog-inner'>
          <div className='melo-flex-between mb-3' >
                   <span className='melo-dailog-title' >Wallet signature</span>
                   <div className='melo-dailog-title-close-btn' onClick={stopSiwe} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
          </div>

          <div className='mt-5 mb-3' style={{display:'flex',alignItems:'center',justifyContent:'center'}} >
                 <Loader/>
                 </div>
            
            <div className='melo-login-text p-4' >
            Connection authentication in progress,This will take a few minutes, please be patient.....
            </div>
            
          </div>
        </Modal.Body>

      
      </Modal>  
    </>
  );
}

export default Wallet;
