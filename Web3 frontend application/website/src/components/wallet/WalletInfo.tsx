'use client';
import { Dropdown } from "react-bootstrap";
import ImageWithFallback from "../ImageWithFallback";
import { useEffect, useRef } from "react";
import { useLayout } from "@/contexts/LayoutContext";

  interface ChildProps { 
    connectWallet:(provider: WalletProviderType,v:boolean)=>void;
    providers: WalletProviderType[];
    connecting:boolean;
  }
  
  export default function WalletInfo({ connectWallet,providers,connecting}: ChildProps) {

    const providerRef=useRef<WalletProviderType>(null);
    const { setIsShowBtn } = useLayout();  


  useEffect(() => {

  
    if (providers.length > 0) {
      const providerName = window.sessionStorage.getItem("providerinfoname");
      
      if (providerName) {
        const savedProvider = providers.find(p => p.info.name === providerName);
        if (savedProvider) providerRef.current = savedProvider;
      }

      // if (window.sessionStorage.getItem("isLogin") === '1') {
        if(providerRef.current){
          connectWallet(providerRef.current,false);
        } 
        // if (window.sessionStorage.getItem("loginsiwe") === '1') recorLogin()
      // } 
       else
       {
         setIsShowBtn(true);
      }
    }
     // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [providers]);

  

  
  return (
    <Dropdown>  
            <Dropdown.Toggle 
               className="btn-custom no-caret"
                disabled={connecting}
                style={{ 
                  borderRadius: '25px', 
                  marginLeft: '16px',
                  width:'150px',
                  height:'42px',
                  justifyContent:'center',
                  display: 'flex',
                  // color:'#FFF',
                  // background:'#f87230',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                
                {/* <Image alt="wallet" src='/wallet.svg' width={18} height={18} />  */}
                <span >{connecting ? 'Connecting...' : 'Connect Wallet'}</span>
              
              </Dropdown.Toggle>
              
              <Dropdown.Menu className="custom-menu" style={{ maxHeight: '300px', overflowY: 'auto' }}>
                {providers.map(p => (
                  <Dropdown.Item 
                    key={p.info.uuid} 
                    onClick={() => connectWallet(p,true)}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '10px',
                      padding: '8px 12px'
                    }}
                  >
                    <ImageWithFallback 
                      src={p.info.icon} 
                      alt={p.info.name}  
                      width={24} 
                      height={24} 
                      style={{ borderRadius: '4px' }}
                    />
                    <span>{p.info.name}</span>
                  </Dropdown.Item>
                ))}
              </Dropdown.Menu>
    </Dropdown>
  );
}

