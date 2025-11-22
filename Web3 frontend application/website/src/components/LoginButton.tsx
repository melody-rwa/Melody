import React, { useImperativeHandle, useState, forwardRef, ForwardRefRenderFunction } from "react";
import { SiweMessage } from 'siwe';
import { useSelector, useDispatch } from 'react-redux';
import {type RootState, type AppDispatch, setErrText, setTipText} from '@/store/store';
import { chainErr } from "@/lib/utils/js";

interface LoginButtonProps {

  command?: boolean; 
}

export interface LoginButtonRef {
  siweLogin: (signer:any) => Promise<boolean>;
}

const LoginButton: ForwardRefRenderFunction<LoginButtonRef, LoginButtonProps> = (props, ref) => {
  const [singering, setSingering] = useState(false); 
  const user = useSelector((state: RootState) => state.valueData.user);
  const dispatch = useDispatch<AppDispatch>();
  const showTip = (str: string) => dispatch(setTipText(str));
  const closeTip = () => dispatch(setTipText(''));
  const showMessage = (str: string) => dispatch(setErrText(str));
  const showError = (str: string) => dispatch(setErrText(str));

  async function createSiweMessage(signer:any): Promise<SiweMessage> {
    const res = await fetch(`/api/siwe/nonce`);
 
    const data = await res.json();
    

    return new SiweMessage({
      domain: window.location.host,
      address: await signer.getAddress(),
      statement: 'Sign in with BSC to the Melody dApp.',
      uri: window.location.origin,
      version: '1',
      chainId: user.chainId,
      nonce:data.nonce
    });
  }

  const siweLogin = async (signer:any): Promise<boolean> => {
  
    // if (singering) return;
    if (!signer || !signer.signMessage){
        showError('Login&$&Wallet not connected');
        return false;
    }
    const controller = new AbortController();
    showTip('Login&$&Signing in progress.....');
    setSingering(true);
    
    try {
      const messageObj = await createSiweMessage(signer);
      const message = messageObj.prepareMessage();
      const signature = await signer.signMessage(message);
   
      const timeoutId = setTimeout(() => controller.abort(), 20000);
      try {
        const res = await fetch(`/api/siwe/login`, {
          method: "POST",
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message, signature }),
          signal: controller.signal,
        });
        clearTimeout(timeoutId);
        const data = await res.json();

        if (res.status !== 200) {
          console.error(data.errMsg);
          showError(`Login&$&Signature verification failed \n ${data.errMsg} `);
          return false;
        } else {
          // if (!props?.second_login) {
           
            window.sessionStorage.setItem("loginsiwe", "1");
            return true;
        
          // }
        }
      } catch (error) {
        clearTimeout(timeoutId);
        if ((error as Error).name === 'AbortError') {
          
          showMessage('Login&$&Login timed out, please log in again!');
        } else {
          console.error(error);
        }
        return false;
      } finally {
        setSingering(false);
        closeTip()
      }
    } catch (err:any) {
      setSingering(false);
      closeTip()
      chainErr(err,showError,'Signature');
      return false;
    }
  };


  useImperativeHandle(ref, () => ({ siweLogin }));

  return (
    <>
      {!props.command && (
        <>
          {singering ? (
            <>
              <span className="spinner-grow spinner-grow-sm" role="status" aria-hidden="true"></span> 
              Signing in progress...
            </>
          ) : (
            <span>login</span>
          )}
        </>
      )}
    </>
  );
};

export default forwardRef(LoginButton);