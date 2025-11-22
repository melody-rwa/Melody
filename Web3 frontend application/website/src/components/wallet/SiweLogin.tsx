"use client";
import React, {useImperativeHandle,useRef,forwardRef,ForwardRefRenderFunction} from "react";
import { SiweMessage } from "siwe";

export interface SiweResult {
  success: boolean;
  reason: "success" | "chainerr"| "error"| "timeout";
  err?: any;
}

export interface LoginButtonRef {
  siweLogin: (signer: any,chainId:number) => Promise<SiweResult>;

}

const SiweLogin: ForwardRefRenderFunction<LoginButtonRef> = (_, ref) => {

  const controllerRef = useRef<AbortController | null>(null);
  const resultResolver = useRef<((r: SiweResult) => void) | null>(null);

  async function createSiweMessage(signer: any,chainId:number): Promise<SiweMessage> {
    const res = await fetch(`/api/siwe/nonce`);
    const data = await res.json();
    return new SiweMessage({
      domain: window.location.host,
      address: await signer.getAddress(),
      statement: "Sign in with BSC to the Melody dApp.",
      uri: window.location.origin,
      version: "1",
      chainId: chainId,
      nonce: data.nonce,
    });
  }

  const siweLogin = (signer: any,chainId:number): Promise<SiweResult> => {
    return new Promise(async (resolve) => {
      if (!signer || !signer.signMessage) {
        resolve({ success: false, reason: "error", err:{message: "Not connected to wallet"} });
        return;
      }

      resultResolver.current = resolve;

      const controller = new AbortController();
      controllerRef.current = controller;

      try {

        const messageObj = await createSiweMessage(signer,chainId);
        const message = messageObj.prepareMessage();
        const signature = await signer.signMessage(message);
        const res = await fetch(`/api/siwe/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message, signature }),
          signal: controller.signal,
        });

        const data = await res.json();
        if (res.status !== 200) {
          resolve({
            success: false,
            reason: "error",
            err:{message: data.errMsg || "Signature verification failed"},
          });
        } else {
          // setShowModal(false)
          resolve({ success: true, reason: "success" });
        }
       
      } catch (err: any) {
       
        const errName=err.name|| err.Error;
        if (errName === "AbortError") {
          // setTimeoutHappened(true);
          clearResources();
         }
        else {
       
          resolve({ success: false, reason: "chainerr", err });
        }
      } finally {
        clearResources();
      }
    });
  };
  useImperativeHandle(ref, () => ({
    siweLogin
  }));


  const clearResources = () => {
    if (controllerRef.current) controllerRef.current.abort();
    // if (timeoutRef.current) clearTimeout(timeoutRef.current);
    // controllerRef.current = null;
    // timeoutRef.current = null;
    // setSignering(false);

  };
  return (
    <>
    </>
  );
};

export default forwardRef(SiweLogin);
