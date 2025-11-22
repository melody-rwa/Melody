"use client";
import { useRef, useState, useCallback } from "react";
import { ethers } from "ethers";
import { WalletManager, WalletProviderType, TargetChainParams } from "../components/wallet/walletManager";

interface UseWalletManagerOptions {
  targetChain?: TargetChainParams;
  debug?: boolean;
  onConnected?: (account: string, provider: ethers.BrowserProvider) => void;
  onDisconnected?: () => void;
  showError?: (msg: string) => void;
  setIsShowBtn?:(v:boolean)=>void;
  onSwitch?:()=>void; 
}

export const useWalletManager = (opts: UseWalletManagerOptions = {}) => {
  const providerRef = useRef<WalletProviderType | null>(null);
  const netRef = useRef<(() => void) | null>(null);
  const userRef = useRef<(() => void) | null>(null);

  const [account, setAccount] = useState<string | null>(null);
  const [chainId, setChainId] = useState<number | null>(null);
  const [connecting, setConnecting] = useState(false);

  const walletManagerRef = useRef<WalletManager>(
    new WalletManager(providerRef, netRef, userRef, {
      setAccount,
      setChainId,
      showError: opts.showError,
      targetChain: opts.targetChain,
      debug: opts.debug,
      setIsShowBtn:opts.setIsShowBtn,
      onConnected: opts?.onConnected,
      onDisconnected:opts?.onDisconnected,
      onSwitch:opts?.onSwitch
    })
  );

  // ============================
  // Connect wallet
  // ============================
  const connectWallet = useCallback(async (provider: WalletProviderType,v:boolean) => {
    setConnecting(true);
    try {
      return await walletManagerRef.current.connectWallet(provider,v);
    } finally {
      setConnecting(false);
    }
  }, []);


  const disconnectWallet = useCallback(() => {
    walletManagerRef.current.disconnectWallet();
  }, []);

  return {
    account,
    chainId,
    connecting,
    connectWallet,
    // autoReconnectWallet,
    disconnectWallet,
  };
};
