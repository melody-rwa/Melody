"use client";
import { ethers } from "ethers";
import type { RefObject } from "react";


export interface WalletProviderType {
  info: {
    uuid: string;
    name: string;
    icon?: string;
    rdns?: string;
  };
  provider: any;
}

export interface TargetChainParams {
  chainId: number; 
  chainName: string;
  rpcUrls: string[];
  nativeCurrency: { name: string; symbol: string; decimals: number };
  blockExplorerUrls?: string[];
}

interface WalletManagerOptions {
  setAccount?: (a: string | null) => void;
  setChainId?: (c: number | null) => void;
  showError?: (msg: string) => void;
  onConnected?: (account: string, provider: ethers.BrowserProvider) => void;
  onDisconnected?: () => void;
  setIsShowBtn?:(v:boolean)=>void;
  onSwitch?:()=>void;
  debug?: boolean;
  targetChain?: TargetChainParams; 
}


export class WalletManager {
  private providerRef: RefObject<WalletProviderType | null>;
  private netWorkSwitchRef: RefObject<(() => void) | null>;
  private userSwitchRef: RefObject<(() => void) | null>;
  private opts: WalletManagerOptions;
  public connecting:boolean;

  constructor(
    providerRef: RefObject<WalletProviderType | null>,
    netWorkSwitchRef: RefObject<(() => void) | null>,
    userSwitchRef: RefObject<(() => void) | null>,
    opts: WalletManagerOptions = {}
  ) {
    this.providerRef = providerRef;
    this.netWorkSwitchRef = netWorkSwitchRef;
    this.userSwitchRef = userSwitchRef;
    this.opts = opts;
    this.connecting=true;
  }

  private log(...args: any[]) {
    if (this.opts.debug) console.log("%c[WalletManager]", "color:#0bf;", ...args);
  }
/** ============================
 * Force switching or adding target network (Edge compatibility + timeout recovery)
 * ============================ */
private async switchOrAddTargetChain(provider: any) {
  if (!this.opts.targetChain) return;

  const { chainId, chainName, rpcUrls, nativeCurrency, blockExplorerUrls } =
    this.opts.targetChain;
  const hexChainId = "0x" + chainId.toString(16);

  const isEdge =
    typeof navigator !== "undefined" && /Edg\//.test(navigator.userAgent);
  const timeoutMs = 8000;

  const raceTimeout = (ms: number) =>
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("wallet_switchEthereumChain timeout")), ms)
    );

  this.log(`Try switching to a different network: ${chainName} (${hexChainId})`);

  try {
    await Promise.race([
      provider.request({
        method: "wallet_switchEthereumChain",
        params: [{ chainId: hexChainId }],
      }),
      raceTimeout(timeoutMs),
    ]);

    if (isEdge) {
      this.log("🕓 Edge detected — waiting for wallet sync...");
      await new Promise((res) => setTimeout(res, 1000));
    }

    this.log(`Successfully switched to network : ${chainName}`);
  } catch (err: any) {
    // Timeout or other errors
    if (err.message?.includes("timeout")) {
      this.log(`Wallet switching timeout (${timeoutMs}ms)`);

      try {
        const tempProvider = new ethers.BrowserProvider(provider);
        const currentNetwork = await tempProvider.getNetwork();
        const currentId = Number(currentNetwork.chainId);
        if (currentId === chainId) {
          this.log("The wallet is already on the target network, continuing the connection process.");
          return;
        }
      } catch {
        this.log("Check current network failure, possibly wallet unresponsive.");
      }

 
      this.opts.showError?.(
        `Wallet connect&$&Edge Failed to switch the wallet network. Please switch to ${chainName}（Chain ID: ${chainId}）。`
      );
      return;
    }

    if (err.code === 4902) {
      this.log(` Wallet not added ${chainName}，Adding ..`);
      try {
        await provider.request({
          method: "wallet_addEthereumChain",
          params: [
            {
              chainId: hexChainId,
              chainName,
              rpcUrls,
              nativeCurrency,
              blockExplorerUrls,
            },
          ],
      });
        this.log(`Added and switched to ${chainName}`);
      } catch (addErr) {
        this.log(" Adding network failed:", addErr);
        this.opts.showError?.(
          `Wallet connect &$&Failed to add the network. Please add  ${chainName}（Chain ID: ${chainId}） manually`
        );
        throw addErr;
      }
    } else {
      this.log(" Network switching failed:", err);
      this.opts.showError?.(
        `Wallet connect &$&Please manually switch to ${chainName}  ID: ${chainId}）。`
      );
      throw err;
    }
  }
}


  /** ============================
 * Connect wallet
 * ============================ */
async connectWallet(providerWithInfo: WalletProviderType,v:boolean) {
  this.connecting = true;
  this.providerRef.current = providerWithInfo;
  this.log("🔗 Connecting wallet:", providerWithInfo.info.name);

  try {
    if(providerWithInfo.info.name.toLowerCase()==='metamask' && v){
      await providerWithInfo.provider.request({
        method: "wallet_requestPermissions",
        params: [{ eth_accounts: {} }],
      });
    }
  
    const accounts: string[] = await providerWithInfo.provider.request({
      method: "eth_requestAccounts",
    });
    const account = accounts?.[0];
    if (!account) throw new Error("No accounts found");


    let ethersProvider = new ethers.BrowserProvider(providerWithInfo.provider);
    // let signer = await ethersProvider.getSigner();
    let network = await ethersProvider.getNetwork();
    let chainId = Number(network.chainId);

    // =============================
    // Automatically switch or add target networks
    // =============================
    if (this.opts.targetChain && chainId !== this.opts.targetChain.chainId) {
      this.log(`current network (${chainId}) Does not match the target network (${this. opts. targetChain. chainId}), switching ..`);
      await this.switchOrAddTargetChain(providerWithInfo.provider);

      await new Promise((res) => setTimeout(res, 800)); // Waiting for the wallet to complete the switch

      ethersProvider = new ethers.BrowserProvider(providerWithInfo.provider);
      // signer = await ethersProvider.getSigner();
      network = await ethersProvider.getNetwork();
      chainId = Number(network.chainId);
    }

    this.log("✅ Connected:", { account, chainId });

    this.setupChainChangeListener(providerWithInfo.provider);
    this.setupUserSwitchListener(providerWithInfo.provider);

    this.opts.setAccount?.(account);
    this.opts.setChainId?.(chainId);
    this.opts.onConnected?.( account, ethersProvider );

      sessionStorage.setItem("providerinfoname", providerWithInfo.info.name);
      sessionStorage.setItem("connectedAccount", account);
      // sessionStorage.setItem("isLogin", "true");
  } catch (error) {
    console.error(" connect error:", error);
    this.opts.showError?.('Wallet connect&$&'+(error as Error).message || "Failed to connect wallet");
  } finally {
    this.connecting = false;
  }
}


  /** ============================
   * Automatically restore wallet connection status
   * ============================ */
  async autoReconnectWallet() {
    this.connecting=true
    try { 
      const providerName = sessionStorage.getItem("providerinfoname");
      const savedAccount = sessionStorage.getItem("connectedAccount");
      if (!providerName) {this.opts.setIsShowBtn?.(true); return;}

      const w = window as any;
      let provider: any = null;


if (w.ethereum && providerName.toLowerCase().includes("metamask")) {

  provider = w.ethereum;
} else if (w.okxwallet && providerName.toLowerCase().includes("okx")) {

  provider = w.okxwallet;
} else if (w.bitkeep?.ethereum && providerName.toLowerCase().includes("bitget")) {

  provider = w.bitkeep.ethereum;
} else if (
  (w.coinbaseWalletExtension && providerName.toLowerCase().includes("coinbase")) ||
  (w.ethereum?.isCoinbaseWallet && providerName.toLowerCase().includes("coinbase"))
) {

  provider = w.coinbaseWalletExtension || w.ethereum;
}


      // if (w.ethereum && providerName.toLowerCase().includes("metamask")) provider = w.ethereum;
      // else if (w.okxwallet && providerName.toLowerCase().includes("okx")) provider = w.okxwallet;
      // else if (w.bitkeep?.ethereum && providerName.toLowerCase().includes("bitget")) provider = w.bitkeep.ethereum;

      if (!provider)  {this.opts.setIsShowBtn?.(true); return;};

      this.providerRef.current = { info: { uuid: "", name: providerName }, provider };
      const accounts: string[] = await new Promise((resolve) => {
        setTimeout(async () => {
          try {
            const accs = await provider.request({ method: "eth_accounts" });
            resolve(accs);
          } catch {
            resolve([]);
          }
        }, 600);
      });

      // const accounts: string[] = await provider.request({ method: "eth_accounts" });
      if (!accounts?.length)  {this.opts.setIsShowBtn?.(true); return;};

      const account = accounts[0];
       //  If the cached account does not match the current wallet account, it will not be automatically restored 
       // (to avoid automatically switching to the wrong account)
    if (savedAccount && account.toLowerCase() !== savedAccount.toLowerCase()) {
      console.warn("[autoReconnectWallet] Account inconsistency detected, skip automatic login");
      this.opts.setIsShowBtn?.(true);
      return;
    }

  let ethersProvider = new ethers.BrowserProvider(provider);
  const signer = await ethersProvider.getSigner();
  let network = await ethersProvider.getNetwork();
  let chainId = Number(network.chainId);

  if (this.opts.targetChain && chainId !== this.opts.targetChain.chainId) {
    this.log(`The current network (${chainId}) does not match the target network (${this. opts. targetChain. chainId}), switching ..`);
    await this.switchOrAddTargetChain(provider);

    await new Promise((res) => setTimeout(res, 600)); 

    ethersProvider = new ethers.BrowserProvider(provider);
    // signer = await ethersProvider.getSigner();
    network = await ethersProvider.getNetwork();
    chainId = Number(network.chainId);
  }

      // if (this.opts.targetChain && chainId !== this.opts.targetChain.chainId) {
      //   await this.switchOrAddTargetChain(provider);
      //   network = await ethersProvider.getNetwork();
      //   chainId = Number(network.chainId);
      // }

      this.setupChainChangeListener(provider);
      this.setupUserSwitchListener(provider);

      // sessionStorage.setItem("isLogin", "true");
      this.opts.setAccount?.(account);
      this.opts.setChainId?.(chainId);
      this.opts.onConnected?.(account, ethersProvider);

      return { account, chainId, ethersProvider, signer };
  } catch (error) {
      console.error(error)
  }finally{
    this.connecting=false;
  }
  }

  /** ============================
   * Disconnect wallet connection
   * ============================ */
  disconnectWallet() {
    sessionStorage.removeItem("providerinfoname");
    sessionStorage.removeItem("connectedAccount");

    this.providerRef.current = null;

    this.netWorkSwitchRef.current?.();
    this.netWorkSwitchRef.current = null;

    this.userSwitchRef.current?.();
    this.userSwitchRef.current = null;

    this.opts.setAccount?.(null);
    this.opts.setChainId?.(null);
    this.opts.onDisconnected?.();
  }
/** ============================
 * Network Switching Monitoring (Enhanced Version)
 * ============================ */
private setupChainChangeListener(provider: any) {

  this.netWorkSwitchRef.current?.();

  const handler = async (chainIdHex: string) => {
    const chainId = parseInt(chainIdHex, 16);
    this.log(`Detected network switch: ${chainId}`);


        this.disconnectWallet();
  
  };

  provider.on?.("chainChanged", handler);
  this.netWorkSwitchRef.current = () => provider.removeListener?.("chainChanged", handler);
}


  /** ============================
   * Account switching monitoring
   * ============================ */
  private setupUserSwitchListener(provider: any) {

    this.userSwitchRef.current?.();

    const handler = async (accounts: string[]) => {
      const newAccount = accounts?.[0] ?? null;
      this.opts.setAccount?.(newAccount);

      if (!newAccount) {
        this.disconnectWallet();
        this.opts.setIsShowBtn?.(true);
        return;
      }

      if (this.providerRef.current) {
        const ethersProvider = new ethers.BrowserProvider(this.providerRef.current.provider);
        this.opts.onConnected?.(newAccount, ethersProvider);
        this.opts.onSwitch?.();
        sessionStorage.setItem("connectedAccount", newAccount);

      }
    };

    provider.on?.("accountsChanged", handler);
    this.userSwitchRef.current = () => provider.removeListener?.("accountsChanged", handler);
  }
}
