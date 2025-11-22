import { useEffect, useState } from 'react';

export interface MetaMaskState {
  available: boolean; 
  unlocked: boolean;  
  accounts: string[]; 
  loading: boolean;   
}

export function useMetaMaskStatus(): MetaMaskState {
  const [state, setState] = useState<MetaMaskState>({
    available: false,
    unlocked: false,
    accounts: [],
    loading: true,
  });

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const eth = (window as any).ethereum;
    if (!eth) {
      setState({ available: false, unlocked: false, accounts: [], loading: false });
      return;
    }

    let mounted = true;

    async function detect() {
      try {
        let unlocked = false;
        let accounts: string[] = [];

        if (eth._metamask?.isUnlocked) {
          try {
            unlocked = await eth._metamask.isUnlocked();
            if (unlocked) accounts = await eth.request({ method: 'eth_accounts' });
          } catch {
            accounts = await eth.request({ method: 'eth_accounts' });
            unlocked = accounts.length > 0;
          }
        } else {
          accounts = await eth.request({ method: 'eth_accounts' });
          unlocked = accounts.length > 0;
        }

        if (mounted) {
          setState({ available: true, unlocked, accounts, loading: false });
        }
      } catch (err) {
        console.error('MetaMask detect error', err);
        if (mounted) {
          setState({ available: true, unlocked: false, accounts: [], loading: false });
        }
      }
    }

    detect();

    const handleAccountsChanged = (accounts: string[]) => {
      setState((prev) => ({
        ...prev,
        unlocked: accounts.length > 0,
        accounts,
      }));
    };

    eth.on?.('accountsChanged', handleAccountsChanged);

    return () => {
      mounted = false;
      eth.removeListener?.('accountsChanged', handleAccountsChanged);
    };
  }, []);

  return state;
}
