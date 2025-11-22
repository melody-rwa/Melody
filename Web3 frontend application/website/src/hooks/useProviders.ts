"use client";
import { useSyncExternalStore, useState, useEffect } from "react";

interface Store {
  value: () => WalletProviderType[];
  subscribe: (callback: () => void) => () => void;
  isInitialized: () => boolean;
  onInit: (callback: () => void) => void;
}

let providers: WalletProviderType[] = [];
let initialized = false;
let initCallbacks: (() => void)[] = [];

type AnnouncementEvent = CustomEvent & { detail: WalletProviderType };

const ALLOWED_WALLETS = ["metamask", "okxwallet", "okx", "okx wallet","coinbase","coinbase Wallet"];
const BLOCKED_WALLETS = ["pocket universe", "backpack"];

const isIOS =typeof navigator !== "undefined" &&
  /iPad|iPhone|iPod/.test(navigator.userAgent);


if (typeof window !== "undefined") {
  window.addEventListener("error", (e) => {
    if (e.message?.includes("Cannot redefine property: ethereum")) {
      console.warn("[Wallet Injection Warning]", e.message);
      e.preventDefault(); 
    }
  });
}

const store: Store = {
  value: () => providers,

  subscribe(callback: () => void) {
    const onAnnouncement = (ev: Event) => {
      const event = ev as AnnouncementEvent;
      const walletName = event.detail.info.name?.toLowerCase() ?? "";
      const rdns = event.detail.info.rdns?.toLowerCase() ?? "";

      const isAllowed =
        ALLOWED_WALLETS.some(
          (key) => walletName.includes(key) || rdns.includes(key)
        ) &&
        !BLOCKED_WALLETS.some(
          (key) => walletName.includes(key) || rdns.includes(key)
        );

      console.groupCollapsed(
        `%c[Wallet Detected] ${event.detail.info.name}`,
        "color:#00b894;font-weight:bold;"
      );
      console.log("🆔 uuid:", event.detail.info.uuid);
      console.log("🏷️ name:", event.detail.info.name);
      console.log("🔗 rdns:", event.detail.info.rdns);
      console.log("✅ allowed:", isAllowed);
      console.groupEnd();

      if (
        isAllowed &&
        !providers.find((p) => p.info.uuid === event.detail.info.uuid)
      ) {
        providers = [...providers, event.detail];
        console.log("💡 Added provider:", event.detail.info.name);
        console.table(
          providers.map((p) => ({
            uuid: p.info.uuid,
            name: p.info.name,
            rdns: p.info.rdns,
          }))
        );
        callback();
      }
    };

    window.addEventListener(
      "eip6963:announceProvider",
      onAnnouncement as EventListener
    );

    const triggerRequest = () => {
      try {
        console.log(
          `[EIP6963] Requesting providers... (isIOS=${isIOS})`
        );
        window.dispatchEvent(new Event("eip6963:requestProvider"));
      } catch (err) {
        console.warn("[Wallet Request Error]", err);
      }
    };

    if (isIOS) {
      setTimeout(triggerRequest, 500); 
    } else {
      triggerRequest();
    }

    setTimeout(() => {
      if (!initialized) {
        initialized = true;
        initCallbacks.forEach((fn) => fn());
        initCallbacks = [];
        console.log("[Wallet Store] Initialization complete.");
      }
    }, 0);

    return () => {
      window.removeEventListener(
        "eip6963:announceProvider",
        onAnnouncement as EventListener
      );
    };
  },

  isInitialized: () => initialized,

  onInit: (callback: () => void) => {
    if (initialized) callback();
    else initCallbacks.push(callback);
  },
};

export const useProviders = () => {
  const providers = useSyncExternalStore(store.subscribe, store.value, store.value);
  const [loading, setLoading] = useState(!store.isInitialized());

  useEffect(() => {
    if (loading) {
      store.onInit(() => setLoading(false));
    }
  }, [loading]);

  return [providers, loading] as const;
};
