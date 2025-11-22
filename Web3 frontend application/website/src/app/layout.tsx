
import "bootstrap/dist/css/bootstrap.min.css";
import "@/styles/globals.css";
import ReduxProvider from '@/store/Providers';
import Layout from '@/components/Layout';
import InitComponent from '@/components/InitComponent';
import { LayoutProvider } from '@/contexts/LayoutContext';

export default async function LocaleLayout({ children }: { children: React.ReactNode }) {
 
  return (
    <html data-bs-theme="dark"  lang="en">
      <body>
          <ReduxProvider>
            <InitComponent />
            <LayoutProvider>
              <Layout>
                {children}
              </Layout>
            </LayoutProvider>
          </ReduxProvider>

      </body>
    </html>
  );
}

export const metadata = {
  title: "Melody",
  description:
    "Melody is a blockchain-based music RWA platform that connects creators, fans, and investors through tokenized music assets. Users subscribe to meme tokens corresponding to music clips, obtaining attribution and revenue rights. Creators receive long-term returns through the LP pool and copyright income. The platform operates through commissions from fundraising and LP income.",
  keywords:
    "RWA;Music;DeFi;RWAFi;Meme; Web3; Launchpad; ICO; IDO; RWAFi; MusicFi;BSC;BNB Chain",
  alternates: {
    canonical: "https://melodylabs.io",
  },
  openGraph: {
    title: "Melody",
    description:
      "Melody: Blockchain-based music RWA platform connecting creators, fans, and investors.",
    url: "https://melodylabs.io",
    siteName: "MelodyLabs",
    images: [
      {
        url: "https://melodylabs.io/logo512.png",
        width: 512,
        height: 512,
        alt: "Melody Logo",
      },
    ],
    locale: "en_US",
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "Melody",
    description:
      "Melody: Blockchain-based music RWA platform connecting creators, fans, and investors.",
    images: ["https://melodylabs.io/logo512.png"],
  },
  icons: {
    icon: "/favicon.ico",
    shortcut: "/favicon.ico",
    apple: "/logo192.png",
  },
  manifest: "/manifest.json",
};
