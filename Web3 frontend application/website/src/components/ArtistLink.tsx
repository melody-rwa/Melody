import { ArtistLeft1, ArtistLeft2, ArtistLeft3, ArtistLeft4 } from "@/lib/jssvg/melosvg";
import Link from "next/link";
import { Button } from "react-bootstrap";

interface ArtistLinkProps {
  url?: string;
  index: number;
}

const Images = [ArtistLeft1, ArtistLeft2, ArtistLeft3, ArtistLeft4];
const bgs=['#FFF','#3C5A99','#1296DB','#BE347F'];
const cos=['#1296DB','#FFF','#FFF','#FFF']

export default function ArtistLink({ url, index }: ArtistLinkProps) {
  const Icon = Images[index];

  const geneUrl=()=>{
    if(!url) return "#";
    if(url?.startsWith('http')) return url;
    return `https://${url}`;

  }

  return (
    <div
      style={{
        flex: "1 1 0",           // Divide the space of the parent container equally
        minWidth: 0,             // Allow contraction
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
    {url?  <Link target="_blank"   href={geneUrl()} passHref >
        <Button
          variant="secondary"
          style={{
            margin: 0,
            padding: 0,
            background: "transparent",
            border: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: "100%",          
            maxWidth: "48px",       
          }}
        >
          <Icon
            fillColor={bgs[index]}
            color={cos[index]}
            style={{
              width: "100%",        
              height: "auto",      
              flexShrink: 1,        
              transition: "all 0.2s ease-in-out",
            }}
          />
        </Button>
      </Link>
      : <Button
      variant="secondary"
      disabled={true}
      className="pe-none"
      style={{
        margin: 0,
        padding: 0,
        background: "transparent",
        border: 0,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        width: "100%",          
        maxWidth: "48px",       
      }}
    >
      <Icon
        fillColor={"#999"}
        color={"#FFF"}
        style={{
          width: "100%",       
          height: "auto",      
          flexShrink: 1,       
          transition: "all 0.2s ease-in-out",
        }}
      />
    </Button>
    }
    </div>
  );
}
