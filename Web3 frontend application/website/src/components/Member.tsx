import Link from "next/link";
import ArtistLink from "./ArtistLink";
import ImageWithFallback from "./ImageWithFallback";


interface EnkiMemberProps {
    userName:string;
    avatar: string; 
    account:string; 
    twitter:string;
    facebook:string;
    tg:string;
    instgram:string;
}

export default function Member({userName,avatar,account,twitter,facebook,tg,instgram}: EnkiMemberProps) {


    return (
        <div className="melo-meber" style={{ display: "flex", alignItems: "center", gap:'16px' }}>
            <div>
                <Link href={`/artist/${account}`} >
                {/* <GeneImg avatar={avatar} hw={hw} /> */}
                <ImageWithFallback src={avatar} alt="" width={48} height={48} style={{borderRadius:'50%'}} />
                </Link>
            </div>
            <div>
                <div style={{overflow:'hidden'}} className="melo-meber-text1 mt-1 mb-1 text-ellipsis" >{userName}</div>
                <div style={{display:'flex',alignItems:'center',justifyContent:'space-between', gap:'10px', width:'110px'}} >
                    <ArtistLink url={twitter} index={0}   />
                    <ArtistLink url={facebook} index={1} />
                    <ArtistLink url={tg} index={2} />
                    <ArtistLink url={instgram} index={3} />
                </div>
            </div>
        </div>
    );
}
