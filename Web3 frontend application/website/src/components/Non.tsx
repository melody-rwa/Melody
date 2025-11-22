import Image from "next/image";

export default function Non(){
    return(
        <div className="melo-loadding" >
            <Image alt="" width={124} height={124} src="/non.svg" />
            <div className="mt-1" style={{color:'#838383',fontSize:'14px',fontWeight:400}} >No data is available</div>
            
        </div>
    );
}
