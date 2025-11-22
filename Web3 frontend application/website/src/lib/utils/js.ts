export function generateMusicState(musicObj:MusicType):number{
  if(musicObj.start_time===0) return -1;
  if(musicObj.now_time<(musicObj.start_time+Number(process.env.NEXT_PUBLIC_COLLECTION)*60)) return 0;
  if(musicObj.now_time<(musicObj.start_time+Number(process.env.NEXT_PUBLIC_WHITELIST)*60)) return 1;
  if(musicObj.now_time<(musicObj.start_time+Number(process.env.NEXT_PUBLIC_INTERVAL)*60)) return 2;
  if(musicObj.now_time<(musicObj.start_time+Number(process.env.NEXT_PUBLIC_PUBLIC)*60)) return 3;
  return 4;
}


export function generateRemaining(st:number,nowst:number,expire_time:number,startS:number):number{
 
  let _time=0;
 
  switch(expire_time){
    case 0:
      _time=((st+(Number(process.env.NEXT_PUBLIC_COLLECTION)*60))-nowst-startS);
        break;
    case 1:
      _time=((st+(Number(process.env.NEXT_PUBLIC_WHITELIST)*60))-nowst-startS);
        break;  
    case 2:
      _time=((st+(Number(process.env.NEXT_PUBLIC_INTERVAL)*60))-nowst-startS);
        break; 
    case 3:
      _time=((st+(Number(process.env.NEXT_PUBLIC_PUBLIC)*60))-nowst-startS);
        break;  
    default:
        break;  
  }
  return _time;
}

export  function trimTrailingZeros(curstr:string|number) {
  const str=curstr.toString();
  return str
    .replace(/(\.\d*?[1-9])0+$/, '$1')
    .replace(/\.0+$/, '');             
}

export const formatTime = (seconds: number) => {
  if (isNaN(seconds)) return "00:00";
  const m = Math.floor(seconds / 60).toString().padStart(2, "0");
  const s = Math.floor(seconds % 60).toString().padStart(2, "0");
  return `${m}:${s}`;
};


export  function formatSeconds(seconds:number) {
  const now = new Date();
  const oneDay = 24 * 60 * 60;
  const oneHour = 60 * 60;
  const oneMinute = 60;

  if (seconds >= oneDay) {

    const pastDate = new Date(now.getTime() - seconds * 1000);
    const month = String(pastDate.getMonth() + 1).padStart(2, '0');
    const day = String(pastDate.getDate()).padStart(2, '0');
    return `${month}-${day}`;
  } else if (seconds >= oneHour) {
    const hours = Math.floor(seconds / oneHour);
    return `${hours}hr ago`;
  } else if (seconds >= oneMinute) {
    const minutes = Math.floor(seconds / oneMinute);
    return `${minutes}min ago`;
  } else {
    return `${seconds}s ago`;
  }
}


export function chainErr(err:any,showError:(v:string)=>void,tip:string)
{

  console.error(err.code);
  if(err.message) {
    const errStr=err.message.toString();
    if(errStr.startsWith('execution reverted'))
    {
      const m = errStr.match(/execution reverted:\s*"([^"]+)"/);
      const mess= m ? m[1] : '';
      showError(`${tip}&$&On-chain error :  ${mess}`)
      return;
    } 
    else 
    {
      if(errStr.includes(' rejected ')){
        showError(`${tip}&$&Transaction rejected by user`);
          return;
      }
      if(errStr.includes('insufficient ')){
        showError(`${tip}&$&Insufficient funds for gas`);
          return;
      }
    }
  }
  showError(`${tip}&$&Transaction discarded: gas too low or network congestion`);
}

export const checkAddress = (v: string): boolean => /^0x[A-Fa-f0-9]{40}$/.test(v);