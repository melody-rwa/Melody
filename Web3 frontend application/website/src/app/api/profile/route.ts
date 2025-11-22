import { NextRequest, NextResponse } from 'next/server';
import { getClientIp, saveImage } from '@/lib/utils';
import { execute } from '@/lib/mysql/common';
import { rateLimit } from '@/lib/rate-limit';
import { getSession } from '@/lib/session';
import { checkAddress } from '@/lib/utils';

export async function POST(req: NextRequest) {
 
  try {
    const clientIp = getClientIp(req);
    if (clientIp) {
      const isRateLimited = await rateLimit(clientIp, 'profile', 10); // 5 times per minute
      if (isRateLimited)  throw new Error("Too many requests")
    }
    const formData = await req.formData();
    const userName = formData.get('userName') as string;
    const userDesc = formData.get('userDesc') as string;
    const userType = formData.get('userType') as string;
    const userLink = formData.get('userLink') as string;
    const twitter = formData.get('twitter') as string;
    const facebook = formData.get('facebook') as string;
    const tg = formData.get('tg') as string;
    const instgram = formData.get('instgram') as string;
    const did = formData.get('did') as string;
    const session = await getSession();
    if(!session || !session.did || session.did.toLowerCase()!==did.toLowerCase()) throw new Error("Unauthorized");
    const file = formData.get("file") as File; //profile picture
    let avatarPath='';
    if (file instanceof File) {
      const {imgPath,} = await saveImage(file,'avatar'); //modify the image
      avatarPath=imgPath;
    }
    // await new Promise(resolve => setTimeout(resolve, 5000));
    if(!checkAddress(did)) throw new Error("Parameter error!")
    let re=0;
    const sql=userType==='1'
    ?`update t_user set ${avatarPath?'artist_avatar=?,':''}artist_name=?,artist_desc=?,artist_link=?,twitter=?,facebook=?,tg=?,instgram=? where user_address=?`
    :`update t_user set ${avatarPath?'artist_avatar=?,':''}user_name=?,user_desc=?,user_link=? where user_address=?`;
    
    const paras=userType==="1"
    ?[userName,userDesc,userLink,twitter,facebook,tg,instgram,did.toLowerCase()]
    :[userName,userDesc,userLink,did.toLowerCase()];

    if(avatarPath) paras.unshift(avatarPath);
    re=await execute(sql,paras);
    if(!re) throw new Error("save fail!");
    return NextResponse.json({success:true,message:'ok',...(avatarPath ? { avatarPath } : {})});
  } catch (err: any) 
  {
    const message = err.message || "Unable to process data.";
    return NextResponse.json({ success: false, message },
      { status: 500,statusText: message,headers: { "Content-Type": "application/json" }}
    );

  }
}
