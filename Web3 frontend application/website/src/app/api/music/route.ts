import { NextRequest, NextResponse } from 'next/server';
import { getClientIp, saveImage } from "@/lib/utils";
import { executeID } from '@/lib/mysql/common';
import { getSession } from '@/lib/session';
import { rateLimit } from '@/lib/rate-limit';
import { checkAddress } from '@/lib/utils';
import { type QueryResult,getSlat } from '@/lib/mysql/message';

export async function POST(req: NextRequest) {
 
  try {
    const clientIp = getClientIp(req);
    if (clientIp) 
    {
      const isRateLimited = await rateLimit(clientIp, 'music', 10); // 5 times per minute
      if (isRateLimited)  throw new Error("Too many requests");
    }
    const session = await getSession();
    const formData = await req.formData();
    const user_address = formData.get("user_address") as string;
    if(!checkAddress(user_address))  throw new Error("Parameter error!");
    if(!session || !session.did || session.did.toLowerCase()!==user_address.toLowerCase()) throw new Error("Unauthorized");
    const music_file = formData.get("music_file") as File; 
    const music_seconds = formData.get("music_seconds") as string;
    const logo = formData.get("logo") as File; 
    const token_name = formData.get("token_name") as string;
    const token_symbol = formData.get("token_symbol") as string;
    const token_desc = formData.get("token_desc") as string;
    const website = formData.get("website") as string;
    const twitter = formData.get("twitter") as string;
    const telegram = formData.get("telegram") as string;
    const total_raise = formData.get("total_raise") as string;
    const {imgPath:music_path,fileName:musicName} =await saveImage(music_file,'music') ;
    const {imgPath:logo_path,fileName:logoName} =await saveImage(logo,'logo') ;
    console.info(logoName)
    
    const sql="INSERT INTO t_music(user_address,music_url,music_name,total_raise,music_seconds,token_logo,token_name,token_symbol,token_desc,website,twitter,telegram) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
    const paras=[user_address,music_path,musicName,total_raise,music_seconds,logo_path,token_name,token_symbol,token_desc,website,twitter,telegram]
    const insrtID=await executeID(sql,paras);
    if(insrtID>0){
      const slatParent:QueryResult=await getSlat();
      const [slatAr]=slatParent;
      if(slatAr.length>0 )
      return NextResponse.json({success: true, message:'ok',insrtID,slat:slatAr[0].slat},{ status: 200 });
      else
      return NextResponse.json({ success: false, message: "No slat for use!" },{ status: 500 }  );
    } else {
      return NextResponse.json({ success: false, message: "insert fail" },{ status: 500 }  );
    }
     
  } catch (err:any) {
    const message = err.message || "Unable to process data.";
    return NextResponse.json({ success: false, message },
      { status: 500,statusText: message,headers: { "Content-Type": "application/json" }}
    );
  }
}
