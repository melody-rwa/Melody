
import { execute } from "@/lib/mysql/common";
import { rateLimit } from "@/lib/rate-limit";
import { NextRequest, NextResponse } from "next/server";
import { getClientIp } from "@/lib/utils";
import { getSession } from "@/lib/session";
import { checkAddress } from "@/lib/utils";

export async function POST(req: NextRequest) {
   
    const session = await getSession();
    try {
        const clientIp = getClientIp(req);
        if (clientIp) {
            const isRateLimited = await rateLimit(clientIp, 'allplaylist', 10); // 5 times per minute
            if (isRateLimited) throw new Error('Too many requests')
        }
        const body = await req.json();
        const { musicId,did } = body;
        if(!checkAddress(did))  throw new Error("Parameter error!");
        if(!session || !session.did || session.did.toLowerCase()!==did.toLowerCase()) throw new Error("Unauthorized");
        
        execute("call in_play(?,?)",[did,musicId])
        return NextResponse.json(
            { success: true,message:'ok' },   { status: 200 }
        );
    
    } catch (err: any) {
        const message = err.message || "Unable to process data.";
        return NextResponse.json({ success: false, message },
        { status: 500,statusText: message,headers: { "Content-Type": "application/json" }}
        );
    }
}
