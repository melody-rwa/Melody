
import { NextRequest, NextResponse } from 'next/server';
import { getClientIp } from '@/lib/utils';
import { rateLimit } from '@/lib/rate-limit';
import { geneUser } from '@/lib/mysql/message';
import { getSession } from '@/lib/session';


export async function POST(req: NextRequest) {
  const clientIp = getClientIp(req);
    // Rate limit check
  if (clientIp) {
    const isRateLimited = await rateLimit(clientIp, 'login', 10); // 5 times per minute
    if (isRateLimited) {
      return NextResponse.json(
        { error: 'Too many requests' },
        { status: 429 }
      );
    }
  }
  try {
    const body = await req.json();
    const { did } = body;
    const session = await getSession();
    const re=await geneUser({did});
    let singed =false;
   
    if(session && session.ip === clientIp && session.did.toLowerCase()===did.toLowerCase() && session.userAgent === req.headers.get('user-agent'))
    {
      singed=true;
    }
    return NextResponse.json({data:re,singed});
   
  } catch (error: any) {
    console.error('SIWE login error:', error);
    return NextResponse.json(
      { errMsg: error.message || 'Internal server error' },
      { status: 500 }
    );
  }
}