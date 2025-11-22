
import { NextRequest, NextResponse } from 'next/server';
import { getSession } from '@/lib/session';
import { getClientIp } from '@/lib/utils';

export async function GET(req:NextRequest) {
  try {
    const session = await getSession();
    let isLogin=false;
    if(session) {
      const currentIp = getClientIp(req);
      if (session.ip === currentIp && session.userAgent === req.headers.get('user-agent')) isLogin=true;
    }
    return NextResponse.json({isLogin});

  } catch (error) {
    console.error('Session API error:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}