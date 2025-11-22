
import { NextRequest, NextResponse } from 'next/server';
import { SiweMessage } from 'siwe';
import { cookies } from 'next/headers';
import { SignJWT } from 'jose';
import { getClientIp } from '@/lib/utils';
import { rateLimit } from '@/lib/rate-limit';

const secret = new TextEncoder().encode(process.env.JWT_SECRET!);

export async function POST(req: NextRequest) {
  const clientIp = getClientIp(req);
    if (clientIp) {
      const isRateLimited = await rateLimit(clientIp, 'login', 5); // 5 times per minute
      if (isRateLimited) {
        return NextResponse.json(
          { error: 'Too many requests' },
          { status: 429 }
        );
      }
    }
  try {
    const body = await req.json();
    if (!body.message) {
      return NextResponse.json(
        { errMsg: 'Expected prepareMessage object as body.' },
        { status: 422 }
      );
    }

    const { message, signature } = body;
    const siweMessage = new SiweMessage(message);

    const cookieStore = await cookies();
    const nonce = cookieStore.get('nonce')?.value;
    
    if (siweMessage.nonce !== nonce) {
      return NextResponse.json(
        { errMsg: 'Invalid nonce.' },
        { status: 422 }
      );
    }

    const verificationResult = await siweMessage.verify({ signature });
    if (!verificationResult.success) {
      return NextResponse.json(
        { errMsg: 'Signature verification failed.' },
        { status: 401 }
      );
    }

    const token = await new SignJWT({
      did: siweMessage.address.toLowerCase(),
      ip: clientIp,
      userAgent: req.headers.get('user-agent')
    })
      .setProtectedHeader({ alg: 'HS256' })
      .setExpirationTime('7d')
      .sign(secret);

      const response = NextResponse.json({
    });

    response.cookies.set('auth-token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24 * 7,
    });

    response.cookies.set('nonce', '', {
      maxAge: 0,
    });

    return response;

  } catch (error: any) {
    console.error('SIWE login error:', error);
    return NextResponse.json(
      { errMsg: error.message || 'Internal server error' },
      { status: 500 }
    );
  }
}