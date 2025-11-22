
import { NextResponse } from 'next/server';
import { generateNonce } from 'siwe';

export async function GET() {
  try {
    const nonce = generateNonce();
    const response = NextResponse.json({ nonce });
    response.cookies.set('nonce', nonce, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 5, // 5-minute validity period
    });

    return response;

  } catch (error: any) {
    console.error('Nonce generation error:', error);
    return NextResponse.json(
      { error: 'Failed to generate nonce' },
      { status: 500 }
    );
  }
}