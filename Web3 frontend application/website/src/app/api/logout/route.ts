import { NextResponse } from 'next/server';

export async function POST() {
  const response = NextResponse.json({ success: true, message: 'Logged out' });

  // Clear cookies: Set the cookie name and expiration time to 0
  response.cookies.set('auth-token', '', {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 0, //  expire immediately
    path: '/', // Ensure coverage of the entire domain
  });

  return response;
}
