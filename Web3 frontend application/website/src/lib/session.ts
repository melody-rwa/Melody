import { jwtVerify, SignJWT } from 'jose';
import { cookies } from 'next/headers';
import { z } from 'zod';

const secret = new TextEncoder().encode(process.env.JWT_SECRET!);

const SessionSchema = z.object({
  did: z.string().min(1, 'DID is required'),
  ip: z.string().optional(),
  userAgent: z.string().optional(),
  exp: z.number().optional(),
  iat: z.number().optional(),
});

export type SessionPayload = z.infer<typeof SessionSchema>;

export async function createSessionToken(payload: SessionPayload): Promise<string> {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: 'HS256' })
    .setExpirationTime('7d')
    .sign(secret);
}

export async function verifySessionToken(token: string): Promise<SessionPayload | null> {
  try {
    const { payload } = await jwtVerify(token, secret);
    
    const validationResult = SessionSchema.safeParse(payload);
    
    if (validationResult.success) {
      return validationResult.data;
    }
    
    console.error('Invalid session token payload:', validationResult.error);
    return null;
  } catch (error) {
    console.error('Session token verification failed:', error);
    return null;
  }
}

export async function getSession(): Promise<SessionPayload | null> {
  try {
    const cookieStore = await cookies();
    const token = cookieStore.get('auth-token')?.value;
    
    if (!token) return null;
    
    const session = await verifySessionToken(token);

    if (session && session.exp && session.exp < Date.now() / 1000) {
      console.info('Session token expired');
      return null;
    }
    
    return session;
  } catch (error) {
    console.error('Failed to get session:', error);
    return null;
  }
}