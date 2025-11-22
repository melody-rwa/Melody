
const rateLimitMap = new Map<string, { count: number; resetTime: number }>();

export async function rateLimit(
  ip: string, 
  action: string, 
  maxAttempts: number = 5,
  windowMs: number = 60000 
): Promise<boolean> {
  const key = `${ip}:${action}`;
  const now = Date.now();
  
  const existing = rateLimitMap.get(key);
  
  if (!existing || now > existing.resetTime) {
  
    rateLimitMap.set(key, {
      count: 1,
      resetTime: now + windowMs
    });
    return false;
  }
  

  existing.count += 1;
  rateLimitMap.set(key, existing);

  return existing.count > maxAttempts;
}