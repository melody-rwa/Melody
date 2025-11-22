import { NextRequest } from 'next/server';


import path from 'node:path';
import * as fs from "node:fs/promises"; 


export async function saveImage(file:File,savePath:string)
{

  if (!(file instanceof File)) {
    console.error("no file object:", file);
    return {imgPath:'',fileName:''};
  }

  try {
    const bytes = await file.arrayBuffer();
    const buffer = Buffer.from(bytes);
    const ext = path.extname(file.name); 
    const randomName = `${Date.now()}-${Math.round(Math.random() * 1e9)}${ext}`;
    const _path=new Date().toISOString().slice(0, 10);
    const uploadDir = path.join(process.cwd(), `${savePath}/${_path}` );
    await fs.mkdir(uploadDir, { recursive: true });
    const filePath = path.join(uploadDir, randomName);
    await fs.writeFile(filePath, buffer);
    return {imgPath:`https://${process.env.NEXT_PUBLIC_DOMAIN}/${savePath}/${_path}/${randomName}`,fileName:file.name.replace(ext,'')} ;
  } catch (error) {
   console.error(error);
   return {imgPath:'',fileName:''}
  }
}

export const checkAddress = (v: string): boolean => /^0x[A-Fa-f0-9]{40}$/.test(v);
 

export function getClientIp(req: NextRequest): string | undefined {

  const xForwardedFor = req.headers.get('x-forwarded-for');
  
  if (xForwardedFor) {
    return xForwardedFor.split(',')[0]?.trim();
  }

  const otherHeaders = ['x-real-ip', 'x-client-ip', 'cf-connecting-ip'];
  for (const header of otherHeaders) {
    const value = req.headers.get(header);
    if (value) {
      return value.trim();
    }
  }

  return undefined;
}