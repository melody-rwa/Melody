

import { getUser,getBnb,checkNameAndSymbol,getMusicData,getWhiteListById,getSubAmountById,messagePageData,getWhitePay
  ,getWhitelistSub,getSubMusicAvatar,getArtistTotal,getProfileTotal,getPlayAll,getPlayArtist,getPlayUser,getSales,getSlat } from '@/lib/mysql/message';
import { NextRequest, NextResponse } from 'next/server';


// Define the methods type
type MethodFn = (params: Record<string, any>) => Promise<any>;

const methods: Record<string, MethodFn> = {
  getUser,
  getBnb,
  checkNameAndSymbol,
  getMusicData,
  getWhiteListById,
  getSubAmountById,
  messagePageData,
  getWhitelistSub,
  getSubMusicAvatar,
  getArtistTotal,
  getProfileTotal,
  getPlayAll,
  getPlayArtist,
  getPlayUser,
  getSales,getWhitePay,getSlat,
};


export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const query = Object.fromEntries(searchParams.entries());
    const method = req.headers.get('x-method'); 


    if (!method || !(method in methods)) {
      console.info("not method:",method)
      return NextResponse.json({ errMsg: 'Invalid Method' }, { status: 400 });
    } else {
      return NextResponse.json(await methods[method](query));
    }

  } catch (err) {
    console.error('get: /api/getData:', err);
    return NextResponse.json({ errMsg: 'fail' }, { status: 500 });
  }
}
