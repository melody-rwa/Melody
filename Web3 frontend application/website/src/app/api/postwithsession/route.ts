
import { NextRequest, NextResponse } from 'next/server';
import { delMusic } from '@/lib/mysql/message';
import { getSession } from '@/lib/session';
import { getClientIp } from '@/lib/utils';

interface ApiMethods {
  [key: string]: (body: any) => Promise<any>;
}

const methods: ApiMethods = {
  delMusic,
};

export async function POST(request: NextRequest) {
    const session = await getSession();
    const currentIp = getClientIp(request);
    if (!session || session.ip !== currentIp || session.userAgent !== request.headers.get('user-agent')) {
        return NextResponse.json({ errMsg: 'No wallet signature login'  }, { status: 500 });
    }
  try {
    const method = request.headers.get('x-method');
    if (!method) {
      return NextResponse.json({ errMsg: 'Method header is required' },{ status: 400 });
    }
    else 
    {
      if (!methods[method]) {return NextResponse.json({ errMsg: `Method '${method}' not found` },{ status: 404 });}
      else 
      {
        const body = await request.json();
        const result = await methods[method](body);
        return NextResponse.json({ state: result });
      }
    }

  } catch (error: any) {
    console.error('error for POST /api/postwithsession:', error);
    return NextResponse.json(
      { errMsg: 'fail: ' + error.toString() },
      { status: 500 }
    );
  }
}

export async function GET() {
  return NextResponse.json({ errMsg: 'Method Not Allowed' }, { status: 405 });
}
