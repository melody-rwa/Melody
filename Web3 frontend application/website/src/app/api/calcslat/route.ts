
import { ethers } from 'ethers';
import { NextResponse } from 'next/server';
import abi from '@/lib/contract/data/MusicFactory_abi.json'
import { execute } from '@/lib/mysql/common';

function saltFromNumber(i: number | bigint): string {
    const n = typeof i === "bigint" ? i : BigInt(i);
    const hex = n.toString(16);             // without 0x
    return "0x" + hex.padStart(64, "0");    // 32 bytes = 64 hex chars
  }


export async function GET() {
  try {
    const provider = new ethers.JsonRpcProvider(process.env.NEXT_PUBLIC_HTTPS_URL);
    const factoryAddress = process.env.NEXT_PUBLIC_MusicFactory??'';
    const factory= new ethers.Contract(factoryAddress,abi,provider);

    const initCodeHash=await factory.getBeaconProxyCodeHash();

    const FACTORY_ADDRESS = factoryAddress.toLowerCase();
    const TARGET_SUFFIX = "123"; // The address is required to end with 0x...123

    let found=0;

    for (let i = 0; found < 50; i++) {
        const salt = saltFromNumber(i);
        const address = ethers.getCreate2Address(
        FACTORY_ADDRESS,
        salt,
        initCodeHash
        );

        if (address.toLowerCase().endsWith(TARGET_SUFFIX)) 
        {
            const sql="insert IGNORE into t_slat(slat) values(?)";
            const n=await execute(sql,[salt]);
            if(n>0) found++;
        }

    }

    return  NextResponse.json({ ok:true });;

  } catch (error: any) {
    console.error(error)
 
    return NextResponse.json(
      { error: 'Failed to calcslat' },
      { status: 500 }
    );
  }
}