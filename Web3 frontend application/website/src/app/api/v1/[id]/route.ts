import { NextRequest, NextResponse } from "next/server";
import { getData } from "@/lib/mysql/common";

export async function GET(request: NextRequest,{ params }: { params: Promise<{ id: string }> }) {
  const { id } =await params;

  try {
    const rows = await getData("select token_symbol,token_desc,token_logo from t_music where music_id=?",[id]);
    if (!rows || rows.length === 0) throw new Error("Not found");

    const jsonData = {
      name: rows[0].token_symbol,
      description: rows[0].token_desc,
      image: rows[0].token_logo,
      external_url: "https://melodylabs.io",
      attributes: [
        { trait_type: "Background", value: "Blue" },
        { trait_type: "Eyes", value: "Laser" },
      ],
    };

    return NextResponse.json(jsonData);
  } catch (error: any) {
    return NextResponse.json(
      {message: error?.message ?? "Not found", },
      { status: 404 }
    );
  }
}
