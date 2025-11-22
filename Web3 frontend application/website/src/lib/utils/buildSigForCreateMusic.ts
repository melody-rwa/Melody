import { ethers } from "ethers";

/**

 *
 * @param signer      ethers.Wallet or ethers.BrowserProvider.getSigner()
 * @param inviteCode  string
 * @param ipfsCid     string
 * @param targetBNBWei  bigint | number | string
 * @param durationSec bigint | number | string
 * @param name        string
 * @param symbol      string
 * @param baseDate   string 
 * @returns { nonce, signature }
 */
export async function buildSigForCreateMusic(
  signer: ethers.Signer,
  inviteCode: string,
  ipfsCid: string,
  targetBNBWei: bigint | number | string,
  durationSec: bigint | number | string,
  name: string,
  symbol: string,
  baseDate: string
) {
 
  const nonce = BigInt("0x" + crypto.randomUUID().replace(/-/g, "")).toString();

  const baseTs = Math.floor(new Date(baseDate).getTime() / 1000);
  const validUntil = baseTs + 16 * 60 * 60; 

  const structHash = ethers.keccak256(ethers.solidityPacked(
    ["string", "string", "uint256", "uint256", "string", "string", "uint256", "uint256"],
    [inviteCode, ipfsCid, targetBNBWei, durationSec, name, symbol, validUntil, nonce]
  ));


  const signature = await signer.signMessage(ethers.getBytes(structHash));

  return { nonce,validUntil, signature };
}
