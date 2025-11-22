import {type  Contract, type Signer, ethers } from "ethers";
import abi from '../data/FractionalVaultV3_abi.json';

export default class FractionalVaultV3 {
  private contract?: Contract;
  public signer: Signer;
  public address: string;
  private abi:any;

  constructor(_signer: Signer,_address: string) {
    this.signer = _signer;
    this.address = _address;
    this.abi = abi;
  }

  private genegateContract() {
    if (!this.contract) {
      this.contract = new ethers.Contract(this.address,this.abi,this.signer);
    }
  }


  async claim(songId:number|string): Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.claim(songId);
    const receipt = await tx.wait();
    return receipt;
  }


   async userLiquidity(song_id:number|string,account:string): Promise<string> {
     this.genegateContract();
     const result = await this.contract!.userLiquidity(song_id,account);
     return result.toString();
   }
  
   
  async getPendingForUser(song_id:number|string,account:string): Promise<any> {
    this.genegateContract();
    const result = await this.contract!.getPendingForUser(song_id,account);
    return result;
  }
   
   
  async redeem(songId:number|string,liquidityToRedeem:number|string): Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.redeem(songId,liquidityToRedeem,0,0);
    const receipt = await tx.wait();
    return receipt;
  }
   
}
