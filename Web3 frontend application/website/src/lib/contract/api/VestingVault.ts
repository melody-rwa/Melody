import {type  Contract, type Signer, ethers } from "ethers";
import abi from '../data/VestingVault_abi.json';

export default class VestingVault {
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

  async claimFees(songId:number|string): Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.claimFees(songId);
    const receipt = await tx.wait();
    return receipt;
  }


  async redeem(songId:number|string): Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.redeem(songId);
    const receipt = await tx.wait();
    return receipt;
  }


  async releasableAmount(song_id:number|string): Promise<string> {
    this.genegateContract();
    const result = await this.contract!.releasableAmount(song_id);
    return result.toString();
  }
   
}
