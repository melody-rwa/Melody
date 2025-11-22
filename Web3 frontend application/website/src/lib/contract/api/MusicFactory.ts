import {type  Contract, type Signer, ethers } from "ethers";
import abi from '../data/MusicFactory_abi.json';

export default class MusicFactory {
  public contract?: Contract;
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
 
  //Post music
  
  async createMusic(invite:string,ipfsCid:string,targetBNBWei:number,durationSec:number,name:string,symbol:string,plannedSec:number,slat:string)
  : Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.createMusic(invite,ipfsCid, ethers.parseEther(targetBNBWei.toString()),durationSec,name,symbol,plannedSec,slat
    ,{gasLimit:'9000000'});
    const receipt = await tx.wait();
    return receipt;
  }


  async isInviteValid(code:string): Promise<boolean> {
    this.genegateContract();
    const result = await this.contract!.isInviteValid(code.trim());
    return result;
  }

  
  //
  async getBeaconProxyCodeHash(): Promise<string> {
    this.genegateContract();
    const result = await this.contract!.getBeaconProxyCodeHash();
    return result;
  }
  
}
