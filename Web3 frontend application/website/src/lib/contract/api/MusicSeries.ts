import {type Signer, ethers } from "ethers";
import abi from '../data/MusicSeries_abi.json';

export default class MusicSeries {
  
  public signer: Signer;
  public address: string;
  private abi:any;

  constructor(_signer: Signer,_address: string) {
    this.signer = _signer;
    this.address = _address;
    this.abi = abi;
  }


  async subscribe(address:string, songId:number|string,v:number|string):  Promise<ethers.TransactionReceipt>  {
    
    const contract=new ethers.Contract(address,this.abi,this.signer);
    
    const tx = await contract!.subscribe(songId.toString(),{value:ethers.parseEther(v.toString())});
    const receipt = await tx.wait();
    return receipt;
  }

  async addWhitelist(address:string, songId:number|string,name:string[]): Promise<ethers.TransactionReceipt>  {
    const contract=new ethers.Contract(address,this.abi,this.signer);
    const tx = await contract!.addWhitelist(songId.toString(),name);
    const receipt = await tx.wait();
    return receipt;
  }

 async isWhitelisted(address:string,songId:number|string, account:string): Promise<boolean> {
  const contract=new ethers.Contract(address,this.abi,this.signer);
  const result = await contract!.isWhitelisted(songId,account);
  return result;
}

 async getWhitelistSpent(address:string,songId:number|string, account:string): Promise<boolean> {
  const contract=new ethers.Contract(address,this.abi,this.signer);
  const result = await contract!.getWhitelistSpent(songId,account);
  return result;
}



  


}
