import {type  Contract, type Signer, ethers } from "ethers";
import abi from '../data/ArtistRegistry_abi.json';

export default class ArtistRegistry {
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

  async register(ipfsCid:string): Promise<ethers.TransactionReceipt>  {
    this.genegateContract();
    const tx = await this.contract!.register(ipfsCid);
    const receipt = await tx.wait();
    return receipt;
  }

  async isArtist(account:string): Promise<boolean> {
    this.genegateContract();
    const result = await this.contract!.isArtist(account);
    return result;
  }
}
