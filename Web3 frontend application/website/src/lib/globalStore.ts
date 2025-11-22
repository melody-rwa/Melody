

import ArtistRegistry from '@/lib/contract/api/ArtistRegistry';
import MusicFactory from '@/lib/contract/api/MusicFactory';
import MusicSeries from '@/lib/contract/api/MusicSeries';
import FractionalVaultV3 from './contract/api/FractionalVaultV3';
import VestingVault from './contract/api/VestingVault';

import type { Signer } from 'ethers';


export interface MeloContract {
 
  ArtistRegistry:ArtistRegistry;
  MusicFactory:MusicFactory;
  MusicSeries:MusicSeries;
  FractionalVaultV3:FractionalVaultV3;
  VestingVault:VestingVault;
  signer:Signer;
 
}

  const daismObj: { instance?: MeloContract } = {};

  export function setDaismContract(obj: MeloContract|undefined) {daismObj.instance = obj;}
  export function getDaismContract() { return daismObj.instance;}

  