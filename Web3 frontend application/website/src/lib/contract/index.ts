'use client';

import ArtistRegistry from './api/ArtistRegistry';
import MusicFactory from './api/MusicFactory';
import MusicSeries from './api/MusicSeries';
import FractionalVaultV3 from './api/FractionalVaultV3';
import VestingVault from './api/VestingVault';

import type { Signer } from 'ethers';


export default class DaoApi {
    public signer: Signer;
    private account: string;

    private ArtistRegistry_obj?:ArtistRegistry;
    private MusicFactory_obj?:MusicFactory;
    private MusicSeries_obj?:MusicSeries;
    private FractionalVaultV3_obj?:FractionalVaultV3;
    private VestingVault_obj?:VestingVault;


    constructor(_signer: Signer, _account: string) {
        this.signer = _signer;
        this.account = _account;
    }

    get ArtistRegistry() { 
        if (!this.ArtistRegistry_obj) {this.ArtistRegistry_obj = new ArtistRegistry(this.signer,  process.env.NEXT_PUBLIC_ArtistRegistry as string); }
        return this.ArtistRegistry_obj; 
    }

    get MusicFactory() { 
        if (!this.MusicFactory_obj) {this.MusicFactory_obj = new MusicFactory(this.signer,  process.env.NEXT_PUBLIC_MusicFactory as string); }
        return this.MusicFactory_obj; 
    }
    get MusicSeries() { 
        if (!this.MusicSeries_obj) {this.MusicSeries_obj = new MusicSeries(this.signer,  process.env.NEXT_PUBLIC_MusicSeries as string); }
        return this.MusicSeries_obj; 
    }

    get FractionalVaultV3() { 
        if (!this.FractionalVaultV3_obj) {this.FractionalVaultV3_obj = new FractionalVaultV3(this.signer,  process.env.NEXT_PUBLIC_FractionalVaultV3 as string); }
        return this.FractionalVaultV3_obj; 
    }
    get VestingVault() { 
        if (!this.VestingVault_obj) {this.VestingVault_obj = new VestingVault(this.signer,  process.env.NEXT_PUBLIC_VestingVault as string); }
        return this.VestingVault_obj; 
    }

}
