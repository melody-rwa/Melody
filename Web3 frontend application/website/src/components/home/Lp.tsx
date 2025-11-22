'use client';

import { useLayout } from "@/contexts/LayoutContext";
import { getDaismContract } from "@/lib/globalStore";
import { stopEvent } from "@/lib/utils/eventUtils";
import { chainErr, generateMusicState } from "@/lib/utils/js";
import { ethers } from "ethers";
import React, { useState } from "react";
import { Button, Form, FormControl, Modal } from "react-bootstrap";
import Loadding from "../Loadding";
import ShowErrorBar from "../ShowErrorBar";
import { useLazyFetch } from "@/hooks/useLazyFetch";
import Image from "next/image";

interface MusicPlayerProps {
  musicObj: MusicType;
  user: MeloUserInfo;
  showError: (v: string) => void;
  showTip: (v: string) => void;
  closeTip: () => void;
  showMessage: (v: string) => void;
}

const Lp = ({ musicObj, user, showError, showTip, closeTip, showMessage }: MusicPlayerProps) => {
  const [show, setShow] = useState(false);
  const state = generateMusicState(musicObj);
  const { setCanSelect } = useLayout();
  const { data, status, error, isRefreshing, fetchData } = useLazyFetch<{ liquidity: string; ableAmount: string }>(
    musicObj.song_id.toString(),
    async () => {
      const daismObj = getDaismContract();
      if (!daismObj) throw new Error("Contract not initialized");

      const [liquidity, ableAmount] = await Promise.all([
        daismObj.FractionalVaultV3.userLiquidity(musicObj.song_id, process.env.NEXT_PUBLIC_VestingVault as string),
        daismObj.VestingVault.releasableAmount(musicObj.song_id),
      ]);

      return { liquidity, ableAmount };
    },
    { autoRefreshOnVisible: false, useCache: false }
  );

  const handleShow = async () => {
    setShow(true);
    setCanSelect(false);
    await fetchData(true);
  };

  const submit = async () => {
    setCanSelect(true)
    if (!data || parseFloat(data.liquidity) === 0) {
      showError('Claim LP&$&LP is 0, unable to claim!');
      return;
    }

    setShow(false);
    const daismObj = getDaismContract();
    showTip('Claim LP&$&Submitting Request...');
    try {
      const receipt = await daismObj?.VestingVault.redeem(musicObj.song_id);
      if (!receipt) throw new Error("Transaction receipt is null");
      showMessage("Claim LP&$&LP claimed");
      await fetchData(true);
    } catch (err: any) {
      chainErr(err, showError,'LP');
    } finally {
      closeTip();
    }
  };

  const content = (
    <>
      <Button variant="dark" className="melo-btn-claim"  onClick={(e:any) => { stopEvent(e); handleShow(); }}>Claim LP</Button>

      <Modal  centered contentClassName="melo-dailog-parent" backdrop="static" keyboard={false}
       show={show} onHide={() => { setShow(false); setCanSelect(true); }}>
       
        <Modal.Body  className='melo-dailog'>
        <div  className='melo-dailog-inner'   >
        <div className='melo-flex-between mb-3' >
                   <span className='melo-dailog-title' > Claim LP </span>
                   <div> <Button variant="dark"  className="melo-btn-dailog melo-dailog-cancel"
                    onClick={() => fetchData(true)}
                    disabled={isRefreshing}
                  >
                    <Image alt='' width={20} height={20} src="/refresh.svg" /> <span style={{display:'inline-block',paddingLeft:'10px'}} >
                    {isRefreshing ? 'Refreshing...' : 'Refresh'} </span> 
                  </Button></div>
                   <div className='melo-dailog-title-close-btn' onClick={()=>{setShow(false);setCanSelect(true);}} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
                </div>
          {/* <InputGroup className="mb-2 mt-2">
            <InputGroup.Text>Unclaimed LP</InputGroup.Text>
            <FormControl readOnly type="text" value={ethers.formatEther(data?.liquidity ?? '0')} />
          </InputGroup>
          <InputGroup className="mb-2 mt-2">
            <InputGroup.Text>Claimable LP</InputGroup.Text>
            <FormControl readOnly type="text" value={ethers.formatEther(data?.ableAmount ?? '0')} />
          </InputGroup> */}

          <Form.Group className="mb-2">
            <Form.Label style={{color:'rgba(255, 255, 255, 0.60)',fontSize:'12px',fontWeight: 400,lineHeight:'16px'}} >Unclaimed LP</Form.Label>
            <FormControl readOnly className="melo-dailog-input"  value={ethers.formatEther(data?.liquidity ?? '0')} type="text" />
          </Form.Group>

          <Form.Group className="mb-2 mt-2">
            <Form.Label style={{color:'rgba(255, 255, 255, 0.60)',fontSize:'12px',fontWeight: 400,lineHeight:'16px'}} >Claimable LP</Form.Label>
            <FormControl readOnly className="melo-dailog-input" value={ethers.formatEther(data?.ableAmount ?? '0')} type="text" />
          </Form.Group>

          {status === 'loading' && <Loadding />}
          {status === 'failed' && <ShowErrorBar errStr={error ?? ''} />}

       
          <div className="mt-5 mb-3" style={{ display:'flex',alignItems:'center',justifyContent:'center',gap:'10px'}}>
            <Button variant="dark"  className="melo-btn-dailog melo-dailog-cancel" onClick={()=>{setShow(false);setCanSelect(true);}} >Cancel</Button>{' '}
            
              <Button variant="dark" className="melo-btn-dailog melo-dailog-submit"   onClick={submit} >
                Submit
              </Button>
          </div>

        </div>
        </Modal.Body>
      </Modal>
    </>
  );

  return (
    <>
      {(user.connected === 1 && (state === 4 || musicObj.is_end===1)) ? content : <Button variant="dark"  className="melo-btn-desposit-disable"   disabled={true}>Claim LP</Button>}
    </>
  );
};

Lp.displayName = "Lp";
export default React.memo(Lp);
