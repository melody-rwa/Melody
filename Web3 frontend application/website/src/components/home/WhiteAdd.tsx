'use client';

import { useLayout } from "@/contexts/LayoutContext";
import { getDaismContract } from "@/lib/globalStore";
import { stopEvent } from "@/lib/utils/eventUtils";
import { chainErr, generateMusicState, trimTrailingZeros } from "@/lib/utils/js";
import React, { useState } from "react";
import { Button, Modal, Tab, Tabs } from "react-bootstrap";
import Loadding from "../Loadding";
import ShowErrorBar from "../ShowErrorBar";
import UploadTxt from "./UploadTxt";
import HandInput from "./HandInput";
import ShowAddress from "../ShowAddress";
import { useLazyFetch } from "@/hooks/useLazyFetch";
import Image from 'next/image'
import { Expand,Zool } from "@/lib/jssvg/melosvg";

interface MusicPlayerProps {
  musicObj: MusicType;
  showError: (v: string) => void;
  showTip: (v: string) => void;
  closeTip: () => void;
  showMessage: (v: string) => void;
}

interface WhiteListType {
  user_address: string;
  sub: number;
}

export default function WhiteAdd({ musicObj, showError, showTip, closeTip, showMessage }: MusicPlayerProps) {
  const [show, setShow] = useState(false);
  const state = generateMusicState(musicObj);
  const { setCanSelect } = useLayout();
  const [full,setFull]=useState<true|string|undefined>(undefined);

  const { data, status, error, isRefreshing, fetchData } = useLazyFetch<WhiteListType[]>(
    `WhiteAdd_${musicObj.song_id}`,
    async () => {
      const res = await fetch(`/api/getData?songId=${musicObj.song_id}`, {
        headers: { "x-method": "getWhitelistSub" }
      });
      if (!res.ok) throw new Error('Fetch failed');
      const json: WhiteListType[] = await res.json();
      return json ?? [];
    },
    { autoRefreshOnVisible: false, useCache: true }
  );

  const handleShow = async () => {
    setShow(true);
    setCanSelect(false);
    await fetchData(true);
  };

  const onSubmit = async (addresses: string[]) => {
    const daismObj = getDaismContract();
    showTip(`Allowlist&$&Submitting Request...`);

    try {
      const receipt = await daismObj?.MusicSeries.addWhitelist(
        musicObj?.series_address ?? '',
        musicObj.song_id,
        addresses
      );
      if (!receipt) throw new Error("Transaction receipt is null");
      showMessage('Allowlist&$&Allowlist submitted on-chain');
      await fetchData(true);
      setShow(false);
    } catch (err: any) {
      chainErr(err, showError,'Allowlist');
    } finally {
      closeTip();
    }
  };



  return (
    <>
      <Button style={{flexShrink:0}} variant="dark" className="melo-btn-white"  onClick={(e) => { stopEvent(e); handleShow(); }}>Allowlist</Button>

        <Modal contentClassName="melo-dailog-parent" fullscreen={full} backdrop="static" keyboard={false} size="lg" 
        show={show} onHide={() => { setShow(false); setCanSelect(true); }}>
          <Modal.Body  className='melo-dailog'>
          <div  className='melo-dailog-inner'   >
            <div className='melo-flex-between mb-3' >
                   <span className='melo-dailog-title' > Claim  </span>
                   <div> <Button variant="dark"  className="melo-btn-dailog melo-dailog-cancel"
                    onClick={() => fetchData(true)}
                    disabled={isRefreshing}
                  >
                    <Image alt='' width={20} height={20} src="/refresh.svg" /> <span style={{display:'inline-block',paddingLeft:'10px'}} >
                    {isRefreshing ? 'Refreshing...' : 'Refresh'} </span> 
                  </Button></div>
                  <div className="melo-flex-start gap-1"  >
                  {full? 
                  <span onClick={()=>{setFull(undefined)}} className='melo-dailog-title-close-btn' style={{color:'#000'}} > <Zool /></span>
                
                :<span className='melo-dailog-title-close-btn'  onClick={()=>{setFull(true)}} style={{color:'#000'}} > <Expand  /></span>
                }
                   <div className='melo-dailog-title-close-btn' onClick={()=>{setShow(false);setCanSelect(true);}} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
                   </div>
                </div>

            {
            (state === 0 && musicObj.song_id > 0) ? 
            (
              <Tabs defaultActiveKey="file" className="mb-3">
                <Tab eventKey="file" title="File Import">
                  <UploadTxt showError={showError} onSubmit={onSubmit} />
                </Tab>
                <Tab eventKey="hand" title="Manual Entry">
                  <HandInput showError={showError} onSubmit={onSubmit} />
                </Tab>
              </Tabs>
            ) :<></>
            //  <ShowErrorBar errStr="Whitelist collection period has ended." />
             }

            <div className="mt-3 px-3" style={{ fontSize: '16px', color: '#FFF', opacity: 0.8, fontWeight: 400 }}>
              Whitelist Sale Information:
            </div>

            {status === 'loading' && <Loadding />}
            {status === 'failed' && <ShowErrorBar errStr={error ?? ''} />}
            {status === 'succeeded' && (!data || data.length === 0) && <ShowErrorBar errStr='non' />}
            {status === 'succeeded' && data && data.length > 0 && <WhiteList data={data} />}
            </div>
          </Modal.Body>
        </Modal>
    </>
  );
}

function WhiteList({ data }: { data: WhiteListType[] }) {
  return (
    <div className="d-flex align-items-center flex-wrap justify-content-start">
      {data.map((obj, idx) => (
        <div key={idx} className="d-inline-flex align-items-center justify-content-between px-3 gap-2">
          <ShowAddress address={obj.user_address} />
          <div>{trimTrailingZeros(obj.sub.toString())} BNB</div>
        </div>
      ))}
    </div>
  );
}
