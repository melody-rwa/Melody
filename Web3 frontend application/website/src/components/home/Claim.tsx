'use client';

import { getDaismContract } from "@/lib/globalStore";
import { stopEvent } from "@/lib/utils/eventUtils";
import { chainErr, generateMusicState } from "@/lib/utils/js";
import React from "react";
import { Button } from "react-bootstrap";

interface MusicPlayerProps {
  musicObj: MusicType;
  user: MeloUserInfo;
  showError: (v: string) => void;
  showTip: (v: string) => void;
  closeTip: () => void;
  showMessage: (v: string) => void;
  refetch:()=>void;
}

const Claim = ({ musicObj, user, showError, showTip, closeTip, showMessage,refetch }: MusicPlayerProps) => {
  const state = generateMusicState(musicObj);

  const handleShow = async () => {
  
    const daismObj = getDaismContract();
    showTip('Claim&$&Submitting Request...');
    try {
      const receipt = await daismObj?.FractionalVaultV3.redeem(musicObj.song_id, 0);
      if (!receipt) throw new Error("Transaction receipt is null");
      showMessage('Claim&$&Tokens claimed');
      setTimeout(async () => {
        refetch(); //Refresh homepage data
        closeTip();
      }, 500);
    
    } catch (err: any) {
      closeTip();
      chainErr(err, showError,'Claim');
    } 
  };


  return (
    <>
      {(user.connected === 1 && musicObj.sub_amount && musicObj.sub_amount > 0) ?
        <>
          {musicObj.is_end===1 || state === 4 ?
            <>{musicObj.is_claim && musicObj.is_claim > 0 ? <Button variant="dark" className="melo-btn-claim-disable" disabled   >Claimed</Button> : 
              <Button variant="dark"  className="melo-btn-claim"  onClick={(e) => { stopEvent(e); handleShow(); }}>Claim</Button>
              }
            </>
            :  <Button variant="dark" className="melo-btn-claim-disable"  disabled>Pending</Button> 
          }
        </>
        :  <Button variant="dark"  className="melo-btn-claim-disable" disabled>Claim</Button>
      }
    </>
  );
};

Claim.displayName = "Claim";
export default React.memo(Claim);
