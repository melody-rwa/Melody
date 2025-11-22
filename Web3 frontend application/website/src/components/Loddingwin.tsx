

'use client';
import {type RootState,} from '../store/store';
import { useSelector } from 'react-redux'
import { Modal } from 'react-bootstrap';
import Loader from './Loader';
import { useEffect, useState } from 'react';

export default function Loddingwin() {
    const [dwin, setDwin] = useState<number>(0); 
    const tipText = useSelector((state: RootState) => state.valueData.tipText);
    const [tip,text]=tipText.split('&$&');
    useEffect(() => {
        let win_i = 0;
        let timein: ReturnType<typeof setInterval> | undefined;
    
        if (tipText !== '') {
          timein = setInterval(() => {
            if (win_i >= 3) win_i = 0;
            else win_i++;
            setDwin(win_i);
          }, 5000);
        }
    
        return () => {
          setDwin(0);
          win_i = 0;
          if (timein) clearInterval(timein);
        };
      }, [tipText]);
    
 
    return (
        <Modal  contentClassName="melo-dailog-parent" 
          centered size='sm'
          show={tipText !== ''}
          backdrop="static"
          keyboard={false}
        >
            <Modal.Body className='melo-dailog'  >
            <div  className='melo-dailog-inner'   >

                 <div className='melo-dailog-title mb-5' >{tip}</div>
                 
                 <div style={{display:'flex',alignItems:'center',justifyContent:'center'}} >
                 <Loader/>
                 </div>

                <div className='melo-dailog-text mt-5 mb-5' >
                    {dwin === 0 && <div> {text}</div>}
                    {dwin === 1 && <div> Blockchain is slow, do not worry.</div>}
                    {dwin === 2 && <div> Sometimes the blockchain can be very slow and take a long time, this is normal.</div>}
                    {dwin === 3 && <div> Submitting on-chain, please be patient.</div>}
                </div>

            </div>
            </Modal.Body>
        </Modal>
    )
}

