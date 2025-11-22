

'use client';
import Image from 'next/image';

import {type RootState, type AppDispatch, setErrText} from '../store/store';
import { useSelector, useDispatch } from 'react-redux'
import { Errimg } from '@/lib/jssvg/melosvg';

import { Button,Modal } from 'react-bootstrap';

export default function ShowErrWin() {
    const errText = useSelector((state:RootState) => state.valueData.errText)
    const dispatch = useDispatch<AppDispatch>();
    const [tip,text]=errText.split('&$&');
 
    return (
        <Modal contentClassName="melo-dailog-parent" 
          size='sm' centered  show={errText!==''} onHide={() => {dispatch(setErrText(''))}}>
           
            <Modal.Body className='melo-dailog'  >
            <div  className='melo-dailog-inner'   >

                 <div className='melo-flex-between' >
                   <span className='melo-dailog-title' >{tip}</span>
                   <div className='melo-dailog-title-close-btn' onClick={()=>{dispatch(setErrText(''))}} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
                </div>
                <div className='melo-dailog-text mt-4 mb-4' >{text}</div>
                
                {/* <div className='mt-2 mb-3 melo-errwin-text px-3 four-line-clamp' >❌{' '}{errText}</div>  */}
               
                <div style={{textAlign:'center'}} className='mb-4' >
                   <Errimg />
                </div>

                <div style={{textAlign:'center',width:'100%'}} >
                    <Button variant='dark' className='melo-dailog-btn' onClick={()=>{dispatch(setErrText(''))}}>
                       OK
                        </Button>
                </div>
            </div>
            </Modal.Body>
        </Modal>
    )
}

