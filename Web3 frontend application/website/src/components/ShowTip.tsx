

'use client';
import Image from 'next/image';

import {type RootState, type AppDispatch, setMessageText} from '../store/store';
import { useSelector, useDispatch } from 'react-redux'
import { Okimg } from '@/lib/jssvg/melosvg';

import { Button,Modal } from 'react-bootstrap';

export default function ShowTip() {
    const messText = useSelector((state:RootState) => state.valueData.messageText)
    const dispatch = useDispatch<AppDispatch>();

    const [tip,text]=messText.split('&$&');
 
    return (
        <Modal contentClassName="melo-dailog-parent" 
          size='sm' centered  show={messText!==''} onHide={() => {dispatch(setMessageText(''))}}>
           
            <Modal.Body className='melo-dailog'  >
            <div  className='melo-dailog-inner'   >

                 <div className='melo-flex-between' >
                   <span className='melo-dailog-title' >{tip} </span>
                   <div className='melo-dailog-title-close-btn' onClick={()=>{dispatch(setMessageText(''))}} >
                   <Image alt='' src='/close.svg' width={10.5} height={10.5} />
                   </div>
                </div>
                <div className='melo-dailog-text mt-4 mb-4' >{text}</div>
                
                {/* <div className='mt-2 mb-3 melo-errwin-text px-3 four-line-clamp' >❌{' '}{errText}</div>  */}
               
                <div style={{textAlign:'center'}} className='mb-4' >
                   <Okimg />
                </div>

                <div  style={{textAlign:'center',width:'100%'}} >
                    <Button variant='dark' className='melo-dailog-btn' onClick={()=>{dispatch(setMessageText(''))}}>
                       OK
                        </Button>
                </div>
            </div>
            </Modal.Body>
        </Modal>
    )
}
