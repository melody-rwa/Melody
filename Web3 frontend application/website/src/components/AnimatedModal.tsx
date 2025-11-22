'use client';
import React from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { useDispatch, useSelector } from 'react-redux';
import { AppDispatch, RootState, setMess } from '@/store/store';
import { Button } from 'react-bootstrap';

interface AnimatedModalProps {
  onClose?: () => void;

}

const AnimatedModal: React.FC<AnimatedModalProps> = ({ onClose }) => {
    
    const mess=useSelector((state:RootState)=>state.valueData.messText);
    const dispatch=useDispatch<AppDispatch>();
  return (
    <AnimatePresence>
      {mess && (
        <motion.div
          className="position-fixed top-0 start-0 w-100 h-100 bg-dark bg-opacity-75 d-flex align-items-center justify-content-center"
          style={{ zIndex: 1050 }}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
        //   onClick={onClose}
        >

          <motion.div
            onClick={(e) => e.stopPropagation()}
            initial={{ scale: 0.8, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            exit={{ scale: 0.8, opacity: 0 }}
            transition={{
              duration: 0.3,
              ease: [0.25, 0.1, 0.25, 1], 
            }}
            className="bg-dark  text-white p-4 rounded text-center shadow-lg"
            style={{ minWidth: '320px', minHeight:'100px',maxWidth:'50%'}}
          >
             <div className='melo-errwin-title mt-1' >Message tip</div>
                
            <div style={{textAlign:'left'}} className='mt-2 mb-3 melo-errwin-text px-3' >✅{' '}{mess}</div>

        
                <Button variant="primary" onClick={() => {
                    dispatch(setMess(''));
                     onClose?.();
                }}>
                    Close
                </Button>
           

          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
};

export default AnimatedModal;
