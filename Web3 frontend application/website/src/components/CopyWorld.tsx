'use client';

import React, { useState, useRef, useEffect } from 'react';
import {Overlay,Tooltip} from 'react-bootstrap';
import Image from 'next/image';

interface ShowAddressProps {
    showStr:string;
    copyStr: string;
}

const CopyWorld: React.FC<ShowAddressProps> = ({ showStr,copyStr}) => {
  const [show, setShow] = useState(false); 
  const target = useRef<HTMLImageElement>(null); 
  const timerRef = useRef<NodeJS.Timeout | null>(null); 

  useEffect(() => {
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, []);

 

 
  const handleCopy = (e: React.MouseEvent<HTMLImageElement>) => {
    e.preventDefault(); 
    e.stopPropagation(); 
    
    const dataAddress = e.currentTarget.dataset.str;
    if (!dataAddress) return;
    
    navigator.clipboard?.writeText(dataAddress)
      .then(() => {
        setShow(true);
        if (timerRef.current) return; 
        
        timerRef.current = setTimeout(() => {
          setShow(false);
          timerRef.current = null;
        }, 1000);
      })
      .catch(err => {
        console.error('copy faild:', err);
      });
  };

  return (
    <span style={{ whiteSpace: 'nowrap' }}>
      <span>{showStr}</span>{' '}
      <Image
        alt={`copy ${showStr}`}
        width={20}
        height={20}
        data-str={copyStr}
        src="/clipboard.svg"
        ref={target}
        onClick={handleCopy}
        style={{ cursor: 'pointer' }}
      />
      
      <Overlay target={target.current} show={show} placement="bottom">
        {(props) => (
          <Tooltip id="copy-tooltip" {...props}> Copied
          </Tooltip>
        )}
      </Overlay>
    </span>
  );
};



CopyWorld.displayName="CopyWorld";
export default React.memo(CopyWorld);