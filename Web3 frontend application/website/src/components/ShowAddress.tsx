'use client';

import React, { useState, useRef, useEffect } from 'react';
import {Overlay,Tooltip} from 'react-bootstrap';
import Image from 'next/image';

interface ShowAddressProps {
  address: string;
  isb?: boolean;
}

const ShowAddress: React.FC<ShowAddressProps> = ({ address, isb = false }) => {
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


  const getAccount = (): string => {
    if (address && address.length === 42) {
      return `${address.slice(0, 6)}......${address.slice(38, 42)}`;
    }
    return address;
  };

  const handleCopy = (e: React.MouseEvent<HTMLImageElement>) => {
    const dataAddress = e.currentTarget.dataset.address;
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
      <span>{isb ? <b>{getAccount()}</b> : getAccount()}</span>{' '}
      
      <Image
        alt="copy address"
        width={20}
        height={14}
        data-address={address}
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



ShowAddress.displayName="ShowAddress";
export default React.memo(ShowAddress);