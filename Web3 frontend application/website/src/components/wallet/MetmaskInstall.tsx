'use client';

import { Modal, Button } from 'react-bootstrap';
import Link from 'next/link';
import Image from 'next/image';
import {  useState } from 'react';

export default function MetmaskInstall() {
    const [showMetaMask, setShowMetaMask] = useState(false); 

    
  return (
    <>
     <Button 
              variant="warning" 
              style={{ 
                borderRadius: '12px', 
                marginLeft: '16px',
                display: 'flex',
                color:'#FFF',
                background:'#f87230',
                alignItems: 'center',
                gap: '6px'
              }}
              onClick={() => setShowMetaMask(true)}
            >
              <Image alt="wallet" src='/wallet.svg' width={18} height={18} /> 
              Connect Wallet
    </Button>


    <Modal
      centered
      show={showMetaMask}
      onHide={() => setShowMetaMask(false)}
      className="wallet-modal"
    >
      <Modal.Header closeButton>
        <Modal.Title>Tip</Modal.Title>
      </Modal.Header>

      <Modal.Body className="daism-tip-body d-flex align-items-center">
        <div className="me-3">
          <Image alt="info" src="/mess.svg" width={40} height={40} />
        </div>
        <div className="daism-tip-text">
          <p>
          1. For computers: If your browser does not have the MetaMask plugin installed, it is recommended to use MetaMask.
            <Link
              href="https://metamask.io"
              target="_blank"
              rel="noreferrer"
              className="text-primary"
            >
              https://metamask.io
            </Link>
          </p>
          <p>2. For mobile: Access directly using the MetaMask app.</p>
        </div>
      </Modal.Body>
    </Modal>
    </>
  );
}

