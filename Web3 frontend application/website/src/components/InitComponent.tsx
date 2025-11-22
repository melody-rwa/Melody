'use client';

import React from 'react';
import Loddingwin from './Loddingwin';
import ShowTip from './ShowTip';
import ShowErrWin from './ShowErrWin';

// import AnimatedModal from './AnimatedModal';


export default function InitComponent() {
  return (
   <>
   <Loddingwin />
    <ShowTip />
    <ShowErrWin />
    {/* <AnimatedModal  /> */}
   </>
  );
}


