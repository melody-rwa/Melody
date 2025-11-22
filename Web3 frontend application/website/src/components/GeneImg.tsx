import { useState } from 'react';
import { User1Svg } from '@/lib/jssvg/melosvg';
import Image from 'next/image';

interface GeneImgProps {
  avatar: string;
  hw?: number; 
}

export default function GeneImg({ avatar, hw = 64 }: GeneImgProps) {
  const [imgError, setImgError] = useState(false);

  if (!avatar || imgError) {
   
    return <User1Svg size={hw} />;
  }

  return (
    <Image
      src={avatar}
      alt="avatar"
      width={hw}
      height={hw}
      style={{ borderRadius: '50%' }}
      onError={() => setImgError(true)} 
    />
  );
}
