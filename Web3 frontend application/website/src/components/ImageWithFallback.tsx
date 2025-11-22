import Image from 'next/image';
import { useState, useEffect, useRef, ImgHTMLAttributes } from 'react';

interface ImageWithFallbackProps extends Omit<ImgHTMLAttributes<HTMLImageElement>, 'src'> {
  src: string | null | undefined;
  fallback?: string;
  alt: string;
}

export default function ImageWithFallback({ 
  src, 
  alt, 
  width, 
  height, 
  fallback = '/favicon.ico', 
  ...props 
}: ImageWithFallbackProps) {
  const [imgSrc, setImgSrc] = useState<string>(src || fallback);
  const imgRef = useRef<HTMLImageElement>(null);
  const[realWidth,setRealWidth]=useState(0);
  const[realHeigh,setRealHeight]=useState(0);

  useEffect(() => {

    setImgSrc(src || fallback);
    

    if (!src) {
      setImgSrc(fallback);
      return;
    }


    const img =  new window.Image();
    img.onload = () => {
      setImgSrc(src);
      setRealHeight(img.height);
      setRealWidth(img.width)
      // resolve({ width: , height:  });
    };
    img.onerror = () => {
      setImgSrc(fallback);
    };
    img.src = src;


    return () => {
      img.onload = null;
      img.onerror = null;
    };
  }, [src, fallback]);

  const handleError = (e: React.SyntheticEvent<HTMLImageElement, Event>) => {
    const target = e.target as HTMLImageElement;
    target.src = fallback;
    setImgSrc(fallback);
  };

  return (
    <Image
      ref={imgRef}
      alt={alt}
      width={width?Number(width):realWidth>0?realWidth:30}
      height={height?Number(height):realHeigh>0?realHeigh:30}
      src={imgSrc}
      placeholder="blur"
      blurDataURL="/user.svg" 
      // style={{ borderRadius: '10px', ...style }}
      onError={handleError}
      {...props}
    />
  );
}
