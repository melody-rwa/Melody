'use client';

interface LoadingProps {
  width?:string;
  height?:string;

}

export default function Loading({ 
  width='64px',height='48px' 

}: LoadingProps) {
  return (

<div className="loading melo-loadding">
  <svg width={width} height={height}>
      <polyline points="0.157 23.954, 14 23.954, 21.843 48, 43 0, 50 24, 64 24" id="back"></polyline>
    <polyline points="0.157 23.954, 14 23.954, 21.843 48, 43 0, 50 24, 64 24" id="front"></polyline>
  </svg>
</div>

  );
}
