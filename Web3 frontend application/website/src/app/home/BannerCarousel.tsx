import React from 'react';
import { Carousel } from 'react-bootstrap';
import Image from 'next/image';
import './styles/carousel.css?a=021';
const images = [
    '/Banner1.png',
    '/Banner2.png',
    '/Banner3.png',
  ];

  export default function BannerCarousel() {
    return (
      <Carousel style={{overflow:'hidden'}} interval={3000} fade>
        {images.map((src, i) => (
          <Carousel.Item key={i}>
            <div className='melo-banece'
              style={{ borderRadius:'10px',
                position: 'relative',
                width: '100%',
                // height: 'clamp(100px, 20vw, 300px)', // Minimum height of 150px, scaling with screen width, maximum height of 300px
                overflow: 'hidden',
              }}
            >
              <Image
                src={src}
                alt={`slide-${i}`}
                fill
                priority={i === 0}
                style={{
                  objectFit: 'cover',
                  objectPosition: 'center',
                }}
              />
            </div>
            {i===0?<FirstCaption />:i===1?<SecondCaption />:<ThreeCaption />}
         
          </Carousel.Item>
        ))}
      </Carousel>
    );
  }


  function FirstCaption(){
    return (<>
      <Carousel.Caption style={{position:'absolute',top:'8%',left: '12%',textAlign: 'left'}}>
      <Image src='/logo.png' width={170} height={40} alt='' />
      <p style={{color:'#FFF',fontSize:'22px'}} >Music asset underlying liquidity platform</p>
     </Carousel.Caption>
     <Carousel.Caption style={{position:'absolute',bottom:'12%',right: '1%',textAlign: 'right'}}>
      <div style={{color:'#FAFAFA',fontSize:'11.5px',fontWeight:700}} >Background Album</div>
      <div className='mt-3 mb-1' style={{color:'#FAFAFA',fontSize:'22px',fontWeight:400}} >Artist</div>
      <div style={{color:'#FAFAFA',fontSize:'38px',fontWeight:400}} >Circles</div>
     </Carousel.Caption>
     <Carousel.Caption style={{position:'absolute',bottom:'2%',left: '1%',textAlign: 'left'}}>
      <div style={{paddingLeft:'10px', color:'#959D99',fontSize:'13.5px',fontWeight:400}} >Public Bata</div>
      <div className='mt-2 mb-2' style={{color:'#FAFAFA',fontSize:'18px',fontWeight:700}} ><svg width="5" height="5" viewBox="0 0 5 5" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle cx="2.38442" cy="2.38442" r="2.38442" fill="white"/>
      </svg> Melody</div>
      <div style={{paddingLeft:'10px',color:'#959D99',fontSize:'13.5px',fontWeight:400}} >MusicFi</div>
     </Carousel.Caption></>
    );
  }

  function SecondCaption(){
    return (<>
     <Carousel.Caption style={{position:'absolute',bottom:'12%',right: '1%',textAlign: 'right'}}>
      <div style={{color:'#FAFAFA',fontSize:'11.5px',fontWeight:700}} >Background Album</div>
      <div className='mt-3 mb-1' style={{color:'#FAFAFA',fontSize:'22px',fontWeight:400}} >Artist</div>
      <div style={{color:'#FAFAFA',fontSize:'38px',fontWeight:400}} >Kater</div>
     </Carousel.Caption>
     <Carousel.Caption style={{position:'absolute',bottom:'2%',left: '1%',textAlign: 'left'}}>
      <div style={{paddingLeft:'10px', color:'#959D99',fontSize:'13.5px',fontWeight:400}} >Melody</div>
      <div className='mt-2 mb-2' style={{color:'#FAFAFA',fontSize:'18px',fontWeight:700}} ><svg width="5" height="5" viewBox="0 0 5 5" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle cx="2.38442" cy="2.38442" r="2.38442" fill="white"/>
      </svg> MusicFi </div>
      <div style={{paddingLeft:'10px',color:'#959D99',fontSize:'13.5px',fontWeight:400}} >Public Bata</div>
     </Carousel.Caption></>
    );
  }

  function ThreeCaption(){
    return (<>
     <Carousel.Caption style={{position:'absolute',bottom:'12%',right: '1%',textAlign: 'right'}}>
      <div style={{color:'#FAFAFA',fontSize:'11.5px',fontWeight:700}} >Background Album</div>
      <div className='mt-3 mb-1' style={{color:'#FAFAFA',fontSize:'22px',fontWeight:400}} >Artist</div>
      <div style={{color:'#FAFAFA',fontSize:'38px',fontWeight:400}} >Firewall</div>
     </Carousel.Caption>
     <Carousel.Caption style={{position:'absolute',bottom:'2%',left: '1%',textAlign: 'left'}}>
      <div style={{paddingLeft:'10px', color:'#959D99',fontSize:'13.5px',fontWeight:400}} >Melody</div>
      <div className='mt-2 mb-2' style={{color:'#FAFAFA',fontSize:'18px',fontWeight:700}} ><svg width="5" height="5" viewBox="0 0 5 5" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle cx="2.38442" cy="2.38442" r="2.38442" fill="white"/>
      </svg> Public Bata </div>
      <div style={{paddingLeft:'10px',color:'#959D99',fontSize:'13.5px',fontWeight:400}} >MusicFi</div>
     </Carousel.Caption></>
    );
  }