"use client";

import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Image from "next/image";
import { type ListArtistType } from "@/lib/mysql/message";
import Member from "@/components/Member";

interface AutoGalleryProps {
  items: ListArtistType[];
  interval?: number; // Carousel interval (milliseconds)
  height?:string; 
  maxWidth?:string; 
}

export default function AutoGallery({ items, interval = 4000,height='234px',maxWidth='234px' }: AutoGalleryProps) {
  const [index, setIndex] = useState(0);
  const total = items.length;

  const next = () => setIndex((index + 1) % total);

  useEffect(() => {
    const timer = setInterval(next, interval);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [index, interval]);

  const current = items[index];

  return (

    <div  className="d-flex flex-column align-items-center">
      <div className="position-relative w-100 melo-right-img" >
        <AnimatePresence mode="wait">
          <motion.div
            key={current.user_address}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.8 }}
            className="position-absolute w-100 h-100"
          >
            <Image
              src={current.artist_avatar}
              alt={current.artist_name}
              fill
              sizes={height}
              className="object-fit-cover rounded"
              priority
            />
          
          </motion.div>
        </AnimatePresence>
        <div className="position-absolute d-flex justify-content-center align-items-center bottom-0 melo-right-artist" >
            {current.artist_name}
            </div>
        <div
          className="position-absolute bottom-0 mb-2 d-flex justify-content-center w-100"
          style={{ gap: "6px" }}
        >
          {items.map((_, i) => (
            <motion.span
              key={i}
              onClick={() => setIndex(i)}
              initial={false}
              animate={{
                scale: i === index ? 1.2 : 1,
                opacity: i === index ? 1 : 0.5,
              }}
              whileHover={{ scale: 1.3 }}
              transition={{ type: "spring", stiffness: 300, damping: 20 }}
              style={{
                width: "8px",
                height: "8px",
                borderRadius: "50%",
                backgroundColor: i === index ? "#FFF" : "#bbb",
                cursor: "pointer",
              }}
            />
          ))}
        </div>

        {/* <Button
          variant="dark"
          onClick={prev}
          className="position-absolute top-50 start-0 translate-middle-y ms-2 opacity-75"
          style={{ borderRadius: "50%", width: "40px", height: "40px" }}
        >
          <ChevronLeft size={20} />
        </Button>
        <Button
          variant="dark"
          onClick={next}
          className="position-absolute top-50 end-0 translate-middle-y me-2 opacity-75"
          style={{ borderRadius: "50%", width: "40px", height: "40px" }}
        >
          <ChevronRight size={20} />
        </Button> */}
      </div>

      {/* <div
        className="w-100 mt-3 bg-secondary"
        style={{ maxWidth: "700px", height: "6px", borderRadius: "4px", overflow: "hidden" }}
      >
        <motion.div
          key={index}
          initial={{ width: "0%" }}
          animate={{ width: "100%" }}
          transition={{ duration: interval / 1000, ease: "linear" }}
          className="bg-primary h-100"
        />
      </div> */}

        <div className="mt-2" >
          <Member userName={current.artist_name} avatar={current.artist_avatar} account={current.user_address}
            twitter={current.twitter} facebook={current.facebook} tg={current.tg} instgram={current.instgram}   />
        </div>    
     
    </div>
  );
}
