'use client';

import React, { useRef, useState, useEffect } from 'react';
import './styles/logouploader.css';
import { useDispatch } from 'react-redux';
import { setErrText } from '@/store/store';
import { Logo_upload } from '@/lib/jssvg/melosvg';
import Image from 'next/image';

interface LogoUploaderProps {
  initialUrl?: string; 
  onChange: (file: File | null, isModified: boolean) => void;
  maxSizeKB?: number;

}

const LogoUploader=({initialUrl,onChange, maxSizeKB = 100,}: LogoUploaderProps)=> {

  const [previewURL, setPreviewURL] = useState<string>(initialUrl || '');

  const [dragActive, setDragActive] = useState<boolean>(false);

  const [scale, setScale] = useState(1);
  const [position, setPosition] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const dragStartRef = useRef({ x: 0, y: 0 });
  const pinchStartRef = useRef({ distance: 0, scale: 1 });
  const lastTouchRef = useRef<{ x: number; y: number }>({ x: 0, y: 0 });

  const inputRef = useRef<HTMLInputElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const previewRef = useRef<HTMLImageElement | null>(null);

  const dispatch = useDispatch();
  const showError = (str: string) => dispatch(setErrText(str));

  const handleFile = (selectedFile: File) => {
    if (!selectedFile) return;

    const validTypes = [
      'image/jpeg',
      'image/jpg',
      'image/png',
      'image/gif',
      'image/svg+xml',
    ];
    if (!validTypes.includes(selectedFile.type)) {
      showError('Launch&$&Invalid file type. Only JPG/PNG/GIF/SVG allowed.');
      return;
    }

    if (selectedFile.size > maxSizeKB * 1024) {
      showError(`Launch&$&File is too large. Max size: ${maxSizeKB} KB`);
      return;
    }


    const url = URL.createObjectURL(selectedFile);
    setPreviewURL(url);
    // setIsModified(true);

    onChange(selectedFile, true);

    setScale(1);
    setPosition({ x: 0, y: 0 });
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0] || null;
    if (selectedFile) handleFile(selectedFile);
  };

  const handleClear = () => {

    if (previewURL && previewURL !== initialUrl) {
      URL.revokeObjectURL(previewURL);
    }
    setPreviewURL('');
    if (inputRef.current) inputRef.current.value = '';
    setScale(1);
    setPosition({ x: 0, y: 0 });

    // setIsModified(true);
    onChange(null, true);
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(true);
  };
  const handleDragLeave = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(false);
  };
  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const clampPosition = (x: number, y: number) => {
    if (!containerRef.current || !previewRef.current) return { x, y };

    const containerRect = containerRef.current.getBoundingClientRect();
    const imgWidth = previewRef.current.naturalWidth * scale;
    const imgHeight = previewRef.current.naturalHeight * scale;

    const maxX = Math.max((imgWidth - containerRect.width) / 2, 0);
    const maxY = Math.max((imgHeight - containerRect.height) / 2, 0);

    return {
      x: Math.min(Math.max(x, -maxX), maxX),
      y: Math.min(Math.max(y, -maxY), maxY),
    };
  };

  const startDrag = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
    dragStartRef.current = { x: e.clientX - position.x, y: e.clientY - position.y };
  };
  const onDrag = (e: React.MouseEvent) => {
    if (!isDragging) return;
    const newPos = { x: e.clientX - dragStartRef.current.x, y: e.clientY - dragStartRef.current.y };
    setPosition(clampPosition(newPos.x, newPos.y));
  };
  const endDrag = () => setIsDragging(false);

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.05 : 0.05;
    const newScale = Math.min(Math.max(scale + delta, 0.1), 5);
    setScale(newScale);
    setPosition(clampPosition(position.x, position.y));
  };

  const handleTouchStart = (e: React.TouchEvent) => {
    if (e.touches.length === 1) {
      lastTouchRef.current = { x: e.touches[0].clientX, y: e.touches[0].clientY };
    } else if (e.touches.length === 2) {
      const dx = e.touches[0].clientX - e.touches[1].clientX;
      const dy = e.touches[0].clientY - e.touches[1].clientY;
      pinchStartRef.current = { distance: Math.hypot(dx, dy), scale };
    }
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    e.preventDefault();
    if (e.touches.length === 1 && lastTouchRef.current) {
      const dx = e.touches[0].clientX - lastTouchRef.current.x;
      const dy = e.touches[0].clientY - lastTouchRef.current.y;
      const newPos = { x: position.x + dx, y: position.y + dy };
      setPosition(clampPosition(newPos.x, newPos.y));
      lastTouchRef.current = { x: e.touches[0].clientX, y: e.touches[0].clientY };
    } else if (e.touches.length === 2) {
      const dx = e.touches[0].clientX - e.touches[1].clientX;
      const dy = e.touches[0].clientY - e.touches[1].clientY;
      const distance = Math.hypot(dx, dy);
      const newScale = Math.min(
        Math.max((distance / pinchStartRef.current.distance) * pinchStartRef.current.scale, 0.1),
        5
      );
      setScale(newScale);
      setPosition(clampPosition(position.x, position.y));
    }
  };

  const handleTouchEnd = (e: React.TouchEvent) => {
    if (e.touches.length < 2) pinchStartRef.current.distance = 0;
  };

  useEffect(() => {
    return () => {
      if (previewURL && previewURL !== initialUrl) {
        URL.revokeObjectURL(previewURL);
      }
    };
  }, [previewURL, initialUrl]);

  return (
    <div style={{width:'140px'}}
      className={`upload-box ${dragActive ? 'drag-active' : ''}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      ref={containerRef}
    >
    <input
        type="file"
        id="logoFile"
        accept=".jpg,.jpeg,.png,.gif,.svg"
        hidden
        ref={inputRef}
        onChange={handleFileChange}
    />
    
    <label htmlFor="logoFile" className="upload-label">
      {previewURL ? (
        <Image
          ref={previewRef}
          src={previewURL}
          alt="Logo Preview"
          width={140}
          height={140}
          className="logo-preview"
          style={{
            transform: `translate(${position.x}px, ${position.y}px) scale(${scale})`,
            cursor: isDragging ? 'grabbing' : 'grab',
          }}
          onMouseDown={startDrag}
          onMouseMove={onDrag}
          onMouseUp={endDrag}
          onMouseLeave={endDrag}
          onWheel={handleWheel}
          onTouchStart={handleTouchStart}
          onTouchMove={handleTouchMove}
          onTouchEnd={handleTouchEnd}
          draggable={false}
        />
      )  : (
      
          <Logo_upload />

      )}
    </label>

      {previewURL && (
        <button type="button" className="clear-btn" onClick={handleClear}>
          ×
        </button>
      )}
    </div>
  );
}

LogoUploader.displayName = "LogoUploader";
export default React.memo(LogoUploader);