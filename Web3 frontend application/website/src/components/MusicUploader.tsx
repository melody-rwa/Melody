'use client';

import { useState, useRef, useEffect } from 'react';
import { Form, InputGroup } from 'react-bootstrap';

interface MusicUploaderProps {
  onUpload?: (file: File, duration: number) => void; 
  showError:(v:string)=>void
}

export default function MusicUploader({ onUpload,showError }: MusicUploaderProps) {
  const [musicFile, setMusicFile] = useState<File | null>(null);
  const [musicURL, setMusicURL] = useState<string | null>(null);
  const [musicSeconds, setMusicSeconds] = useState<number>(0);
  const audioRef = useRef<HTMLAudioElement | null>(null);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0] ?? null;
    if (!file) return;

    if (file.size > 50 * 1024 * 1024) {
      showError('Launch&$&File size exceeds 50MB');
      return;
    }

    setMusicFile(file);

    if (audioRef.current) {
      audioRef.current.pause();
      audioRef.current.currentTime = 0;
    }

    if (musicURL) URL.revokeObjectURL(musicURL);

    const url = URL.createObjectURL(file);
    setMusicURL(url);
    setMusicSeconds(0);
  };

  const handleLoadedMetadata = (e: React.SyntheticEvent<HTMLAudioElement, Event>) => {
    const target = e.currentTarget;
    const duration = Math.floor(target.duration);
    setMusicSeconds(duration);

    if (musicFile && onUpload) {
      onUpload(musicFile, duration);
    }

    target.play().catch((err) => console.warn('Auto-play failed:', err));
  };

  useEffect(() => {
    return () => {
      if (musicURL) URL.revokeObjectURL(musicURL);
    };
  }, [musicURL]);

  return (
    <div className='mb-3' >
      <InputGroup className="mb-3">
        <InputGroup.Text >Upload Music</InputGroup.Text>
        <input type="file" id="musicFile" accept=".mp3,.wav" style={{ display: 'none' }} onChange={handleChange} />
        <Form.Control type="text" readOnly
          onClick={() => document.getElementById('musicFile')?.click()}
          value={ musicFile? musicFile.name
              : 'Upload An Original Music file (MP3/WAV, Max 50MB)'
          }
        />

     
      </InputGroup>


      {musicURL && (
        <div style={{ display: "flex", alignItems: "center", flex: 1 }}>
          <audio
            controls
            src={musicURL}
            onLoadedMetadata={handleLoadedMetadata}
            style={{ flex: 1 }} 
          />
          <span style={{ marginLeft: "8px", whiteSpace: "nowrap" }}>
            {musicSeconds} s
          </span>
        </div>
      )}
    </div>
  );
}
