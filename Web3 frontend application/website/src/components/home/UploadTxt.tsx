'use client';
import {  useState } from "react";
import { Alert,  Button,  Container, Form } from "react-bootstrap";
import Papa from "papaparse";
import { checkAddress } from "@/lib/utils/js";

interface MusicPlayerProps {

    showError:(v:string)=>void;
    onSubmit:(v:string[])=>void;
}

export default function UploadTxt({showError,onSubmit}: MusicPlayerProps) {   

    const [addresses, setAddresses] = useState<string[]>([]);
    const addressRegex = /0x[a-fA-F0-9]{40}/g;
    const handleCsv=(file:File)=>{
        Papa.parse(file, {
            header: false, 
            skipEmptyLines: true,
            complete: (results) => {
              const rows = results.data as string[][];
      
              const allText = rows.flat().join(","); 
              const matches = allText.match(addressRegex) || [];
              const uniqueAddresses = Array.from(new Set(matches));
      
              setAddresses(uniqueAddresses);
           
            },
            error: (err) => {
              console.error("CSV Parsing error:", err);
            },
          });
    }

const validTypes = [
    'text/plain',
    'text/csv',
  ];
  
    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) {
            setAddresses([]);
            return;
        } 
     
        if (!validTypes.includes(file.type)) {
        showError?.('Invalid file type. Only csv/txt allowed.');
        setAddresses([]);
        return;
        }

        if(file.type==='text/csv') handleCsv(file);
        else {
        const reader = new FileReader();
        reader.onload = (event) => {
            const text = event.target?.result as string;
            const tAr=text.split(/[,，;；]/)
              .map((s) => s.trim())
              .filter((s) => checkAddress(s));
            const uniqueAddresses = Array.from(new Set(tAr));
            setAddresses(uniqueAddresses);
         
        };
    
        reader.onerror = (err) => {
            console.error("File read error:", err);
        };
    
        reader.readAsText(file, "utf-8"); 
        }
    
      
      };

    const submit=async ()=>{
        if(addresses.length===0) showError("Allowlist&$&No valid wallet addresses found.");
        else onSubmit(addresses);
    }

  return (<>
    <Container>
        <div  className="mt-2 mb-1">Upload a .txt or .csv file, separated by commas or semicolons.</div>
    <Form.Control 
              type="file"
              required
              name="file"
              onChange={handleFileChange}
              accept=".txt,.csv"
            />
     
   {addresses.length>0 && <Alert className="mt-2 mb-3" variant="dark"> 
        Found {addresses.length} valid addresses.
        </Alert>}
  </Container>
   <div className="mt-3 mb-3" style={{textAlign:'center'}} >
        <Button className="btn-custom"  disabled={addresses.length===0} onClick={submit} >Submit</Button>
   </div>

  </>);
}

