'use client';
import {  FormEvent, useState } from "react";
import {  Button, Form, FormControl, InputGroup } from "react-bootstrap";
import { checkAddress } from "@/lib/utils/js";

interface AddArItem { index: number; isErr: boolean; }

interface MusicPlayerProps {
    showError:(v:string)=>void;
    onSubmit:(v:string[])=>void;
}

export default function HandInput({showError,onSubmit}: MusicPlayerProps) {   
    const [addAr, setAddAr] = useState<AddArItem[]>([]);
    const [errorFirrst, setErrorFirrst] = useState(false);

    const addMember = () => {
        if (addAr.length) setAddAr([...addAr, { index: addAr[addAr.length - 1].index + 1, isErr: false}]);
        else setAddAr([{ index: 0, isErr: false }]);
    };

    const delMember = (event: React.MouseEvent<HTMLButtonElement>) => {
        const _num = parseInt(event.currentTarget.getAttribute('data-key')!);
        for (let i = 0; i < addAr.length; i++) {
        if (addAr[i].index === _num) { addAr.splice(i, 1); setAddAr([...addAr]); }
        }
    };

  const submit= async(event: FormEvent<HTMLFormElement>) =>{
    event.preventDefault(); event.stopPropagation();
    const form = event.currentTarget;     
    let errAmount = 0;

    const _temp = (form.elements.namedItem('firstName') as HTMLInputElement).value.trim();
    if (!_temp || !checkAddress(_temp)) {
      errAmount++;
      setErrorFirrst(true);
    }

    const newAddAr = addAr.map(v => {
      const val = (form.elements.namedItem('firstName' + v.index) as HTMLInputElement).value.trim();
      if (!val || !checkAddress(val)) {
        errAmount++;
        return { ...v, isErr: true }; 
      }
      return { ...v, isErr: false }; 
    });

    setAddAr(newAddAr);

    if (errAmount > 0) {
      showError('Allowlist&$&Validation failed. Please correct the errors and try again.');
      return;
    }

    const members = [(form.elements.namedItem('firstName') as HTMLInputElement).value.trim()];
    addAr.forEach(v => {
      members.push((form.elements.namedItem('firstName' + v.index) as HTMLInputElement).value.trim());
    });


    if(members.length===0) showError("Allowlist&$&No valid wallet addresses found.");
    else onSubmit(members);
 
  }

  return (<>
     <Form onSubmit={submit}>
            <InputGroup hasValidation className="mb-2">
              <InputGroup.Text  >1</InputGroup.Text>
              <FormControl id='firstName' isInvalid={errorFirrst} type="text" placeholder="0x" 
              onFocus={() => { setErrorFirrst(false) }}  />
              <Button className="btn-custom" onClick={addMember}>Add</Button>
              <Form.Control.Feedback type="invalid">Must begin with 0x and be followed by 40 alphanumeric characters (case-sensitive).</Form.Control.Feedback>
            </InputGroup>

            {addAr.map((placement,idx) => (<div key={'org_' + placement.index} >
                <InputGroup hasValidation className="mb-2">
                <InputGroup.Text>{idx+2} </InputGroup.Text>
                  <FormControl id={'firstName' + placement.index} isInvalid={placement.isErr} 
                  onFocus={() => { placement.isErr = false; setAddAr([...addAr]) }} type="text" placeholder="0x" defaultValue="" />
                  <Button  className="btn-custom"  data-key={placement.index} onClick={delMember}>Del</Button>
                  <Form.Control.Feedback type="invalid">Must begin with 0x and be followed by 40 alphanumeric characters (case-sensitive).</Form.Control.Feedback>
                </InputGroup>
              
              </div>))
            }

            <div  className="mt-3 mb-3"  style={{ textAlign: 'center' }}>
                <Button   className="btn-custom"  type="submit" >
                  Submit
                </Button>
            </div>
          </Form>

  </>);
}

