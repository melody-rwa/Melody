
"use client";

import MusicItem from "./MusicItem";
import {Table} from "react-bootstrap";
import React from "react";
import { useLayout } from "@/contexts/LayoutContext";

interface PropsType{
  rows:MusicType[];
  states:string;
  user:MeloUserInfo;
  isEdit?:boolean;
  refetch:()=>void;

}

const MusicGrid=({rows,states,user,refetch,isEdit=true}:PropsType)=> {
    const { setSelectedRow,setSelectedLeftRow } = useLayout(); 

  return (
    <div style={{width:'100%',overflowX:'auto'}} >
    <Table className="music-table" >
    <thead >
      <tr>
        <th style={{paddingLeft:'40px'}}>Music</th>
        <th>Ticker</th>
        {states.startsWith('artist') && <th style={{textAlign:'center'}}>Start Time</th>}
        {!states.startsWith('artist') && <th style={{paddingLeft:'20px'}}>Artist</th>}
        
        {states.startsWith('user') && <th style={{textAlign:'center'}} >Invested</th>}
        
        {(!states.startsWith('artist') && !states.startsWith('user')) && <th style={{textAlign:'center'}} >Supply</th>}
        {!states.startsWith('user') && <th style={{textAlign:'center'}} >Progress</th>}
        {/* {states.startsWith('artist') && <th>M-NFT</th>} */}
        {/* {(!states.startsWith('artist') && !states.startsWith('user'))  && <th>Limit</th>} */}
        {!states.startsWith('artist') && !states.startsWith('user')  && <th style={{textAlign:'center'}} >End time</th>}
        {!states.startsWith('artist') && !states.startsWith('user') && <th style={{textAlign:'center'}} >Participate</th>}
        {states.startsWith('user') && <th style={{textAlign:'center'}} >Allocation</th>}
        

        {!states.startsWith('artist')  &&<th style={{textAlign:'center'}} >{states.startsWith('user')?'  Status':'Allocation' }</th>}

        {states.startsWith('user') && <th style={{textAlign:'center'}} >Infomation</th>}

       
        {states.startsWith('artist')&& isEdit && <th style={{textAlign:'center'}} >Whitelist</th>}
        {states.startsWith('artist')&& isEdit && <th style={{textAlign:'center'}} >LP Allocation Released</th>}
        {states.startsWith('artist')&& isEdit && <th style={{textAlign:'center'}}>LP Fee</th>}

      </tr>
    </thead>
    <tbody>
    
      {rows.map((obj, index) => (
        <MusicItem musicObj={obj} states={states} refetch={refetch} user={user} isEdit={isEdit}
          key={index}
          index={index}
          onSelect={(i) => {setSelectedRow(i);setSelectedLeftRow(null);}} 
        />
      ))}
    </tbody>
  </Table>
  </div>
  );
}

MusicGrid.displayName="MusicGrid";
export default React.memo(MusicGrid);