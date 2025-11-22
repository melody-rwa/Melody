
import React, { useState } from "react";
import ProfileShow from "./ProfileShow";
import Edit from "./Edit";

const Show=({user}:{user:MeloUserInfo})=> {
    const [st,setSt]=useState('profileshow'); //profileshow /edit

    return (
    <> 
    {st==='profileshow'?
        <ProfileShow user={user} setSt={setSt} />
        :<Edit user={user} setSt={setSt} />
    }
    </> 
  );
}


export default Show;