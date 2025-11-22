import { Dropdown } from 'react-bootstrap';
import { AccountSvg } from '@/lib/jssvg/melosvg';
import { useSelector } from 'react-redux';
import {type RootState} from '@/store/store';
import GeneImg from '@/components/GeneImg';
import Image from 'next/image';

interface ChildProps { 
  onDisconnect:()=>void,
}
export default function User({onDisconnect}: ChildProps) {

  const user = useSelector((state: RootState) => state.valueData.user) as MeloUserInfo;
  
  return (
 
      <Dropdown>
      <Dropdown.Toggle style={{background:'transparent',border:0}} >

        {(user.artist_avatar || user.user_avatar) ? <GeneImg avatar={user.artist_avatar|| user.user_avatar} hw={42} />:<AccountSvg />}
   
      <span className='melo-username'>{user.artist_name || user.user_name}</span>
      </Dropdown.Toggle>

      <Dropdown.Menu className="custom-menu">
        <Dropdown.Item href="/profile"> 
        <Image src="/myprofile.svg" width={20} height={20} alt='' />
        <span style={{display:'inline-block',paddingLeft:'6px'}}>My account...</span></Dropdown.Item>
        <Dropdown.Item href="#" onClick={()=>{ onDisconnect();}} >
        <Image src="/disconnect.svg" width={20} height={20} alt='' />
          <span style={{display:'inline-block',paddingLeft:'6px'}} >Disconnect Wallet</span></Dropdown.Item>

      </Dropdown.Menu>
    </Dropdown>
     
  );
}
