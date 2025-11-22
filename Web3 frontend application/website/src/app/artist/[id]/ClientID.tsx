
import ArtistShow from '../ArtistShow';

interface PropsType {
    data:UserInfo
}

export default async function ClientID({ data }: PropsType) {
  
    return ( 
        <ArtistShow user={data} isEdit={false} />
    );
}

