
import ClientID from './ClientID';
import { getUser } from '@/lib/mysql/message';
import { notFound } from 'next/navigation';

interface PropsType {params: Promise<{ id:string;}>}

export default async function ArtistIDPage({ params }: PropsType) {
    const { id } = await params;
    const dataAr=await getUser({did:id})
    if(!dataAr || !Array.isArray(dataAr)|| dataAr.length===0)
    {
       return notFound(); 
    }
    else 
    {
        return ( <ClientID data={dataAr[0]} />);
    }
    
}

