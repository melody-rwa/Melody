import {configureStore, createSlice, PayloadAction} from '@reduxjs/toolkit';

  interface CounterState {
    user: MeloUserInfo;
    ethBalance: string;
    tipText: string;
    messageText: string; 
    errText: string; 
    where: string;
    current:number;
    messText:string; 

  }

const initialState: CounterState = 
{


    user:{connected:0,chainId:0,networkName:'',user_address:'',
      user_avatar:'',user_name:'',user_desc:'',user_type:0,user_link:'',
      twitter:'',tg:'',facebook:'',instgram:'',
      artist_name:'',artist_avatar:'',artist_desc:'',artist_link:'' },
    ethBalance:'0', 
    tipText:'', 
    messageText:'', 
    errText:'',
    where:'',  
    messText:'',
    current:0, 

};

const valueDataSlice = createSlice({
  name: 'valueData',
  initialState,
  reducers: {
    setMess: (state, action: PayloadAction<string>) => {
      state.messText = action.payload;
    },
    setUser: (state, action: PayloadAction<MeloUserInfo>) => {
      state.user = action.payload;
    },
    setCurrent: (state, action: PayloadAction<number>) => {
      state.current = action.payload;
    },
  
    setEthBalance: (state, action:PayloadAction<string>) => {
        state.ethBalance = action.payload;
      },
   
    setTipText: (state, action:PayloadAction<string>) => {
        state.tipText = action.payload;
      },
    setMessageText: (state, action:PayloadAction<string>) => {
        state.messageText = action.payload;
      },
      setErrText: (state, action:PayloadAction<string>) => {
        state.errText = action.payload;
      },

    setWhere: (state, action:PayloadAction<string>) => {
      state.where = action.payload;
    }
  }
});

export const { setEthBalance,setTipText,setMessageText,setUser,setErrText,setWhere,setCurrent,setMess} 
    = valueDataSlice.actions

export const store = configureStore({
    reducer: {valueData: valueDataSlice.reducer}
  });

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
