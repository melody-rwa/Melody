
const { Web3 } = require('web3');
const Daoapi = require("./index");
const dotenv=require('dotenv');
dotenv.config();


class Server {
    constructor() {
        this.daoapi = null;
        this.web3 = null;
    }

    async start() {
        try {
            const { NEXT_PUBLIC_WSS_URL, ADMINISTRUTOR_ADDRESS } = process.env;
            this.web3 = new Web3(NEXT_PUBLIC_WSS_URL);
            this.daoapi = new Daoapi(this.web3, ADMINISTRUTOR_ADDRESS);
        } catch (error) {
            console.error("Error starting the server:", error);
            throw error; 
        }
    }

    async restart() {
        try {
         
            if (this.web3?.currentProvider?.close) {
                await this.web3.currentProvider.close();
            }

            await this.start();
        } catch (error) {
            console.error("Error restarting the server:", error);
            throw error; 
        }
    }
}

module.exports = Server;