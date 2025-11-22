class BlockScanner {
  constructor({ web3, contract, storage,cname, step = 1000 }) {
    this.web3 = web3;
    this.contract = contract; // web3.eth.Contract 
    this.storage = storage;   // { get(): string|null, set(v: string) }
    this.step = BigInt(10000);
    this.lastBlock = null;    // BigInt
    this.events = new Map();  // eventName -> { abi, callback }
    this.timer = null;
    this.cname=cname;
  }


  on(eventName, callback) {
    const abiItem = this.contract.options.jsonInterface.find(
      x => x.type === 'event' && x.name === eventName
    );
    if (!abiItem) throw new Error(`Event ${eventName} not found in ABI`);
    this.events.set(eventName, { abi: abiItem, callback });
  }

  async loadLastBlock() {
    const v = await this.storage.get();
    if (v) return BigInt(v);
    const cur = await this.web3.eth.getBlockNumber();
    return BigInt(cur); 
  }

  async saveLastBlock(bn) {
    await this.storage.set(bn.toString());
  }

  async start(pollIntervalMs = 2000) {

    this.lastBlock = await this.loadLastBlock();
       console.log(this.cname,this.lastBlock)

    this.timer = setInterval(() => this._tick().catch(console.error), pollIntervalMs);

    await this._tick();
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
  }

  async _tick() {
    const latestNum = await this.web3.eth.getBlockNumber();
    const latest = BigInt(latestNum);

    if (this.lastBlock >= latest) return;

    let from = this.lastBlock + 1n;
    let to = from + this.step - 1n;
    if (to > latest) to = latest;

    const fromStr = from.toString(); 
    const toStr = to.toString();


    for (const [eventName, { abi, callback }] of this.events.entries()) {
      const topic = this.web3.eth.abi.encodeEventSignature(abi);

      try {
        const logs = await this.web3.eth.getPastLogs({
          address: this.contract.options.address,
          fromBlock: fromStr,
          toBlock: toStr,
          topics: [topic]
        });

        for (const log of logs) {
          try {
              const decoded = this.web3.eth.abi.decodeLog(
              abi.inputs,
              log.data,
              log.topics.slice(1)
            );
   
            await callback({
              event: eventName,
              transactionIndex:log.transactionIndex,
              blockHash:log.blockHash,
              address: log.address,
              blockNumber: log.blockNumber,
              transactionHash: log.transactionHash,
              returnValues: decoded
            });
          } catch (err) {
            console.error(`Error decoding log for ${eventName}:`, err.message);
          }
        }

      } catch (err) {
        const msg = err.message || "";
        if (msg.includes("pruned") || msg.includes("not found")) {
          console.warn(`⚠ block range ${fromStr}-${toStr} was pruned. Skip.`,fromStr,toStr);
        } else if (msg.includes("invalid block range")) {
          console.warn(`⚠ invalid block range ${fromStr}-${toStr}. Skip.`);
        } else {
          console.error(`❌ fetch error for ${eventName}:`, msg);
        }
      }
    }

    this.lastBlock = to;
    await this.saveLastBlock(this.lastBlock);
  }
}

module.exports = BlockScanner;

BlockScanner.instance = null;
module.exports = BlockScanner;

