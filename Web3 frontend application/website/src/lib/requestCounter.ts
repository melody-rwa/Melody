
const globalForCounter = globalThis as unknown as {
    requestCounter?: { count: number };
  };
  

  export const requestCounter =
    globalForCounter.requestCounter || { count: 0 };
 
  if (process.env.NODE_ENV !== "production") {
    globalForCounter.requestCounter = requestCounter;
  }

  export function incrementRequestCount() {
    requestCounter.count += 1;
  }

  export function getRequestCount() {
    return requestCounter.count;
  }
  