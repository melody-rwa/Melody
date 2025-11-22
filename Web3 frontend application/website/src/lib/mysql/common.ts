import mysql, { Pool } from 'mysql2/promise';

let promisePool: Pool | null = null;

async function createConnection(): Promise<Pool> {
  if (promisePool) return promisePool;

  promisePool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    port: process.env.MYSQL_PORT ? parseInt(process.env.MYSQL_PORT) : 3306,
    waitForConnections: true,
    connectionLimit: 20,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0,
  });

  return promisePool;
}

export async function getData(sql: string,sqlParams: any[] = [],single = false): Promise<any | any[]> {
  const pool = await createConnection();

  if (Number(process.env.IS_DEBUGGER??'0') === 1) {
    console.info(`${new Date().toLocaleString()}: getData: ${sql} --> ${sqlParams.join()}`);
  }
    

  try {
    const [rows] = await pool.query<any[]>(sql, sqlParams);
    return single ? (rows[0] ?? {}) : rows;
  } catch (error) {
    console.error('Database query error:', error);
    return single ? {} : [];
  }
}

export async function execute(sql: string,sqlParams: any[] = []): Promise<number> {
  
  if (Number(process.env.IS_DEBUGGER??'0') === 1) {
    console.info(`${new Date().toLocaleString()}: execute: ${sql} --> ${sqlParams.join()}`);
  }
    
  const pool = await createConnection();
  let attempt = 0;
  const maxRetries = 3;

  while (attempt < maxRetries) {
    try {
      const [result] = await pool.execute<any>(sql, sqlParams);
      if (sql.trim().toUpperCase().startsWith('CALL')) return 1; 
      else return (result as any).affectedRows as number;   
    } catch (err: any) {
      if (err.code === 'ER_LOCK_DEADLOCK') {
        attempt++;
        console.warn(`⚠️ Deadlock detected, retrying...`);
        await new Promise(res => setTimeout(res, 200 * attempt));
      } else {
        console.error(`Execute error: ${sql} --> ${sqlParams.join()}`, err);
        return 0;
      }
    }
  }

  return 0;
}

export async function executeID(sql: string, sqlParams: any[] = []): Promise<number> {

   
  if (Number(process.env.IS_DEBUGGER??'0') === 1) {
    console.info(`${new Date().toLocaleString()}: executeID: ${sql} --> ${sqlParams.join()}`);
  }

  try {
    const pool = await createConnection();
    const [result] = await pool.execute(sql,sqlParams );
    const insertId = (result as any).insertId;
    return Number(insertId??'0');
  } catch (error) {
    console.error('Database executeID error:', error);
    return 0;
  }

}

// getJsonArray
export async function getJsonArray(
  cid: string,
  sqlParams: any[] = [],
  single = false
): Promise<any | any[]> {
  const pool = await createConnection();
  const [rows] = await pool.query<any[]>("SELECT sqls FROM aux_tree WHERE id=?", [cid]);
  const sql = rows[0]?.sqls as string;

  if (Number(process.env.IS_DEBUGGER??'0') === 1) 
    console.info(`${new Date().toLocaleString()}: ${cid} --> getJsonArray: ${sql} --> ${sqlParams.join()}`);

  try {
    const [resultRows] = await pool.query<any[]>(sql, sqlParams);
    return single ? (resultRows[0] ?? {}) : resultRows;
  } catch (error) {
    console.error(`getJsonArray error for cid=${cid}:`, error);
    return single ? {} : [];
  }
}


export async function getPageData(
  tid: string,
  ps: number,
  pi: number,
  s: string,
  a: 'asc' | 'desc',
  w: string
): Promise<{ rows: any[]; total: number; pages: number }> {
  if (Number(process.env.IS_DEBUGGER??'0') === 1) 
    console.info(`${new Date().toLocaleString()}: getPageData --> ${[tid, ps, pi, s, a, w].join()}`);

  const re: any = await getData('CALL get_page(?,?,?,?,?,?)', [tid, ps, pi, s, a, w]);
  const rows = re[0] ?? [];
  const total = re[1]?.[0]?.mcount ?? 0;
  const pages = Math.ceil(total / ps);

  return { rows, total, pages };
}

async function gracefulShutdown() {
  console.info('Shutting down gracefully...');
  if (promisePool) {
    await promisePool.end();
    console.info('Database connection pool closed.');
  }
  process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});
