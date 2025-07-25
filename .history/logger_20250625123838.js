

const fs   = require('fs');
const path = require('path');
const { Client } = require('pg');

/* ---------- FILE LOG CONFIG ---------- */
const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

function getLogStream () {
  const file = path.join(logDir,
    'ETL_logs '+
    new Date().toISOString().slice(0,10) + '.log'   // YYYY‑MM‑DD.log
  );
  return fs.createWriteStream(file, { flags: 'a' });
}
let logStream = getLogStream();

/* ---------- OPTIONAL DB LOG CONFIG ---------- */
const ENABLE_DB_LOG = true;   
const pgConfig = {
  user     : 'postgres',
  host     : 'localhost',
  database : 'ETL',
  password : 'Moodle@123',
  port     : 5432
};
let pgClient;
(async () => {
  if (!ENABLE_DB_LOG) return;
  pgClient = new Client(pgConfig);
  try {
    await pgClient.connect();
    await pgClient.query(`
      CREATE TABLE IF NOT EXISTS etl_console_logs (
        id         SERIAL PRIMARY KEY,
        log_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
        level      TEXT        NOT NULL,
        message    TEXT        NOT NULL
      );
    `);
  } catch (e) {
    process.stderr.write('⚠️  DB logging disabled: ' + e.message + '\n');
    pgClient = null;
  }
})();

/* ---------- OVERRIDE CONSOLE METHODS ---------- */


['log', 'warn', 'error', 'info', 'debug'].forEach(level => {
  const orig = console[level].bind(console);

  console[level] = (...args) => {
    /* 1. Build single string */
    const msg = args.map(a =>
      typeof a === 'string' ? a :
      typeof a === 'object' ? JSON.stringify(a) :
      String(a)
    ).join(' ');

    /* 2. Timestamp */
    const stamp = new Date().toISOString(); // 2025‑06‑19T12:34:56.789Z

    /* 3. Write to file (rotate daily) */
    const today = new Date().toISOString().slice(0,10);
    if (!logStream.path.endsWith(`${today}.log`)) {
      logStream.end();              // close yesterday’s file
      logStream = getLogStream();   // open new one
    }
    logStream.write(`[${stamp}] [${level.toUpperCase()}] ${msg}\n`);

    /* 4. Write to DB ) */
    if (pgClient) {
      pgClient.query(
        'INSERT INTO etl_console_logs(level,message) VALUES($1,$2)',
        [level.toUpperCase(), msg]
      ).catch(() => {}); // swallow errors to avoid recursion
    }

    /* 5. Echo to original console */
    orig(...args);
  };
});
