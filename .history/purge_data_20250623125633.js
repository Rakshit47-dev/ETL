/**
 * purge.js
 * --------
 * Dev‑reset helper.
 *
 * ① Clears listed PostgreSQL tables (TRUNCATE … RESTART IDENTITY CASCADE)
 * ② Clears listed Oracle tables
 *    – Disables all FK constraints
 *    – DELETEs every row
 *    – Re‑enables constraints
 *
 * After this you can safely run:
 *    node load_to_oracle.js
 *    node oracle_to_postgres.js
 */
require('./logger');
const oracledb = require('oracledb');
const { Client } = require('pg');

/* ---------------- CONFIG ---------------- */
const oracleConfig = {
  user          : 'system',
  password      : 'Moodle@123',
  connectString : 'localhost:1522/oracle'
};

const pgConfig = {
  user     : 'postgres',
  host     : 'localhost',
  database : 'ETL',
  password : 'Moodle@123',
  port     : 5432
};

/* ---------- TABLE LISTS (edit as needed) ---------- */
const postgresTables = [
  'student_academics'
];

const oracleTables = [
  'MARKS',       // child of STUDENTS & SUBJECTS
  'STUDENTS',    // child of DEPARTMENTS
  'SUBJECTS',
  'DEPARTMENTS',
   'GRADE'      // ← include only if you really want to wipe lookup data
];

/* ---------------- HELPERS ---------------- */
async function purgePostgres(pgClient, tables) {
  console.log('🔧 Deleting rows from PostgreSQL tables …');
  for (const t of tables) {
    try {
      await pgClient.query(`DELETE FROM ${t}`);
      console.log(`🗑️  Deleted rows from ${t} (Postgres)`);
    } catch (err) {
      console.error(`❌ Failed to delete from ${t}:`, err.message);
    }
  }
  await pgClient.query(`COMMIT`);
}


async function disableOracleFKs (conn) {
  console.log('🔧 Disabling Oracle FK constraints …');
  await conn.execute(`
    BEGIN
      FOR c IN (
        SELECT table_name, constraint_name
        FROM   user_constraints
        WHERE  constraint_type = 'R'
          AND  status = 'ENABLED'
      ) LOOP
        EXECUTE IMMEDIATE
          'ALTER TABLE "'||c.table_name||'" DISABLE CONSTRAINT "'||c.constraint_name||'"';
      END LOOP;
    END;`);
}

async function enableOracleFKs (conn) {
  console.log('🔧 Re‑enabling Oracle FK constraints …');
  await conn.execute(`
    BEGIN
      FOR c IN (
        SELECT table_name, constraint_name
        FROM   user_constraints
        WHERE  constraint_type = 'R'
          AND  status = 'DISABLED'
      ) LOOP
        EXECUTE IMMEDIATE
          'ALTER TABLE "'||c.table_name||'" ENABLE CONSTRAINT "'||c.constraint_name||'"';
      END LOOP;
    END;`);
}

async function purgeOracle (conn, tables) {
  await disableOracleFKs(conn);

  console.war('🔧 Deleting rows from Oracle tables …');
  for (const t of tables) {
    await conn.execute(`DELETE FROM ${t}`);
    console.log(`🗑️  Deleted rows from ${t} (Oracle)`);
  }
  await conn.execute(`COMMIT`);

  await enableOracleFKs(conn);
  await conn.execute(`COMMIT`);
  console.log('✅ Oracle purge complete (constraints restored)');
}

/* ---------------- MAIN ---------------- */
async function purgeData () {
  let oracleConn;
  const pgClient = new Client(pgConfig);

  try {
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    /* 1. Postgres */
    await purgePostgres(pgClient, postgresTables);

    /* 2. Oracle */
    await purgeOracle(oracleConn, oracleTables);

    console.log('🎉 Purge finished – databases are now empty.');
  } catch (err) {
    console.error('❌ Purge failed:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

purgeData();
