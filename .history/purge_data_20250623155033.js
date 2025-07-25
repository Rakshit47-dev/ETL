/**
 * purge.js
 * --------
 * Dev‑reset helper.
 *
 * ① Clears listed PostgreSQL tables (DELETE + COMMIT)
 * ② Clears listed Oracle  tables (with FK disable/enable)
 *
 * Reports **exact row counts** removed per table for both databases.
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


console.log

/* ---------- TABLE LISTS ---------- */
const postgresTables = [
  'student_academics'
];

const oracleTables = [
  'MARKS',       // child of STUDENTS & SUBJECTS
  'STUDENTS',    // child of DEPARTMENTS
  'SUBJECTS',
  'DEPARTMENTS',
  'GRADE'        // ← include only if you really want to wipe lookup data
];

/* ---------------- HELPERS ---------------- */
async function purgePostgres (pgClient, tables) {
  console.log('🔧 Deleting rows from PostgreSQL tables …');
  const counts = {};
  for (const t of tables) {
    try {
      const res = await pgClient.query(`DELETE FROM ${t}`);
      counts[t] = res.rowCount ?? 0;
      console.log(`🗑️  Deleted ${counts[t]} row(s) from ${t} (Postgres)`);
    } catch (err) {
      console.error(`❌ Failed to delete from ${t}:`, err.message);
      counts[t] = null;
    }
  }
  await pgClient.query('COMMIT');
  return counts;
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

  console.log('🔧 Deleting rows from Oracle tables …');
  const counts = {};
  for (const t of tables) {
    const res = await conn.execute(`DELETE FROM ${t}`);
    counts[t] = res.rowsAffected ?? 0;
    console.log(`🗑️  Deleted ${counts[t]} row(s) from ${t} (Oracle)`);
  }
  await conn.execute('COMMIT');

  await enableOracleFKs(conn);
  await conn.execute('COMMIT');
  console.log('✅ Oracle purge complete (constraints restored)');

  return counts;
}

function printSummary (title, counts) {
  console.log(`\n=== ${title} ===`);
  Object.entries(counts).forEach(([tbl, cnt]) => {
    if (cnt === null) {
      console.warn(`⚠️  ${tbl}: error – see above.`);
    } else {
      console.info(`${tbl}: ${cnt} row(s) deleted.`);
    }
  });
}

/* ---------------- MAIN ---------------- */
async function purgeData () {
  let oracleConn;
  const pgClient = new Client(pgConfig);
  let pgCounts, oracleCounts;

  try {
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    /* 1. Postgres */
    pgCounts = await purgePostgres(pgClient, postgresTables);

    /* 2. Oracle */
    oracleCounts = await purgeOracle(oracleConn, oracleTables);

    console.log('\n🎉 Purge finished – databases are now empty.');
  } catch (err) {
    console.error('❌ Purge failed:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }

  /* -------- Summary -------- */
  if (pgCounts)     printSummary('PostgreSQL deletion summary', pgCounts);
  if (oracleCounts) printSummary('Oracle deletion summary', oracleCounts);
}

purgeData();
