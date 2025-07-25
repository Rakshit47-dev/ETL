/**
 * purge_data.js
 * -------------
 * Deletes all data from the Oracle source tables
 * and the PostgreSQL target table `student_academics`.
 * Adjust TABLES arrays if you add / remove tables later.
 */

const oracledb = require('oracledb');
const { Client } = require('pg');

/* ---------- CONFIG (same creds you already use) ---------- */
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

/* ---------- TABLE LISTS ---------- */
const oracleTables   = [            // child→parent order if FK constraints
  'MARKS',
  'STUDENTS',
  'DEPARTMENTS',
  'GRADE'           // keep here only if you really want to clear the lookup
];

const postgresTables = [
  'student_academics'
];

/* ---------- MAIN ---------- */
async function purgeData () {
  let oracleConn;
  const pgClient = new Client(pgConfig);

  try {
    /* 1. Connect */
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    /* 2. Purge Postgres */
    for (const t of postgresTables) {
      await pgClient.query(`TRUNCATE TABLE ${t} RESTART IDENTITY CASCADE`);
      console.log(`🗑️  Cleared ${t} (Postgres)`);
    }

    /* 3. Purge Oracle */
    for (const t of oracleTables) {
      await oracleConn.execute(``);
      console.log(`🗑️  Cleared ${t} (Oracle)`);
    }

    console.log('🎉 Purge completed – databases are empty.');

  } catch (err) {
    console.error('❌ Purge failed:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

purgeData();
