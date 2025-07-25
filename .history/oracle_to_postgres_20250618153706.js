/**
 * oracle_to_postgres.js
 *
 * One‑shot ETL: Oracle  ➜  PostgreSQL
 * ---------------------------------------------------------------
 * • Pulls student / department / marks / grade data from Oracle
 * • Calculates average GPA per student
 * • Upserts into PostgreSQL table: student_academics
 * ---------------------------------------------------------------
 * Prerequisites:
 *   - Oracle Instant Client installed (adjust libDir below)
 *   - Node‑oracledb   :  npm i oracledb
 *   - pg (node‑postgres): npm i pg
 */

const oracledb = require('oracledb');
const { Client } = require('pg');

// ---------- Oracle client & connection ----------
oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'  // ← adjust if needed
});

const oracleConfig = {
  user:          'system',
  password:      'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

// ---------- PostgreSQL connection ----------
const pgClient = new Client({
  user:     'postgres',
  host:     'localhost',
  database: 'ETL',
  password: 'Moodle@123',
  port:     5432
});

// ---------- main ETL ----------
async function transferData () {
  let oracleConn;

  try {
    // 1. Connect to both databases
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    // 2. Pull all needed rows from Oracle
    const oracleQuery = `
      SELECT
        s.STUDENTS_STU_ID,                 -- 0
        s.STUDENTS_FIRST_NAME,             -- 1
        s.STUDENTS_LAST_NAME,              -- 2
        s.STUDENTS_EMAIL,                  -- 3
        s.STUDENTS_JOINING_DATE,           -- 4
        d.DEPARTMENTS_DEPT_NAME,           -- 5
        m.MARKS,                           -- 6
        g.GPA_EQUIVALENT                   -- 7
      FROM   STUDENTS   s
      JOIN   DEPARTMENTS d ON s.STUDENTS_DEPT_ID = d.DEPARTMENTS_DEPT_ID
      JOIN   MARKS       m ON s.STUDENTS_STU_ID  = m.MARKS_STUDENT_ID
      JOIN   SUBJECTS    sub ON m.MARKS_SUBJECT_ID = s.SUBJECT_ID
      JOIN   GRADE       g  ON
             TO_NUMBER(SUBSTR(g.PERCENTAGE_RANGE, 1,
                               INSTR(g.PERCENTAGE_RANGE, '-') - 1)) <= m.MARKS
        AND  TO_NUMBER(SUBSTR(g.PERCENTAGE_RANGE,
                               INSTR(g.PERCENTAGE_RANGE, '-') + 1)) >= m.MARKS
      ORDER  BY s.STUDENTS_STU_ID
    `;

    const result = await oracleConn.execute(oracleQuery);
    console.log(`🔹 Pulled ${result.rows.length} rows from Oracle`);

    // 3. Transform → build a map keyed by student, aggregating GPA
    const studentMap = new Map();          // id → { details…, totalGpa, count }

    for (const row of result.rows) {
      const [
        id,
        firstName,
        lastName,
        email,
        joiningDate,
        deptName,
        _marks,
        gpaEquiv
      ] = row;

      if (!studentMap.has(id)) {
        studentMap.set(id, {
          firstName,
          lastName,
          email,
          joiningDate,
          deptName,
          totalGpa: 0,
          count:    0
        });
      }
      const rec = studentMap.get(id);
      rec.totalGpa += parseFloat(gpaEquiv);
      rec.count    += 1;
    }

    // 4. Upsert each record into PostgreSQL
    const upsertSql = `
      INSERT INTO student_academics (
        student_academics_student_id,
        student_academics_first_name,
        student_academics_last_name,
        student_academics_email,
        student_academics_department,
        student_academics_joining_date,
        student_academics_gpa
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (student_academics_student_id) DO UPDATE
      SET
        student_academics_first_name   = EXCLUDED.student_academics_first_name,
        student_academics_last_name    = EXCLUDED.student_academics_last_name,
        student_academics_email        = EXCLUDED.student_academics_email,
        student_academics_department   = EXCLUDED.student_academics_department,
        student_academics_joining_date = EXCLUDED.student_academics_joining_date,
        student_academics_gpa          = EXCLUDED.student_academics_gpa;
    `;

    for (const [id, data] of studentMap) {
      const avgGpa = (data.totalGpa / data.count).toFixed(2);

      await pgClient.query(upsertSql, [
        id,
        data.firstName,
        data.lastName,
        data.email,
        data.deptName,
        data.joiningDate,     // a JS Date maps to Postgres DATE/TIMESTAMP
        avgGpa
      ]);
    }

    console.log(`✅ Upserted ${studentMap.size} students into PostgreSQL`);
  } catch (err) {
    console.error('❌ Error during ETL:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();
