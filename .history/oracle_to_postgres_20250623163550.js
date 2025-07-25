require('./logger');
const oracledb = require('oracledb');
const { Client } = require('pg');

/* ---------- CONFIG ---------- */
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

/* ---------- MAIN ------------ */
console.log('------------ORACLE_TO_POSTGRES------------')
async function transferData () {
  let oracleConn;
  const pgClient = new Client(pgConfig);

  try {
    await pgClient.connect();
    console.log('⏳ Trying to Connect To  database ⏳')
    console.log('✅ Connected to PostgreSQL');

  
    await pgClient.query(`
      CREATE TABLE IF NOT EXISTS student_academics (
        student_academics_student_id  INTEGER PRIMARY KEY,
        student_academics_first_name  VARCHAR(100),
        student_academics_last_name   VARCHAR(100),
        student_academics_email       VARCHAR(100),
        student_academics_department  VARCHAR(100),
        student_academics_joining_date DATE,
        student_academics_gpa         NUMERIC(3,2)
      );
    `);
    

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    /* ----- 1. Pull & transform in SQL ----- */
    const oracleSQL = `
      SELECT
        s.STUDENTS_STU_ID,
        s.STUDENTS_FIRST_NAME,
        s.STUDENTS_LAST_NAME,
        s.STUDENTS_EMAIL,
        COALESCE(d.DEPARTMENTS_DEPT_NAME, 'Unknown')          AS DEPT_NAME,
        s.STUDENTS_JOINING_DATE,
        /* Average GPA (defaults to 0 when no grade match) */
        ROUND( AVG( COALESCE(g.GPA_EQUIVALENT, 0) ), 2 )      AS AVG_GPA
      FROM   STUDENTS s
      LEFT JOIN DEPARTMENTS d
             ON d.DEPARTMENTS_DEPT_ID = s.STUDENTS_DEPT_ID
      LEFT JOIN MARKS m
             ON m.MARKS_STUDENT_ID    = s.STUDENTS_STU_ID
      /* Map mark -> grade code -> GPA */
      LEFT JOIN GRADE g
               ON CASE
            WHEN m.MARKS BETWEEN 91 AND 100 THEN 'A1'  -- Outstanding
            WHEN m.MARKS BETWEEN 81 AND  90 THEN 'A2'  -- Excellent
            WHEN m.MARKS BETWEEN 71 AND  80 THEN 'B1'  -- Very Good
            WHEN m.MARKS BETWEEN 61 AND  70 THEN 'B2'  -- Good
            WHEN m.MARKS BETWEEN 51 AND  60 THEN 'C1'  -- Above Average
            WHEN m.MARKS BETWEEN 41 AND  50 THEN 'C2'  -- Average
            WHEN m.MARKS BETWEEN 33 AND  40 THEN 'D'   -- Pass
            WHEN m.MARKS BETWEEN 21 AND  32 THEN 'E1'  -- Needs Improvement
            ELSE 'E2'                                   -- Fail (0-20)
                END = g.GRADE_CODE
      GROUP BY
        s.STUDENTS_STU_ID,
        s.STUDENTS_FIRST_NAME,
        s.STUDENTS_LAST_NAME,
        s.STUDENTS_EMAIL,
        d.DEPARTMENTS_DEPT_NAME,
        s.STUDENTS_JOINING_DATE
      ORDER BY s.STUDENTS_STU_ID
    `;

    const { rows } = await oracleConn.execute(oracleSQL);
    console.log(`🔹 Pulled ${rows.length} students from Oracle`);

    /* ----- 2. Prepare Postgres upsert ----- */
    const upsertSQL = `
      INSERT INTO student_academics (
        student_academics_student_id,
        student_academics_first_name,
        student_academics_last_name,
        student_academics_email,
        student_academics_department,
        student_academics_joining_date,
        student_academics_gpa
      ) VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (student_academics_student_id) DO UPDATE
      SET
        student_academics_first_name   = EXCLUDED.student_academics_first_name,
        student_academics_last_name    = EXCLUDED.student_academics_last_name,
        student_academics_email        = EXCLUDED.student_academics_email,
        student_academics_department   = EXCLUDED.student_academics_department,
        student_academics_joining_date = EXCLUDED.student_academics_joining_date,
        student_academics_gpa          = EXCLUDED.student_academics_gpa
    `;

    /* ----- 3. Loop & insert ----- */
    let inserted = 0;
    for (const [
      id,
      first,
      last,
      email,
      dept,
      joinDate,
      gpa 
    ] of rows) {

      /* Log if important fields are still null */
      if (!email) console.warn(`⚠️  Student ${id} has NULL email`);
      if (!dept)  console.warn(`⚠️  Student ${id} has NULL department`);
       if (!gpa)  console.warn(`⚠️  Student ${id} has NULL gpa`);
      
      
      await pgClient.query(upsertSQL, [
        id,
        first,
        last,
        email,
        dept,
        joinDate,
        gpa                
      ]);

      inserted++;
    }

    console.log(`✅ Upserted ${inserted} records into PostgreSQL`);
    console.log('✅ ETL from Oracle to PostgreSQL completed')

  } catch (err) {
    console.error('❌ Error during ETL:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();

