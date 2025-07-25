// const oracledb = require('oracledb');
// const { Client } = require('pg');

// // Oracle DB config
// const oracleConfig = {
//   user: 'system',
//   password: 'Moodle@123',
//   connectString: 'localhost:1522/oracle'
// };

// // Postgres DB config
// const pgConfig = {
//   user: 'postgres',
//   host: 'localhost',
//   database: 'ETL',
//   password: 'Moodle@123',
//   port: 5432,
// };

// async function transferData() {
//   let oracleConn;
//   const pgClient = new Client(pgConfig);

//   try {
//     await pgClient.connect();
//     console.log('✅ Connected to PostgreSQL');

//     oracleConn = await oracledb.getConnection(oracleConfig);
//     console.log('✅ Connected to Oracle');

//     // Query students joined with departments, marks and grades to get GPA
//     const result = await oracleConn.execute(`
//       SELECT
//         s.STUDENTS_STU_ID,
//         s.STUDENTS_FIRST_NAME,
//         s.STUDENTS_LAST_NAME,
//         s.STUDENTS_EMAIL,
//         d.DEPARTMENTS_DEPT_NAME,
//         s.STUDENTS_JOINING_DATE,
//         AVG(g.GPA_EQUIVALENT) AS AVG_GPA
//       FROM
//         STUDENTS s
//         LEFT JOIN DEPARTMENTS d ON s.STUDENTS_DEPT_ID = d.DEPARTMENTS_DEPT_ID
//         LEFT JOIN MARKS m ON m.MARKS_STUDENT_ID = s.STUDENTS_STU_ID
//         LEFT JOIN GRADE g ON
//           CASE
//             WHEN m.MARKS BETWEEN 90 AND 100 THEN 'A'
//             WHEN m.MARKS BETWEEN 80 AND 89 THEN 'B'
//             WHEN m.MARKS BETWEEN 70 AND 79 THEN 'C'
//             WHEN m.MARKS BETWEEN 60 AND 69 THEN 'D'
//             ELSE 'F'
//           END = g.GRADE_CODE
//       GROUP BY
//         s.STUDENTS_STU_ID,
//         s.STUDENTS_FIRST_NAME,
//         s.STUDENTS_LAST_NAME,
//         s.STUDENTS_EMAIL,
//         d.DEPARTMENTS_DEPT_NAME,
//         s.STUDENTS_JOINING_DATE
//       ORDER BY s.STUDENTS_STU_ID
//     `);

//     for (const row of result.rows) {
//       const [
//         studentId,
//         firstName,
//         lastName,
//         email,
//         department,
//         joiningDate,
//         avgGpa
//       ] = row;

//       // Insert into Postgres student_academics table
//       await pgClient.query(
//         `INSERT INTO student_academics (
//           Student_academics_student_id,
//           student_academics_first_name,
//           student_academics_last_name,
//           student_academics_email,
//           student_academics_department,
//           student_academics_joining_date,
//           student_academics_gpa
//         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
//         ON CONFLICT (Student_academics_student_id) DO UPDATE SET
//           student_academics_first_name = EXCLUDED.student_academics_first_name,
//           student_academics_last_name = EXCLUDED.student_academics_last_name,
//           student_academics_email = EXCLUDED.student_academics_email,
//           student_academics_department = EXCLUDED.student_academics_department,
//           student_academics_joining_date = EXCLUDED.student_academics_joining_date,
//           student_academics_gpa = EXCLUDED.student_academics_gpa`,
//         [
//           studentId,
//           firstName,
//           lastName,
//           email,
//           department,
//           joiningDate,
//           avgGpa ? parseFloat(avgGpa.toFixed(2)) : null,
//         ]
//       );
//     }

//     console.log(`✅ Transferred ${result.rows.length} records from Oracle to PostgreSQL.`);

//   } catch (err) {
//     console.error('❌ Error during ETL:', err);
//   } finally {
//     if (oracleConn) await oracleConn.close();
//     await pgClient.end();
//   }
// }

// transferData();








/**
 * oracle_to_postgres.js  –  FINAL
 * ----------------------------------------
 * Reads data from Oracle, transforms, and loads
 * into PostgreSQL table:  student_academics
 */

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
async function transferData () {
  let oracleConn;
  const pgClient = new Client(pgConfig);

  try {
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    /* Uncomment once if you want the script to create the target table automatically
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
    */

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
                  WHEN m.MARKS BETWEEN 90 AND 100 THEN 'A'
                  WHEN m.MARKS BETWEEN 80 AND  89 THEN 'B'
                  WHEN m.MARKS BETWEEN 70 AND  79 THEN 'C'
                  WHEN m.MARKS BETWEEN 60 AND  69 THEN 'D'
                  ELSE 'F'
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

      await pgClient.query(upsertSQL, [
        id,
        first,
        last,
        email,
        dept,
        joinDate,
        gpa ?? 0               
      ]);

      inserted++;
    }

    console.log(`✅ Upserted ${inserted} records into PostgreSQL`);

  } catch (err) {
    console.error('❌ Error during ETL:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();

