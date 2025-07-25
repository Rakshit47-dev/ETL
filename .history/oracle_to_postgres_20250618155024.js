const oracledb = require('oracledb');
const { Client } = require('pg');

// Oracle DB config
const oracleConfig = {
  user: 'system',
  password: 'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

// Postgres DB config
const pgConfig = {
  user: 'postgres',
  host: 'localhost',
  database: 'ETL',
  password: '',
  port: 5432,
};

async function transferData() {
  let oracleConn;
  const pgClient = new Client(pgConfig);

  try {
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log('✅ Connected to Oracle');

    // Query students joined with departments, marks and grades to get GPA
    const result = await oracleConn.execute(`
      SELECT
        s.STUDENTS_STU_ID,
        s.STUDENTS_FIRST_NAME,
        s.STUDENTS_LAST_NAME,
        s.STUDENTS_EMAIL,
        d.DEPARTMENTS_DEPT_NAME,
        s.STUDENTS_JOINING_DATE,
        AVG(g.GPA_EQUIVALENT) AS AVG_GPA
      FROM
        STUDENTS s
        LEFT JOIN DEPARTMENTS d ON s.STUDENTS_DEPT_ID = d.DEPARTMENTS_DEPT_ID
        LEFT JOIN MARKS m ON m.MARKS_STUDENT_ID = s.STUDENTS_STU_ID
        LEFT JOIN GRADE g ON
          CASE
            WHEN m.MARKS BETWEEN 90 AND 100 THEN 'A'
            WHEN m.MARKS BETWEEN 80 AND 89 THEN 'B'
            WHEN m.MARKS BETWEEN 70 AND 79 THEN 'C'
            WHEN m.MARKS BETWEEN 60 AND 69 THEN 'D'
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
    `);

    for (const row of result.rows) {
      const [
        studentId,
        firstName,
        lastName,
        email,
        department,
        joiningDate,
        avgGpa
      ] = row;

      // Insert into Postgres student_academics table
      await pgClient.query(
        `INSERT INTO student_academics (
          Student_academics_student_id,
          student_academics_first_name,
          student_academics_last_name,
          student_academics_email,
          student_academics_department,
          student_academics_joining_date,
          student_academics_ gpa
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (Student_academics_student_id) DO UPDATE SET
          student_academics_first_name = EXCLUDED.student_academics_first_name,
          student_academics_last_name = EXCLUDED.student_academics_last_name,
          student_academics_email = EXCLUDED.student_academics_email,
          student_academics_department = EXCLUDED.student_academics_department,
          student_academics_joining_date = EXCLUDED.student_academics_joining_date,
          student_academics_ gpa = EXCLUDED.student_academics_ gpa`,
        [
          studentId,
          firstName,
          lastName,
          email,
          department,
          joiningDate,
          avgGpa ? parseFloat(avgGpa.toFixed(2)) : null,
        ]
      );
    }

    console.log(`✅ Transferred ${result.rows.length} records from Oracle to PostgreSQL.`);

  } catch (err) {
    console.error('❌ Error during ETL:', err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();
