const oracledb = require('oracledb');
const { Client } = require('pg');

oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

const oracleConfig = {
  user: 'system',
  password: 'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

const pgClient = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'ETL',
  password: 'Moodle@123',
  port: 5432
});

async function transferData() {
  let oracleConn;

  try {
    await pgClient.connect();
    console.log("✅ Connected to PostgreSQL");

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log("✅ Connected to Oracle");

    const result = await oracleConn.execute(`
      SELECT 
        s.STUDENTS_STU_ID,
        s.STUDENTS_FIRST_NAME,
        s.STUDENTS_LAST_NAME,
        s.STUDENTS_EMAIL,
        s.STUDENTS_JOINING_DATE,
        d.DEPARTMENTS_DEPT_NAME,
        m.MARKS,
        g.GPA_EQUIVALENT
      FROM STUDENTS s
      JOIN DEPARTMENTS d ON s.STUDENTS_DEPT_ID = d.DEPARTMENTS_DEPT_ID
      JOIN MARKS m ON s.STUDENTS_STU_ID = m.STUDENT_ID
      JOIN SUBJECTS sub ON m.SUBJECT_ID = sub.SUBJECT_ID
      JOIN GRADE g ON
        TO_NUMBER(SUBSTR(g.PERCENTAGE_RANGE, 1, INSTR(g.PERCENTAGE_RANGE, '-') - 1)) <= m.MARKS AND
        TO_NUMBER(SUBSTR(g.PERCENTAGE_RANGE, INSTR(g.PERCENTAGE_RANGE, '-') + 1)) >= m.MARKS
      ORDER BY s.STUDENTS_STU_ID
    `);

    const studentMap = new Map();

    for (let row of result.rows) {
      const [id, first, last, email, joinDate, dept, marks, gpa] = row;

      if (!studentMap.has(id)) {
        studentMap.set(id, {
          first,
          last,
          email,
          joinDate,
          dept,
          totalGpa: 0,
          count: 0
        });
      }

      const record = studentMap.get(id);
      record.totalGpa += parseFloat(gpa);
      record.count += 1;
    }

    for (let [id, data] of studentMap) {
      const avgGpa = (data.totalGpa / data.count).toFixed(2);

      await pgClient.query(
        `INSERT INTO student_academics (
           student_academics_student_id,
           student_academics_first_name,
           student_academics_last_name,
           student_academics _email,
           student_academics _joining_date,
           student_academics _department,
           student_academics_ gpa
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (student_academics_student_id) DO UPDATE
         SET student_academics_ gpa = EXCLUDED.student_academics_ gpa`,
        [id, data.first, data.last, data.email, data.joinDate, data.dept, avgGpa]
      );
    }

    console.log("✅ Data transferred successfully");

  } catch (err) {
    console.error("❌ Error during ETL:", err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();
