const oracledb = require('oracledb');
const { Client } = require('pg');

// Oracle client setup
oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

// Oracle connection config
const oracleConfig = {
  user: 'system',
  password: 'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

// PostgreSQL connection config
const pgClient = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'ETL',
  password: '',
  port: 5432
});

async function transferData() {
  let oracleConn;

  try {
    await pgClient.connect();
    console.log("✅ Connected to PostgreSQL");

    oracleConn = await oracledb.getConnection(oracleConfig);
    console.log("✅ Connected to Oracle");

    // Fetch joined data: student, department, marks, subject, grade
    const result = await oracleConn.execute(`
      SELECT s.student_id,
             s.first_name || ' ' || s.last_name AS full_name,
             d.dept_name,
             m.marks_obtained,
             g.gpa_equivalent
      FROM students s
      JOIN departments d ON s.dept_id = d.dept_id
      JOIN marks m ON s.student_id = m.student_id
      JOIN subjects sub ON m.subject_id = sub.subject_id
      JOIN grade g ON
        TO_NUMBER(SUBSTR(g.percentage_range, 1, INSTR(g.percentage_range, '-') - 1)) <= m.marks_obtained AND
        TO_NUMBER(SUBSTR(g.percentage_range, INSTR(g.percentage_range, '-') + 1)) >= m.marks_obtained
      ORDER BY s.student_id
    `);

    // Transform data: group by student, calculate average GPA
    const studentMap = new Map();

    for (let row of result.rows) {
      const [id, name, dept, marks, gpa] = row;

      if (!studentMap.has(id)) {
        studentMap.set(id, { name, dept, totalGpa: 0, count: 0 });
      }

      const record = studentMap.get(id);
      record.totalGpa += parseFloat(gpa);
      record.count += 1;
    }

    // Insert transformed data into PostgreSQL
    for (let [id, data] of studentMap) {
      const avgGpa = (data.totalGpa / data.count).toFixed(2);

      await pgClient.query(
        `INSERT INTO student_academics (student_id, full_name, dept_name, avg_gpa)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (student_id) DO UPDATE
         SET avg_gpa = EXCLUDED.avg_gpa`,
        [id, data.name, data.dept, avgGpa]
      );
    }

    console.log("✅ Transformed data inserted into PostgreSQL");

  } catch (err) {
    console.error("❌ Error during ETL:", err);
  } finally {
    if (oracleConn) await oracleConn.close();
    await pgClient.end();
  }
}

transferData();
