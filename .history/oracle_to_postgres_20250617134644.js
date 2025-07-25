// oracle_to_postgres.js
const oracledb = require('oracledb');
const { Client } = require('pg');

async function transferData() {
  const ora = await oracledb.getConnection({ user: 'user', password: 'pass', connectionString: 'localhost/orclpdb1' });
  const pg = new Client({ user: 'postgres', password: 'pass', database: 'dw', host: 'localhost', port: 5432 });
  await pg.connect();

  const gradeRows = await ora.execute(`SELECT * FROM GRADE`);
  const gradeMap = {};
  for (const row of gradeRows.rows) {
    const [id, code, label, range, gpa] = row;
    const [min, max] = range.split('-').map(Number);
    for (let i = min; i <= max; i++) {
      gradeMap[i] = gpa;
    }
  }

  const result = await ora.execute(`
    SELECT S.student_id, S.first_name, S.last_name, S.email, D.department_name, S.joining_date,
           AVG(M.marks) AS avg_marks
    FROM STUDENTS S
    JOIN DEPARTMENTS D ON S.dept_id = D.dept_id
    JOIN MARKS M ON S.student_id = M.student_id
    GROUP BY S.student_id, S.first_name, S.last_name, S.email, D.department_name, S.joining_date
  `);

  for (const row of result.rows) {
    const [id, first, last, email, dept, date, avg] = row;
    const gpa = gradeMap[Math.round(avg)] || 0;

    await pg.query(`
      INSERT INTO student_academics (student_id, first_name, last_name, email, department, joining_date, gpa)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, [id, first, last, email, dept, date, gpa]);
  }

  await pg.end();
  await ora.close();
}

transferData();
