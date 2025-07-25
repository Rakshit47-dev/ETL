// load_to_oracle.js
const fs = require('fs');
const oracledb = require('oracledb');
const path = require('path');

// Set Oracle client config
oracledb.initOracleClient({ libDir: 'C:\\oracle\\instantclient_19_12' }); // Update path to your Oracle Instant Client

const dbConfig = {
  user: 'system',
  password: 'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

async function run() {
  let connection;

  try {
    connection = await oracledb.getConnection(dbConfig);
    console.log("✅ Connected to Oracle");

    // 1. Load and insert GRADE data
    const gradeData = fs.readFileSync('grade.txt', 'utf-8').split('\n').slice(1);
    for (let line of gradeData) {
      if (!line.trim()) continue;
      const [id, code, label, range, gpa] = line.split(',').map(s => s.trim());
      await connection.execute(
        `INSERT INTO grade (grade_id, grade_code, grade_label, percentage_range, gpa_equivalent)
         VALUES (:1, :2, :3, :4, :5)`,
        [id, code, label, range, gpa]
      );
    }

    // 2. Load student data
    const studentsRaw = fs.readFileSync('students.txt', 'utf-8').split('\n').slice(1);
    const departmentsSet = new Set();
    const subjectsSet = new Set();
    const departmentMap = new Map();
    const subjectMap = new Map();

    // Extract unique departments and subjects first
    for (let line of studentsRaw) {
      const parts = line.split(',').map(p => p.trim());
      if (parts.length < 16) continue;

      const department = parts[4];
      departmentsSet.add(department);

      const subjects = parts.slice(6, 11);
      subjects.forEach(sub => subjectsSet.add(`${department}::${sub}`));
    }

    // Insert departments
    for (let dept of departmentsSet) {
      const result = await connection.execute(
        `INSERT INTO departments (dept_name) VALUES (:1) RETURNING dept_id INTO :2`,
        {
          1: dept,
          2: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
        }
      );
      departmentMap.set(dept, result.outBinds[2][0]);
    }

    // Insert subjects
    for (let entry of subjectsSet) {
      const [dept, subject] = entry.split('::');
      const result = await connection.execute(
        `INSERT INTO subjects (subject_name, dept_id) VALUES (:1, :2) RETURNING subject_id INTO :3`,
        {
          1: subject,
          2: departmentMap.get(dept),
          3: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
        }
      );
      subjectMap.set(`${dept}::${subject}`, result.outBinds[3][0]);
    }

    // Insert students and marks
    for (let line of studentsRaw) {
      const parts = line.split(',').map(p => p.trim());
      if (parts.length < 16) continue;

      const [id, first, last, email, dept, joinDate, ...rest] = parts;
      const subjects = rest.slice(0, 5);
      const marks = rest.slice(5, 10);

      await connection.execute(
        `INSERT INTO students (student_id, first_name, last_name, email, dept_id, joining_date)
         VALUES (:1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD'))`,
        [id, first, last, email, departmentMap.get(dept), joinDate]
      );

      for (let i = 0; i < 5; i++) {
        const subjectKey = `${dept}::${subjects[i]}`;
        const subjectId = subjectMap.get(subjectKey);
        const mark = parseInt(marks[i]);

        if (!subjectId || isNaN(mark)) continue;

        await connection.execute(
          `INSERT INTO marks (student_id, subject_id, marks_obtained)
           VALUES (:1, :2, :3)`,
          [id, subjectId, mark]
        );
      }
    }

    await connection.commit();
    console.log("✅ Data inserted successfully into Oracle database.");

  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    if (connection) await connection.close();
  }
}

run();
