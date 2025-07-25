// load_to_oracle.js
const oracledb = require('oracledb');
const fs = require('fs');
const readline = require('readline');
const csv = require('csv-parse/lib/sync');

async function loadData() {
  const conn = await oracledb.getConnection({ user: 'user', password: 'pass', connectionString: 'localhost/orclpdb1' });

  const studentsRaw = fs.readFileSync('students.txt', 'utf-8');
  const gradesRaw = fs.readFileSync('grade.txt', 'utf-8');

  const students = csv(studentsRaw, { columns: true, skip_empty_lines: true });
  const grades = csv(gradesRaw, { columns: true, skip_empty_lines: true });

  const departmentsMap = new Map();
  const subjectsMap = new Map();

  // Insert grades
  for (const grade of grades) {
    await conn.execute(`
      INSERT INTO GRADE (grade_id, grade_code, grade_label, percentage_range, gpa_equivalent)
      VALUES (:id, :code, :label, :range, :gpa)
    `, {
      id: grade.Garde_id,
      code: grade.grade_code,
      label: grade.grade_label,
      range: grade.percentage_range,
      gpa: grade.gpa_equivalent
    });
  }

  for (const student of students) {
    const deptName = student.department.trim();

    // Insert department
    if (!departmentsMap.has(deptName)) {
      const result = await conn.execute(`INSERT INTO DEPARTMENTS (department_name) VALUES (:name) RETURNING dept_id INTO :id`, {
        name: deptName,
        id: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
      });
      departmentsMap.set(deptName, result.outBinds.id[0]);
    }

    const deptId = departmentsMap.get(deptName);

    // Insert student
    await conn.execute(`
      INSERT INTO STUDENTS (student_id, first_name, last_name, email, joining_date, dept_id)
      VALUES (:id, :first, :last, :email, TO_DATE(:date, 'YYYY-MM-DD'), :deptId)
    `, {
      id: student.student_id,
      first: student.first_name.trim(),
      last: student.last_name.trim(),
      email: student.email.trim(),
      date: student.joining_date,
      deptId
    });

    // Insert subjects and marks
    for (let i = 1; i <= 5; i++) {
      const subject = student[`subject${i}`]?.trim();
      const marks = student[`subject${i}_marks`] || student[`subject${i}_mark`];

      if (!subject || isNaN(marks)) continue;

      let subjectId;
      if (!subjectsMap.has(subject)) {
        const result = await conn.execute(`
          INSERT INTO SUBJECTS (subject_name, dept_id) VALUES (:name, :deptId)
          RETURNING subject_id INTO :id
        `, {
          name: subject,
          deptId,
          id: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
        });
        subjectId = result.outBinds.id[0];
        subjectsMap.set(subject, subjectId);
      } else {
        subjectId = subjectsMap.get(subject);
      }

      await conn.execute(`
        INSERT INTO MARKS (student_id, subject_id, marks) VALUES (:sid, :subid, :marks)
      `, {
        sid: student.student_id,
        subid: subjectId,
        marks: +marks
      });
    }
  }

  await conn.commit();
  await conn.close();
}

loadData();
