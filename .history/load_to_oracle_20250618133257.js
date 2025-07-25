// // load_to_oracle.js
// const fs = require('fs');
// const oracledb = require('oracledb');
// const path = require('path');

// // Set Oracle client config
// // oracledb.initOracleClient({ libDir: 'C:\\oracle\\instantclient_19_12' }); // Update path to your Oracle Instant Client

// // (async () => {
// //   try {
// //     const conn = await oracledb.getConnection({
// //       user: 'system',
// //       password: 'Moodle@123',
// //       connectString: 'localhost:1522/oracle'
// //     });
// //     console.log('✅ Connected!');
// //     await conn.close();
// //   } catch (e) {
// //     console.error(e);
// //   }
// // })();

// async function run() {
//   let connection;

//   try {
//     connection = await oracledb.getConnection(dbConfig);
//     console.log("✅ Connected to Oracle");

//     // 1. Load and insert GRADE data
//     const gradeData = fs.readFileSync('grade.txt', 'utf-8').split('\n').slice(1);
//     for (let line of gradeData) {
//       if (!line.trim()) continue;
//       const [id, code, label, range, gpa] = line.split(',').map(s => s.trim());
//       await connection.execute(
//         `INSERT INTO grade (grade_id, grade_code, grade_label, percentage_range, gpa_equivalent)
//          VALUES (:1, :2, :3, :4, :5)`,
//         [id, code, label, range, gpa]
//       );
//     }

//     // 2. Load student data
//     const studentsRaw = fs.readFileSync('students.txt', 'utf-8').split('\n').slice(1);
//     const departmentsSet = new Set();
//     const subjectsSet = new Set();
//     const departmentMap = new Map();
//     const subjectMap = new Map();

//     // Extract unique departments and subjects first
//     for (let line of studentsRaw) {
//       const parts = line.split(',').map(p => p.trim());
//       if (parts.length < 16) continue;

//       const department = parts[4];
//       departmentsSet.add(department);

//       const subjects = parts.slice(6, 11);
//       subjects.forEach(sub => subjectsSet.add(`${department}::${sub}`));
//     }

//     // Insert departments
//     for (let dept of departmentsSet) {
//       const result = await connection.execute(
//         `INSERT INTO departments (dept_name) VALUES (:1) RETURNING dept_id INTO :2`,
//         {
//           1: dept,
//           2: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
//         }
//       );
//       departmentMap.set(dept, result.outBinds[2][0]);
//     }

//     // Insert subjects
//     for (let entry of subjectsSet) {
//       const [dept, subject] = entry.split('::');
//       const result = await connection.execute(
//         `INSERT INTO subjects (subject_name, dept_id) VALUES (:1, :2) RETURNING subject_id INTO :3`,
//         {
//           1: subject,
//           2: departmentMap.get(dept),
//           3: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
//         }
//       );
//       subjectMap.set(`${dept}::${subject}`, result.outBinds[3][0]);
//     }

//     // Insert students and marks
//     for (let line of studentsRaw) {
//       const parts = line.split(',').map(p => p.trim());
//       if (parts.length < 16) continue;

//       const [id, first, last, email, dept, joinDate, ...rest] = parts;
//       const subjects = rest.slice(0, 5);
//       const marks = rest.slice(5, 10);

//       await connection.execute(
//         `INSERT INTO students (student_id, first_name, last_name, email, dept_id, joining_date)
//          VALUES (:1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD'))`,
//         [id, first, last, email, departmentMap.get(dept), joinDate]
//       );

//       for (let i = 0; i < 5; i++) {
//         const subjectKey = `${dept}::${subjects[i]}`;
//         const subjectId = subjectMap.get(subjectKey);
//         const mark = parseInt(marks[i]);

//         if (!subjectId || isNaN(mark)) continue;

//         await connection.execute(
//           `INSERT INTO marks (student_id, subject_id, marks_obtained)
//            VALUES (:1, :2, :3)`,
//           [id, subjectId, mark]
//         );
//       }
//     }

//     await connection.commit();
//     console.log("✅ Data inserted successfully into Oracle database.");

//   } catch (err) {
//     console.error("❌ Error:", err);
//   } finally {
//     if (connection) await connection.close();
//   }
// }

// run();









/******************************************************************
 *  load_to_oracle.js
 *  Reads students.txt  &  grade.txt  and loads them into Oracle  *
 ******************************************************************/
const fs       = require('fs');
const oracledb = require('oracledb');

// ---------------------------------------------------------------
// 1.  Tell node‑oracledb where oci.dll lives  (ONLY ONCE!)
// ---------------------------------------------------------------
oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

// ---------------------------------------------------------------
// 2.  Connection details
// ---------------------------------------------------------------
const dbConfig = {
  user          : 'system',        // or your app user
  password      : 'Moodle@123',    // ← change if you use another user
  connectString : 'localhost:1522/oracle'
};

// ---------------------------------------------------------------
// 3.  Helper: read CSV, drop empty last line, skip header
// ---------------------------------------------------------------
function readLines(file) {
  return fs.readFileSync(file, 'utf-8')
           .split('\n')
           .slice(1)               // skip header
           .filter(l => l.trim()); // drop blank lines
}

// ---------------------------------------------------------------
// 4.  Main ETL
// ---------------------------------------------------------------
async function run() {
  let conn;
  try {
    conn = await oracledb.getConnection(dbConfig);
    console.log('✅ Connected to Oracle');

    /*-----------------------------------------------------------
      4‑A.  Load  GRADE
    -----------------------------------------------------------*/
    const gradeRows = readLines('grade.txt');
    for (const row of gradeRows) {
      const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
      await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(grade, grade_id) */
         INTO grade (grade_id, grade_code, grade_label, percentage_range, gpa_equivalent)
         VALUES (:id, :code, :label, :range, :gpa)`,
        { id, code, label, range, gpa }
      );
    }
    console.log(`→ Inserted ${gradeRows.length} rows into GRADE`);

    /*-----------------------------------------------------------
      4‑B.  Read students.txt  – first gather depts & subjects
    -----------------------------------------------------------*/
    const studentRows   = readLines('students.txt');
    const deptSet       = new Set();
    const subjSet       = new Set();       // key = `${dept}::${subject}`
    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;      // bad row
      const dept      = cols[4];
      const subjects  = cols.slice(6, 11);
      deptSet.add(dept);
      subjects.forEach(s => subjSet.add(`${dept}::${s}`));
    }

    /*-----------------------------------------------------------
      4‑C.  Insert departments
    -----------------------------------------------------------*/
    const deptMap = new Map();             // dept → dept_id
    for (const dept of deptSet) {
      const result = await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(departments, dept_name) */
         INTO departments (dept_name)
         VALUES (:dept)
         RETURNING dept_id INTO :id`,
        {
          dept,
          id: { dir : oracledb.BIND_OUT, type : oracledb.NUMBER }
        }
      );
      // If row already existed, fetch id:
      const deptId = result.outBinds.id?.[0] ||
                     (await conn.execute(
                       `SELECT dept_id FROM departments WHERE dept_name = :d`, [dept]
                     )).rows[0][0];
      deptMap.set(dept, deptId);
    }

    /*-----------------------------------------------------------
      4‑D.  Insert subjects
    -----------------------------------------------------------*/
    const subjMap = new Map();             // `${dept}::${subject}` → subject_id
    for (const key of subjSet) {
      const [dept, subject] = key.split('::');
      const result = await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(subjects, subject_name) */
         INTO subjects (subject_name, dept_id)
         VALUES (:subject, :dept_id)
         RETURNING subject_id INTO :id`,
        {
          subject,
          dept_id : deptMap.get(dept),
          id      : { dir : oracledb.BIND_OUT, type : oracledb.NUMBER }
        }
      );
      const subId = result.outBinds.id?.[0] ||
                    (await conn.execute(
                      `SELECT subject_id FROM subjects
                       WHERE subject_name = :s AND dept_id = :d`,
                      [subject, deptMap.get(dept)]
                    )).rows[0][0];
      subjMap.set(key, subId);
    }

    /*-----------------------------------------------------------
      4‑E.  Insert students & marks
    -----------------------------------------------------------*/
    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;

      const  [id, first, last, email, dept, joinDate, ...rest] = cols;
      const subjects = rest.slice(0,5);
      const marks    = rest.slice(5,10);

      await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(students, student_id) */
         INTO students (student_id, first_name, last_name, email, dept_id, joining_date)
         VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd,'YYYY-MM-DD'))`,
        { id, first, last, email, dept_id : deptMap.get(dept), jd : joinDate }
      );

      for (let i = 0; i < 5; i++) {
        const key   = `${dept}::${subjects[i]}`;
        const subId = subjMap.get(key);
        const mark  = parseInt(marks[i]);
        if (!subId || isNaN(mark)) continue;

        await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(marks, student_id_subject_id_pk) */
           INTO marks (student_id, subject_id, marks)
           VALUES (:sid, :subId, :mark)`,
          { sid : id, subId, mark }
        );
      }
    }

    await conn.commit();
    console.log('✅ All data loaded into Oracle.');

  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    if (conn) await conn.close();
  }
}

run();

