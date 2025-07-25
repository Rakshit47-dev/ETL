// require('./logger');
// const fs = require('fs');
// const oracledb = require('oracledb');

// oracledb.initOracleClient({
//   libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
// });

// const dbConfig = {
//   user: 'system',
//   password: 'Moodle@123',
//   connectString: 'localhost:1522/oracle'
// };


// function safeReadLines(file) {
//   try {
//     const lines = fs.readFileSync(file, 'utf-8')
//       .split('\n')
//       .slice(1)                 
//       .filter(l => l.trim());
//     console.log(`✅ ${file} loaded successfully – data extracted (${lines.length} rows)`);
//     return lines;
//   } catch (err) {
//     console.error(`❌ Error reading ${file}:`, err.message);
//     process.exit(1);         
//   }
// }


// async function run() {
//   let conn;
//   try {
//     conn = await oracledb.getConnection(dbConfig);
//     console.log('✅ Connected to Oracle');

//     const gradeRows   = safeReadLines('grade.txt'); 
//     for (const row of gradeRows) {
//       const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
//       await conn.execute(
//         `BEGIN
//            INSERT INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
//            VALUES (:id, :code, :label, :range, :gpa);
//          EXCEPTION
//           WHEN DUP_VAL_ON_INDEX THEN NULL;
//           WHEN NO_DATA_FOUND THEN  NULL;
//          END;`,
//         { id, code, label, range, gpa }
//       );
//     }
//     console.log(`→ Inserted ${gradeRows.length} rows into GRADE`);

//     const studentRows = safeReadLines('students.txt'); 
//     const deptSet = new Set();
//     const subjSet = new Set();

//     for (const row of studentRows) {
//       const cols = row.split(',').map(x => x.trim());
//       if (cols.length < 16) continue;
//       const dept = cols[4];
//       const subjects = cols.slice(6, 11);
//       deptSet.add(dept);
//       subjects.forEach(s => subjSet.add(`${dept}::${s}`));
//     }

//     const deptMap = new Map();
//     for (const dept of deptSet) {
//       let result;
//       try {
//         result = await conn.execute(
//           `INSERT INTO DEPARTMENTS (DEPARTMENTS_DEPT_NAME)
//            VALUES (:dept)
//            RETURNING DEPARTMENTS_DEPT_ID INTO :id`,
//           {
//             dept,
//             id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
//           }
//         );
//       } catch (e) {
//         result = await conn.execute(
//           `SELECT DEPARTMENTS_DEPT_ID AS ID FROM DEPARTMENTS WHERE DEPARTMENTS_DEPT_NAME = :dept`,
//           { dept }
//         );
//       }
//       const deptId = result.outBinds?.id?.[0] || result.rows?.[0]?.ID;
//       deptMap.set(dept, deptId);
//     }

//     const subjMap = new Map();
//     for (const key of subjSet) {
//       const [dept, subject] = key.split('::');
//       const deptId = deptMap.get(dept);

//       let result;
//       try {
//         result = await conn.execute(
//           `INSERT INTO SUBJECTS (SUBJECTS_SUB_NAME, SUBJECTS_DEPT_ID)
//            VALUES (:subject, :dept_id)
//            RETURNING SUBJECTS_SUB_ID INTO :id`,
//           {
//             subject,
//             dept_id: deptId,
//             id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
//           }
//         );
//       } catch (e) {
//         result = await conn.execute(
//           `SELECT SUBJECTS_SUB_ID AS ID FROM SUBJECTS
//            WHERE SUBJECTS_SUB_NAME = :subject AND SUBJECTS_DEPT_ID = :dept_id`,
//           { subject, dept_id: deptId }
//         );
//       }

//       const subId = result.outBinds?.id?.[0] || result.rows?.[0]?.ID;
//       subjMap.set(key, subId);
//     }

//     for (const row of studentRows) {
//       const cols = row.split(',').map(x => x.trim());
//       if (cols.length < 16) continue;

//       const [id, first, last, email, dept, joinDate, ...rest] = cols;
//       const subjects = rest.slice(0, 5);
//       const marks = rest.slice(5, 10);
//       const deptId = deptMap.get(dept);

//       await conn.execute(
//         `BEGIN
//            INSERT INTO STUDENTS (STUDENTS_STU_ID, STUDENTS_FIRST_NAME, STUDENTS_LAST_NAME, STUDENTS_EMAIL, STUDENTS_DEPT_ID, STUDENTS_JOINING_DATE)
//            VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd, 'YYYY-MM-DD'));
//          EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
//          END;`,
//         { id, first, last, email, dept_id: deptId, jd: joinDate }
//       );

//       for (let i = 0; i < 5; i++) {
//         const key = `${dept}::${subjects[i]}`;
//         const subId = subjMap.get(key);
//         const mark = parseInt(marks[i], 10);
//         if (!subId || isNaN(mark)) continue;

//         await conn.execute(
//           `BEGIN
//              INSERT INTO MARKS (MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS)
//              VALUES (:sid, :subId, :mark);
//            EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
//            END;`,
//           { sid: id, subId, mark }
//         );
//       }
//     }

//     await conn.commit();
//     console.log('✅ All data loaded into Oracle.');

//   } catch (err) {
//     console.error('❌ Error:', err);
//   } finally {
//     if (conn) await conn.close();
//   }
// }

// run();




require('./logger');
const fs = require('fs');
const oracledb = require('oracledb');

// ─────────────────────────────────────────────────────────────────────────────
// Oracle client initialisation
// ─────────────────────────────────────────────────────────────────────────────
oracledb.initOracleClient({
  libDir: 'C\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

const dbConfig = {
  user          : 'system',
  password      : 'Moodle@123',
  connectString : 'localhost:1522/oracle'
};

// ─────────────────────────────────────────────────────────────────────────────
// FILE HELPERS & VALIDATION
// ─────────────────────────────────────────────────────────────────────────────
function safeReadLines(file) {
  try {
    const lines = fs.readFileSync(file, 'utf-8')
      .split('\n')
      .slice(1)                 // skip CSV header row
      .filter(l => l.trim());   // drop blank lines
    console.log(`✅ ${file} loaded – ${lines.length} data row(s)`);
    return lines;
  } catch (err) {
    console.error(`❌ Error reading ${file}:`, err.message);
    process.exit(1);
  }
}

function bail(file, rowNum, msg) {
  console.error(`❌ Validation failed in ${file} (row ${rowNum + 2}): ${msg}`); // +2 for header + 1‑based lines
  process.exit(1);
}

function validateGradeRows(rows) {
  rows.forEach((r, i) => {
    const cols = r.split(',').map(c => c.trim());
    if (cols.length !== 5) bail('grade.txt', i, `Expected 5 columns, found ${cols.length}`);
    const [id, code, label, range, gpa] = cols;
    if (!/^[0-9]+$/.test(id))            bail('grade.txt', i, 'GRADE_ID must be an integer');
    if (!code)                          bail('grade.txt', i, 'GRADE_CODE cannot be empty');
    if (!label)                         bail('grade.txt', i, 'GRADE_LABEL cannot be empty');
    if (!/^[0-9]+(-[0-9]+)?%?$/.test(range)) bail('grade.txt', i, 'PERCENTAGE_RANGE looks wrong');
    if (isNaN(parseFloat(gpa)))         bail('grade.txt', i, 'GPA_EQUIVALENT must be numeric');
  });
  console.log('✅ grade.txt validation passed');
}

function validateStudentRows(rows) {
  rows.forEach((r, i) => {
    const cols = r.split(',').map(c => c.trim());
    if (cols.length < 16) bail('students.txt', i, `Expected ≥16 columns, found ${cols.length}`);

    const [id, first, last, email, dept, joinDate, ...rest] = cols;
    if (!/^[0-9]+$/.test(id))           bail('students.txt', i, 'STUDENTS_STU_ID must be an integer');
    if (!first || !last)                bail('students.txt', i, 'First/last name cannot be empty');
    if (!/.+@.+\..+/.test(email))       bail('students.txt', i, 'Invalid email');
    if (!dept)                          bail('students.txt', i, 'Department cannot be empty');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(joinDate)) bail('students.txt', i, 'Joining date must be YYYY-MM-DD');

    const subjects = rest.slice(0, 5);
    const marks    = rest.slice(5, 10);
    if (subjects.length !== 5 || marks.length !== 5) bail('students.txt', i, 'Need 5 subjects & 5 marks');
    marks.forEach(m => {
      if (isNaN(parseInt(m, 10))) bail('students.txt', i, 'Mark must be an integer');
    });
  });
  console.log('✅ students.txt validation passed');
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN IMPORT LOGIC
// ─────────────────────────────────────────────────────────────────────────────
async function run() {
  let conn;
  try {
    // 1️⃣  Read & validate files (fail‑fast, no DB touch yet)  ──────────────
    const gradeRows   = safeReadLines('grade.txt');
    validateGradeRows(gradeRows);

    const studentRows = safeReadLines('students.txt');
    validateStudentRows(studentRows);

    // 2️⃣  Connect to Oracle only after validation passes  ────────────────
    conn = await oracledb.getConnection(dbConfig);
    console.log('✅ Connected to Oracle');

    // Summary counters
    let gradeInserted = 0;
    let deptInserted  = 0;
    let subjInserted  = 0;
    let stuInserted   = 0;
    let markInserted  = 0;

    // ────────────────  GRADE  ────────────────
    for (const row of gradeRows) {
      const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
      try {
        const res = await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(GRADE, GRADE_PK) */
           INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
           VALUES (:id, :code, :label, :range, :gpa)`,
          { id, code, label, range, gpa }
        );
        gradeInserted += res.rowsAffected ?? 0;
      } catch {/* duplicate ignored */}
    }
    console.log(`→ GRADE       : ${gradeInserted} row(s) inserted (duplicates ignored)`);

    // ────────────────  DEPARTMENTS + SUBJECTS PREP  ────────────────
    const deptSet = new Set();
    const subjSet = new Set();
    for (const line of studentRows) {
      const cols = line.split(',').map(x => x.trim());
      const dept = cols[4];
      const subjects = cols.slice(6, 11);
      deptSet.add(dept);
      subjects.forEach(s => subjSet.add(`${dept}::${s}`));
    }

    // ────────────────  DEPARTMENTS  ────────────────
    const deptMap = new Map();
    for (const dept of deptSet) {
      try {
        const res = await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DEPARTMENTS, DEPARTMENTS_PK) */
           INTO DEPARTMENTS (DEPARTMENTS_DEPT_NAME)
           VALUES (:dept)
           RETURNING DEPARTMENTS_DEPT_ID INTO :id`,
          { dept, id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER } }
        );
        if (res.rowsAffected === 1) deptInserted++;
        deptMap.set(dept, res.outBinds.id[0]);
      } catch {
        const res = await conn.execute(
          `SELECT DEPARTMENTS_DEPT_ID AS ID FROM DEPARTMENTS WHERE DEPARTMENTS_DEPT_NAME = :dept`,
          { dept }
        );
        deptMap.set(dept, res.rows[0].ID);
      }
    }
    console.log(`→ DEPARTMENTS : ${deptInserted} row(s) inserted (duplicates ignored)`);

    // ────────────────  SUBJECTS  ────────────────
    const subjMap = new Map();
    for (const key of subjSet) {
      const [dept, subject] = key.split('::');
      const deptId = deptMap.get(dept);
      try {
        const res = await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(SUBJECTS, SUBJECTS_PK) */
           INTO SUBJECTS (SUBJECTS_SUB_NAME, SUBJECTS_DEPT_ID)
           VALUES (:subject, :dept_id)
           RETURNING SUBJECTS_SUB_ID INTO :id`,
          { subject, dept_id: deptId, id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER } }
        );
        if (res.rowsAffected === 1) subjInserted++;
        subjMap.set(key, res.outBinds.id[0]);
      } catch {
        const res = await conn.execute(
          `SELECT SUBJECTS_SUB_ID AS ID FROM SUBJECTS
           WHERE SUBJECTS_SUB_NAME = :subject AND SUBJECTS_DEPT_ID = :dept_id`,
          { subject, dept_id: deptId }
        );
        subjMap.set(key, res.rows[0].ID);
      }
    }
    console.log(`→ SUBJECTS    : ${subjInserted} row(s) inserted (duplicates ignored)`);

    // ────────────────  STUDENTS & MARKS  ────────────────
    for (const line of studentRows) {
      const cols = line.split(',').map(x => x.trim());
      const [id, first, last, email, dept, joinDate, ...rest] = cols;
      const subjects = rest.slice(0, 5);
      const marks    = rest.slice(5, 10).map(m => parseInt(m, 10));
      const deptId = deptMap.get(dept);

      // STUDENT
      try {
        const res = await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(STUDENTS, STUDENTS_PK) */
           INTO STUDENTS (STUDENTS_STU_ID, STUDENTS_FIRST_NAME, STUDENTS_LAST_NAME, STUDENTS_EMAIL, STUDENTS_DEPT_ID, STUDENTS_JOINING_DATE)
           VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd, 'YYYY-MM-DD'))`,
          { id, first, last, email, dept_id: deptId, jd: joinDate }
        );
        if (res.rowsAffected === 1) stuInserted++;
      } catch {/* duplicate */}

      // MARKS (5 subjects)
      for (let i = 0; i < 5; i++) {
        const mark = marks[i];
        if (isNaN(mark)) continue;
        const subId = subjMap.get(`${dept}::${subjects[i]}`);
        if (!subId) continue;
        try {
          const res = await conn.execute(
            `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(MARKS, MARKS_PK) */
             INTO MARKS (MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS)
             VALUES (:sid, :subId, :mark)`,
            { sid: id, subId, mark }
          );
          if (res.rowsAffected === 1) markInserted++;
        } catch {/* duplicate */}
      }
    }
    console.log(`→ STUDENTS    : ${stuInserted} row(s) inserted (duplicates ignored)`);
    console.log(`→ MARKS       : ${markInserted} row(s) inserted (duplicates ignored)`);

    // 3️⃣  Commit & final summary  ──────────────────────────────────────────
    await conn.commit();

    console.success('\n🎉 Oracle load complete');
    console.info('──────── ── Summary ── ─────────');
    console.info(`GRADE       : ${gradeInserted}`);
    console.info(`DEPARTMENTS : ${deptInserted}`);
    console.info(`SUBJECTS    : ${subjInserted}`);
    console.info(`STUDENTS    : ${stuInserted}`);
    console.info(`MARKS       : ${markInserted}`);
  } catch (err) {
    console.error('❌ Import failed – rolling back:', err.message);
    try { if (conn) await conn.rollback(); } catch {}
    process.exit(1);
  } finally {
    if (conn) {
      try { await conn.close(); } catch {}
    }
  }
}

run()