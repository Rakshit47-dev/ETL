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

// function readLines(file) {
//   return fs.readFileSync(file, 'utf-8')
//     .split('\n')
//     .slice(1)
//     .filter(l => l.trim());
// }

// async function run() {
//   let conn;
//   try {
//     conn = await oracledb.getConnection(dbConfig);
//     console.log('✅ Connected to Oracle');

//     const gradeRows = readLines('grade.txt');
//     for (const row of gradeRows) {
//       const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
//       await conn.execute(
//         `BEGIN
//            INSERT INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
//            VALUES (:id, :code, :label, :range, :gpa);
//          EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
//          END;`,
//         { id, code, label, range, gpa }
//       );
//     }
//     console.log(`→ Inserted ${gradeRows.length} rows into GRADE`);

//     const studentRows = readLines('students.txt');
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
//         const mark = parseInt(marks[i]);
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




const fs = require('fs');
const oracledb = require('oracledb');

oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

const dbConfig = {
  user          : 'system',
  password      : 'Moodle@123',
  connectString : 'localhost:1522/oracle'
};

/* ---------- safe file reader with logging ---------- */
function safeReadLines (file) {
  try {
    const lines = fs.readFileSync(file, 'utf-8')
      .split('\n')
      .slice(1)               // skip header
      .filter(l => l.trim());

    console.log(`✅ ${file} loaded successfully – data extracted (${lines.length} rows)`);
    return lines;
  } catch (err) {
    console.error(`❌ Error reading ${file}:`, err.message);
    process.exit(1);
  }
}

async function run () {
  let conn;
  try {
    conn = await oracledb.getConnection(dbConfig);
    console.log('✅ Connected to Oracle');

    /* ---------- 1. GRADE ---------- */
    const gradeRows = safeReadLines('grade.txt');
    for (const row of gradeRows) {
      const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
      try {
        await conn.execute(
          `BEGIN
             INSERT INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
             VALUES (:id, :code, :label, :range, :gpa);
           EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
           END;`,
          { id, code, label, range, gpa }
        );
      } catch (e) {
        console.error(`⚠️  Could not load GRADE row [${row}]: ${e.message}`);
      }
    }
    console.log(`→ Processed ${gradeRows.length} rows for GRADE`);

    /* ---------- 2. Build department & subject sets ---------- */
    const studentRows = safeReadLines('students.txt');

    const deptSet = new Set();
    const subjSet = new Set();
    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;
      const dept      = cols[4];
      const subjects  = cols.slice(6, 11);
      deptSet.add(dept);
      subjects.forEach(s => subjSet.add(`${dept}::${s}`));
    }

    /* ---------- 3. DEPARTMENTS ---------- */
    const deptMap = new Map();
    for (const dept of deptSet) {
      try {
        const result = await conn.execute(
          `INSERT INTO DEPARTMENTS (DEPARTMENTS_DEPT_NAME)
           VALUES (:dept)
           RETURNING DEPARTMENTS_DEPT_ID INTO :id`,
          {
            dept,
            id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
          }
        );
        deptMap.set(dept, result.outBinds.id[0]);
      } catch {
        const r = await conn.execute(
          `SELECT DEPARTMENTS_DEPT_ID AS ID FROM DEPARTMENTS
           WHERE DEPARTMENTS_DEPT_NAME = :dept`,
          { dept }
        );
        deptMap.set(dept, r.rows[0].ID);
      }
    }
    console.log(`→ Upserted ${deptMap.size} departments`);

    /* ---------- 4. SUBJECTS ---------- */
    const subjMap = new Map();
    for (const key of subjSet) {
      const [dept, subject] = key.split('::');
      const deptId = deptMap.get(dept);

      try {
        const result = await conn.execute(
          `INSERT INTO SUBJECTS (SUBJECTS_SUB_NAME, SUBJECTS_DEPT_ID)
           VALUES (:subject, :dept_id)
           RETURNING SUBJECTS_SUB_ID INTO :id`,
          {
            subject,
            dept_id: deptId,
            id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
          }
        );
        subjMap.set(key, result.outBinds.id[0]);
      } catch {
        const r = await conn.execute(
          `SELECT SUBJECTS_SUB_ID AS ID FROM SUBJECTS
           WHERE SUBJECTS_SUB_NAME = :subject AND SUBJECTS_DEPT_ID = :dept_id`,
          { subject, dept_id: deptId }
        );
        subjMap.set(key, r.rows[0].ID);
      }
    }
    console.log(`→ Upserted ${subjMap.size} subjects`);

    /* ---------- 5. STUDENTS & MARKS ---------- */
    let studentCount = 0, markCount = 0;
    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;

      const [id, first, last, email, dept, joinDate, ...rest] = cols;
      const subjects = rest.slice(0, 5);
      const marks    = rest.slice(5, 10);
      const deptId   = deptMap.get(dept);

      /* STUDENT row */
      try {
        await conn.execute(
          `BEGIN
             INSERT INTO STUDENTS (
               STUDENTS_STU_ID, STUDENTS_FIRST_NAME, STUDENTS_LAST_NAME,
               STUDENTS_EMAIL, STUDENTS_DEPT_ID, STUDENTS_JOINING_DATE
             )
             VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd, 'YYYY-MM-DD'));
           EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
           END;`,
          { id, first, last, email, dept_id: deptId, jd: joinDate }
        );
        studentCount++;
      } catch (e) {
        console.error(`⚠️  Could not load STUDENT ${id}: ${e.message}`);
      }

      /* MARK rows */
      for (let i = 0; i < 5; i++) {
        const key  = `${dept}::${subjects[i]}`;
        const subId = subjMap.get(key);
        const mark  = parseInt(marks[i], 10);
        if (!subId || isNaN(mark)) continue;

        try {
          await conn.execute(
            `BEGIN
               INSERT INTO MARKS (MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS)
               VALUES (:sid, :subId, :mark);
             EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL;
             END;`,
            { sid: id, subId, mark }
          );
          markCount++;
        } catch (e) {
          console.error(`⚠️  Could not load MARK (${id}, ${subjects[i]}): ${e.message}`);
        }
      }
    }

    await conn.commit();
    console.log(`✅ Loaded ${studentCount} students and ${markCount} marks into Oracle`);

  } catch (err) {
    console.error('❌ Fatal error:', err);
  } finally {
    if (conn) await conn.close();
  }
}

run();
