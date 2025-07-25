/******************************************************************
 *  load_to_oracle.js - ETL script to load data into Oracle DB
 ******************************************************************/

const fs = require('fs');
const oracledb = require('oracledb');

// ---------------------------------------------------------------
// 1. Oracle Client Initialization
// ---------------------------------------------------------------
oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

// ---------------------------------------------------------------
// 2. Oracle DB Connection Configuration
// ---------------------------------------------------------------
const dbConfig = {
  user: 'system',
  password: 'Moodle@123',
  connectString: 'localhost:1522/oracle'
};

// ---------------------------------------------------------------
// 3. Helper to Read CSV and Clean Lines
// ---------------------------------------------------------------
function readLines(file) {
  return fs.readFileSync(file, 'utf-8')
    .split('\n')
    .slice(1)
    .filter(line => line.trim());
}

// ---------------------------------------------------------------
// 4. Main ETL Logic
// ---------------------------------------------------------------
async function run() {
  let conn;
  try {
    conn = await oracledb.getConnection(dbConfig);
    console.log('✅ Connected to Oracle');

    // ------------------ Load GRADE ------------------
    const gradeRows = readLines('grade.txt');
    for (const row of gradeRows) {
      const [id, code, label, range, gpa] = row.split(',').map(x => x.trim());
      await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(GRADE GRADE_PK) */
         INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
         VALUES (:id, :code, :label, :range, :gpa)`,
        { id, code, label, range, gpa }
      );
    }
    console.log(`→ Inserted ${gradeRows.length} rows into GRADE`);

    // ------------------ Parse Students ------------------
    const studentRows = readLines('students.txt');
    const deptSet = new Set();
    const subjSet = new Set(); // format: `${dept}::${subject}`

    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;
      const dept = cols[4];
      const subjects = cols.slice(6, 11);
      deptSet.add(dept);
      subjects.forEach(s => subjSet.add(`${dept}::${s}`));
    }

    // ------------------ Insert Departments ------------------
    const deptMap = new Map();
    for (const dept of deptSet) {
      const result = await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DEPARTMENTS DEPARTMENTS_DEPT_NAME_UK) */
         INTO DEPARTMENTS (DEPARTMENTS_DEPT_NAME)
         VALUES (:dept)
         RETURNING DEPARTMENTS_DEPT_ID INTO :id`,
        {
          dept,
          id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
        }
      );

      const deptId = result.outBinds.id?.[0] ||
        (await conn.execute(
          `SELECT DEPARTMENTS_DEPT_ID FROM DEPARTMENTS WHERE DEPARTMENTS_DEPT_NAME = :dept`,
          [dept]
        )).rows[0][0];
      deptMap.set(dept, deptId);
    }

    // ------------------ Insert Subjects ------------------
    const subjMap = new Map();
    for (const key of subjSet) {
      const [dept, subject] = key.split('::');
      const result = await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(SUBJECTS SUBJECTS_SUB_NAME_UK) */
         INTO SUBJECTS (SUBJECTS_SUB_NAME, SUBJECTS_DEPT_ID)
         VALUES (:subject, :dept_id)
         RETURNING SUBJECTS_SUB_ID INTO :id`,
        {
          subject,
          dept_id: deptMap.get(dept),
          id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
        }
      );

      const subId = result.outBinds.id?.[0] ||
        (await conn.execute(
          `SELECT SUBJECTS_SUB_ID FROM SUBJECTS
           WHERE SUBJECTS_SUB_NAME = :subject AND SUBJECTS_DEPT_ID = :dept_id`,
          [subject, deptMap.get(dept)]
        )).rows[0][0];
      subjMap.set(key, subId);
    }

    // ------------------ Insert Students and Marks ------------------
    for (const row of studentRows) {
      const cols = row.split(',').map(x => x.trim());
      if (cols.length < 16) continue;

      const [id, first, last, email, dept, joinDate, ...rest] = cols;
      const subjects = rest.slice(0, 5);
      const marks = rest.slice(5, 10);

      await conn.execute(
        `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(STUDENTS STUDENTS_STU_ID_PK) */
         INTO STUDENTS (STUDENTS_STU_ID, STUDENTS_FIRST_NAME, STUDENTS_LAST_NAME,
                        STUDENTS_EMAIL, STUDENTS_DEPT_ID, STUDENTS_JOINING_DATE)
         VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd,'YYYY-MM-DD'))`,
        { id, first, last, email, dept_id: deptMap.get(dept), jd: joinDate }
      );

      for (let i = 0; i < 5; i++) {
        const key = `${dept}::${subjects[i]}`;
        const subId = subjMap.get(key);
        const mark = parseInt(marks[i]);
        if (!subId || isNaN(mark)) continue;

        await conn.execute(
          `INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(MARKS MARKS_PK) */
           INTO MARKS (MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS)
           VALUES (:sid, :subId, :mark)`,
          { sid: id, subId, mark }
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
