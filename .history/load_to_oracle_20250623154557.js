

require("./logger");
const fs = require("fs");
const oracledb = require("oracledb");

oracledb.initOracleClient({
  libDir:
    "C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin",
});

const dbConfig = {
  user: "system",
  password: "Moodle@123",
  connectString: "localhost:1522/oracle",
};

/* -----------------------------------------------------------------------------
 * FILE HELPERS & VALIDATION
 * ---------------------------------------------------------------------------*/

console.log(L)
function safeReadLines(file) {
  try {
    const lines = fs
      .readFileSync(file, "utf-8")
      .split("\n")
      .slice(1) // skip header
      .filter((l) => l.trim()); // no blank lines
    console.log(`✅ ${file} loaded – ${lines.length} data row(s)`);
    return lines;
  } catch (err) {
    console.error(`❌ Error reading ${file}:`, err.message);
    process.exit(1);
  }
}

function bail(file, rowNum, msg) {
  console.error(`❌ Validation failed in ${file} (row ${rowNum + 2}): ${msg}`); // +2 to account for skipped header (index+1) & 1‑based humans
  process.exit(1);
}

function validateGradeRows(rows) {
  rows.forEach((r, i) => {
    const cols = r.split(",").map((c) => c.trim());
    if (cols.length !== 5)
      bail("grade.txt", i, "Expected 5 columns, found " + cols.length);
    const [id, code, label, range, gpa] = cols;
    if (!/^[0-9]+$/.test(id))
      bail("grade.txt", i, "GRADE_ID must be an integer");
    if (!code) bail("grade.txt", i, "GRADE_CODE cannot be empty");
    if (!label) bail("grade.txt", i, "GRADE_LABEL cannot be empty");
    if (!/^[0-9]+(-[0-9]+)?%?$/.test(range))
      bail("grade.txt", i, "PERCENTAGE_RANGE looks wrong");
    if (isNaN(parseFloat(gpa)))
      bail("grade.txt", i, "GPA_EQUIVALENT must be numeric");
  });
  console.log("✅ grade.txt validation passed");
}

function validateStudentRows(rows) {
  rows.forEach((r, i) => {
    const cols = r.split(",").map((c) => c.trim());
    if (cols.length < 16)
      bail("students.txt", i, "Expected ≥16 columns, found " + cols.length);

    const [id, first, last, email, dept, joinDate, ...rest] = cols;
    if (!/^[0-9]+$/.test(id))
      bail("students.txt", i, "STUDENTS_STU_ID must be an integer");
    if (!first || !last)
      bail("students.txt", i, "First/last name cannot be empty");
    if (!/.+@.+\..+/.test(email)) bail("students.txt", i, "Invalid email");
    if (!dept) bail("students.txt", i, "Department cannot be empty");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(joinDate))
      bail("students.txt", i, "Joining date must be YYYY-MM-DD");

    const subjects = rest.slice(0, 5);
    const marks = rest.slice(5, 10);
    if (subjects.length !== 5 || marks.length !== 5)
      bail("students.txt", i, "Need 5 subjects & 5 marks");
    marks.forEach((m) => {
      if (isNaN(parseInt(m, 10)))
        bail("students.txt", i, "Mark must be an integer");
    });
  });
  console.log("✅ students.txt validation passed");
}

/* -----------------------------------------------------------------------------
 * MAIN IMPORT LOGIC
 * ---------------------------------------------------------------------------*/
async function run() {
  let conn;
  try {
    // 1. Read + validate files **before** touching DB
    const gradeRows = safeReadLines("grade.txt");
    validateGradeRows(gradeRows);

    const studentRows = safeReadLines("students.txt");
    validateStudentRows(studentRows);

    // 2. Connect to Oracle after validation succeeds
    conn = await oracledb.getConnection(dbConfig);
    console.log("✅ Connected to Oracle");

    /* ---------------- Load GRADE ---------------- */
    for (const row of gradeRows) {
      const [id, code, label, range, gpa] = row.split(",").map((x) => x.trim());
      await conn.execute(
        `BEGIN
           INSERT INTO GRADE (GRADE_ID, GRADE_CODE, GRADE_LABEL, PERCENTAGE_RANGE, GPA_EQUIVALENT)
           VALUES (:id, :code, :label, :range, :gpa);
         EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL; END;`,
        { id, code, label, range, gpa }
      );
    }
    console.log(`→ Extracted ${gradeRows.length} GRADE row(s)`);

    /* ---------------- Prepare caches ---------------- */
    const deptSet = new Set();
    const subjSet = new Set();
    for (const row of studentRows) {
      const cols = row.split(",").map((x) => x.trim());
      const dept = cols[4];
      const subjects = cols.slice(6, 11);
      deptSet.add(dept);
      subjects.forEach((s) => subjSet.add(`${dept}::${s}`));
    }

    /* ---------------- Load DEPARTMENTS ---------------- */
    const deptMap = new Map();
    for (const dept of deptSet) {
      let result;
      try {
        result = await conn.execute(
          `INSERT INTO DEPARTMENTS (DEPARTMENTS_DEPT_NAME)
           VALUES (:dept)
           RETURNING DEPARTMENTS_DEPT_ID INTO :id`,
          { dept, id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER } }
        );
      } catch (e) {
        result = await conn.execute(
          `SELECT DEPARTMENTS_DEPT_ID AS ID FROM DEPARTMENTS WHERE DEPARTMENTS_DEPT_NAME = :dept`,
          { dept }
        );
      }
      const deptId = result.outBinds?.id?.[0] || result.rows?.[0]?.ID;
      deptMap.set(dept, deptId);
    }

    /* ---------------- Load SUBJECTS ---------------- */
    const subjMap = new Map();
    for (const key of subjSet) {
      const [dept, subject] = key.split("::");
      const deptId = deptMap.get(dept);
      let result;
      try {
        result = await conn.execute(
          `INSERT INTO SUBJECTS (SUBJECTS_SUB_NAME, SUBJECTS_DEPT_ID)
           VALUES (:subject, :dept_id)
           RETURNING SUBJECTS_SUB_ID INTO :id`,
          {
            subject,
            dept_id: deptId,
            id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
          }
        );
      } catch (e) {
        result = await conn.execute(
          `SELECT SUBJECTS_SUB_ID AS ID FROM SUBJECTS
           WHERE SUBJECTS_SUB_NAME = :subject AND SUBJECTS_DEPT_ID = :dept_id`,
          { subject, dept_id: deptId }
        );
      }
      const subId = result.outBinds?.id?.[0] || result.rows?.[0]?.ID;
      subjMap.set(key, subId);
    }

    /* ---------------- Load STUDENTS + MARKS ---------------- */
    let stuCount = 0;

    for (const row of studentRows) {
      const cols = row.split(",").map((x) => x.trim());
      const [id, first, last, email, dept, joinDate, ...rest] = cols;
      const subjects = rest.slice(0, 5);
      const marks = rest.slice(5, 10);
      const deptId = deptMap.get(dept);

      await conn.execute(
        `BEGIN
       INSERT INTO STUDENTS (STUDENTS_STU_ID, STUDENTS_FIRST_NAME, STUDENTS_LAST_NAME, STUDENTS_EMAIL, STUDENTS_DEPT_ID, STUDENTS_JOINING_DATE)
       VALUES (:id, :first, :last, :email, :dept_id, TO_DATE(:jd, 'YYYY-MM-DD'));
     EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL; END;`,
        { id, first, last, email, dept_id: deptId, jd: joinDate }
      );

      stuCount++;

     

      for (let i = 0; i < 5; i++) {
        const subId = subjMap.get(`${dept}::${subjects[i]}`);
        const mark = parseInt(marks[i], 10);
        if (!subId || isNaN(mark)) continue;
        await conn.execute(
          `BEGIN
             INSERT INTO MARKS (MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS)
             VALUES (:sid, :subId, :mark);
           EXCEPTION WHEN DUP_VAL_ON_INDEX THEN NULL; END;`,
          { sid: id, subId, mark }
        );
      }
    }

     console.log(`→ Extracted ${stuCount} STUDENT row(s)`);

    await conn.commit();
    console.log("✅ All data loaded into Oracle.");
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    if (conn) await conn.close();
  }
}

run();
