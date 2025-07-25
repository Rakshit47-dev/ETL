const { exportToExcel } = require("./exportToExcel");
const { sendEmailWithAttachment } = require("./sendEmail");
const { Client } = require("pg");

// Postgres DB config
const pgClient = new Client({
  user: "postgres",
  host: "localhost",
  database: "your_db_name",
  password: "your_password",
  port: 5432,
});

async function getRecordsFromPostgres() {
  await pgClient.connect();
  const res = await pgClient.query("SELECT * FROM student_academics");
  await pgClient.end();
  return res.rows;
}

async function run() {
  const records = await getRecordsFromPostgres();
  const filePath = await exportToExcel(records);
  await sendEmailWithAttachment(filePath);
}

run();
