// export_postgres_to_excel.js
const { Client } = require("pg");
const { exportToExcel } = require("./exportToExcel"); // adjust path if needed

const pgConfig = {
  user: "postgres",
  host: "localhost",
  database: "your_db_name",
  password: "your_password",
  port: 5432,
};

async function fetchAndExport() {
  const client = new Client(pgConfig);
  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL");

    const result = await client.query("SELECT * FROM student_academics");
    const data = result.rows;

    if (data.length === 0) {
      console.log("⚠️ No data found in student_academics table.");
      return;
    }

    const filePath = await exportToExcel(data, "student_academics.xlsx");
    console.log("✅ Excel file created:", filePath);
  } catch (err) {
    console.error("❌ Error exporting to Excel:", err);
  } finally {
    await client.end();
  }
}

fetchAndExport();
