// exportToExcel.js
const ExcelJS = require("exceljs");
const path = require("path");
const fs = require("fs");

async function exportToExcel(data, outputFileName = "etl_result.xlsx") {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Students GPA");

  if (!data.length) return;

  worksheet.columns = Object.keys(data[0]).map((key) => ({
    header: key,
    key: key,
    width: 25,
  }));

  data.forEach((record) => worksheet.addRow(record));

  const outputDir = path.resolve(__dirname, "exports");
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);

  const filePath = path.join(outputDir, outputFileName);
  await workbook.xlsx.writeFile(filePath);
  return filePath;
}

module.exports = { exportToExcel };
