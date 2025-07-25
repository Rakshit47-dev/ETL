const nodemailer = require("nodemailer");
const path = require("path");

async function sendEmailWithAttachment(filePath) {
  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: "rs11113@gmail.com", // 🔐 use an App Password if 2FA is enabled
      pass: "Moodle@123"
    }
  });

  const mailOptions = {
    from: "rs11113@gmail.com",
    to: "sakshamdubeyy23@example.com",
    subject: "ETL Excel Export",
    text: "Attached is the exported student GPA report.",
    attachments: [
      {
        filename: path.basename(filePath),
        path: filePath
      }
    ]
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    console.log("✅ Email sent:", info.response);
  } catch (err) {
    console.error("❌ Error sending email:", err);
  }
}

module.exports = { sendEmailWithAttachment };
