require('./logger');
const oracledb = require('oracledb');
oracledb.initOracleClient({
  libDir: 'C:\\Users\\RakshitSharma\\Downloads\\WINDOWS.X64_193000_db_home\\bin'
});

(async () => {
  console.log('⏳Trying to Connect To ORACLE DB')
  try {
    const conn = await oracledb.getConnection({
      user: 'system',
      password: 'Moodle@123',
      connectString: 'localhost:1522/oracle'
    });
    console.log('✅ Connected! to ORACLE database');
    await conn.close();
  } catch (e) {
    console.error(e);
  }
})();