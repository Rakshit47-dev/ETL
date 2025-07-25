const oracledb = require('oracledb');
oracledb.initOracleClient({ libDir: 'C:\\app\\RakshitSharma\\product\\19.0.0\\dbhome_1\\bin' }); // Update path to your Oracle Instant Client

(async () => {
  try {
    const conn = await oracledb.getConnection({
      user: 'system',
      password: 'Moodle@123',
      connectString: 'localhost:1522/oracle'
    });
    console.log('✅ Connected!');
    await conn.close();
  } catch (e) {
    console.error(e);
  }
})();