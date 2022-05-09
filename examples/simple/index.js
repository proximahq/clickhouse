const {clickhouse} = require('../../dist');

const run = async () => {
  const s = clickhouse({
    host: 'localhost',
    port: 8123,
    connections: 128,
    user: 'default',
    protocol: 'http',
    password: 'password',
    sessionId: 'helloworld',
  });
  await s.open();
  try {
    const ss = await s.query('SELECT 1 as F');
    console.log(ss);
  } catch (e) {
    console.log(e);
  }
  await s.close();
};

run();
