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
    const test = await s.query('SELECT name FROM system.databases');
    console.log(test);
  } catch (e) {
    console.log(e);
  }
  await s.close();
};

run();
