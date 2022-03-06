const fastify = require('fastify');

const server = fastify({
  logger: {
    prettyPrint: {
      translateTime: 'HH:MM:ss Z',
      ignore: 'pid,hostname',
    },
  },
});

server.register(require('./clickhouse'));

server.route({
  url: '/',
  method: 'GET',
  handler: (request, response) => {
    return {hello: 'world'};
  },
});

server.route({
  url: '/stream',
  method: 'GET',
  handler: (request, response) => {
    server.clickhouse.request({
      queryStr: `SELECT 1 as foo FORMAT JSON;`,
      opq: response,
    });
  },
});

const start = async () => {
  try {
    await server.listen(3000);
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};
start();
