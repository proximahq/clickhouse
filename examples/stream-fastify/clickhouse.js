const fp = require('fastify-plugin');
const clickhouse = require('../../dist/clickhouse');

const decorate = (server, options, done) => {
  const client = clickhouse({
    host: 'play.clickhouse.com',
    protocol: 'https',
    port: 8443,
    user: 'playground',
    password: 'clickhouse',
    db: 'datasets',
  });
  server.log.info('Decorating server with clickhouse');

  server.decorate('clickhouse', {
    client: client,
    request: ({queryStr, headers, opq}) =>
      client.queryStream({
        query: queryStr,
        opaque: opq,
        factory: ({statusCode, headers, opaque}) => {
          return opaque.raw;
        },
      }),
  });

  server.addHook('onClose', async server => {
    server.log.info('Closing clickhouse client');
    await client.close();
  });
  done();
};

module.exports = fp(decorate);
