const Stream = require('stream');
const {Client} = require('undici');
const {Writable} = require('stream');
const fs = require('fs');

const singleDocumentJSONFormats = [
  'JSON',
  'JSONStrings',
  'JSONCompact',
  'JSONCompactStrings',
  'JSONColumnsWithMetadata',
  'JSONObjectEachRow',
];

const streamableJSONFormats = [
  'JSONEachRow',
  'JSONStringsEachRow',
  'JSONCompactEachRow',
  'JSONCompactStringsEachRow',
  'JSONCompactEachRowWithNames',
  'JSONCompactEachRowWithNamesAndTypes',
  'JSONCompactStringsEachRowWithNames',
  'JSONCompactStringsEachRowWithNamesAndTypes',
];

const supportedJSONFormats = [
  ...singleDocumentJSONFormats,
  ...streamableJSONFormats,
];

function isStream(obj) {
  return obj !== null && typeof obj.pipe === 'function';
}

function mapStream(mapper) {
  return new Stream.Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      callback(null, mapper(chunk));
    },
  });
}

function encodeJSON(value, format) {
  if (supportedJSONFormats.includes(format)) {
    return JSON.stringify(value) + '\n';
  }
  throw new Error(
    `The client does not support JSON encoding in [${format}] format.`,
  );
}

function encodeValues(values, format) {
  if (isStream(values)) {
    // TSV/CSV/CustomSeparated formats don't require additional serialization
    if (!values.readableObjectMode) {
      return values;
    }
    // JSON* formats streams
    return Stream.pipeline(
      values,
      mapStream(value => encodeJSON(value, format)),
      pipelineCb,
    );
  }
  // JSON* arrays
  if (Array.isArray(values)) {
    return values.map(value => encodeJSON(value, format)).join('');
  }
  // JSON & JSONObjectEachRow format input
  if (typeof values === 'object') {
    return encodeJSON(values, format);
  }
  throw new Error(
    `Cannot encode values of type ${typeof values} with ${format} format`,
  );
}

const client = new Client(`http://localhost:8123/`);
const bufs = [];

(async () => {
  try {
    await client
      .stream(
        {
          path: encodeURI(
            '/?query=INSERT INTO example_table FORMAT CSVWithNames',
          ),
          method: 'POST',
          // body: 'SELECT * FROM example_table FORMAT Pretty',
          body: encodeValues(fs.createReadStream('./data.csv'), 'CSVWithNames'),
          headers: {
            'Content-Type': 'application/json',
            //   'X-ClickHouse-User': 'default',
            //   'X-ClickHouse-Key': 'password',
          },
          opaque: {bufs},
        },
        ({statusCode, headers, opaque: {bufs}}) => {
          if (statusCode !== 200) {
            throw new Error(`Unexpected status code: ${statusCode}`);
          }
          return new Writable({
            write(chunk, encoding, callback) {
              bufs.push(chunk);
              callback();
            },
          });
        },
      )
      .catch(err => {
        console.log('errorrrrrrr');
        console.error(err);
      });

    console.log(Buffer.concat(bufs).toString('utf-8'));

    client.close();
  } catch (error) {
    console.error(error);
  }
})();
