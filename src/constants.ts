export const TRAILING_SEMI = /;+$/;
export const JSON_SUFFIX = 'FORMAT JSON';
export const JSON_EACH_SUFFIX = 'FORMAT JSONEachRow';

export const OK = 'Ok.';

export const defaultOpts = {
  host: 'localhost',
  port: 8123,
  db: 'default',
  protocol: 'http',
  user: '',
  password: '',
  connections: 1024,
  keepAliveMaxTimeout: 128,
  headersTimeout: 0,
  bodyTimeout: 0,
  size: 128,
};

export const streamableJSONFormats = [
  'JSONEachRow',
  'JSONStringsEachRow',
  'JSONCompactEachRow',
  'JSONCompactStringsEachRow',
  'JSONCompactEachRowWithNames',
  'JSONCompactEachRowWithNamesAndTypes',
  'JSONCompactStringsEachRowWithNames',
  'JSONCompactStringsEachRowWithNamesAndTypes',
] as const;

export const singleDocumentJSONFormats = [
  'JSON',
  'JSONStrings',
  'JSONCompact',
  'JSONCompactStrings',
  'JSONColumnsWithMetadata',
  'JSONObjectEachRow',
] as const;

export const supportedJSONFormats = [
  ...singleDocumentJSONFormats,
  ...streamableJSONFormats,
] as const;

export const supportedRawFormats = [
  'CSV',
  'CSVWithNames',
  'CSVWithNamesAndTypes',
  'TabSeparated',
  'TabSeparatedRaw',
  'TabSeparatedWithNames',
  'TabSeparatedWithNamesAndTypes',
  'CustomSeparated',
  'CustomSeparatedWithNames',
  'CustomSeparatedWithNamesAndTypes',
] as const;
