import {TRAILING_SEMI} from './constants';
import dbg from 'debug';
import {nanoid} from 'nanoid';
import type {JsonObject} from './types';
const log = dbg('proxima:clickhouse-driver:utils');

export const isEmptyObj = (obj: {}) =>
  Object.keys(obj).length === 0 && obj.constructor === Object;

export const isEmptyArr = (arr: any) => Array.isArray(arr) && arr.length === 0;

export const isNil = (v: any) =>
  v === undefined || v === null || isEmptyObj(v) || isEmptyArr(v) || v === '';

export const cleanupObj = (obj: any = {}) => {
  log('cleanupObj');
  const s = Object.keys(obj).reduce((acc, k) => {
    if (isNil(obj[k])) {
      return acc;
    }
    return {...acc, [k]: obj[k]};
  }, {});
  log('obj: %o', obj);
  log('cleanupObj: %o', s);
  return s;
};

export const cleanup = (str: string) => str.replace(TRAILING_SEMI, '');

export const createPathGen = () => {
  const s = {
    session_timeout: 60,
    output_format_json_quote_64bit_integers: 0,
    enable_http_compression: 1,
  };
  return function create(obj: JsonObject) {
    return createPath({...s, ...obj});
  };
};

export const createPath = (obj: any) => {
  const cleaned = cleanupObj(obj);
  const str = new URLSearchParams(cleaned).toString();
  return !str ? '/' : `/?${str}`;
};

export const genIds = () => {
  const maxInt = 2147483647;
  let nextReqId = 0;
  const str = nanoid(16);
  return function next() {
    nextReqId = (nextReqId + 1) & maxInt;
    return `${str}-${nextReqId.toString(36)}`;
  };
};
