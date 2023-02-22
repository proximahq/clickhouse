import type {Dispatcher} from 'undici';
import dbg from 'debug';

const ERRORED = new RegExp('Code: ([0-9]{2}), .*Exception:');

const log = dbg('proxima:clickhouse-driver:error');

interface ErrorProps {
  statusCode: Dispatcher.ResponseData['statusCode'];
  txt: string;
}

export async function getErrorObj(opts: ErrorProps): Promise<Error> {
  const {statusCode, txt} = opts;
  log('getErrorObj');
  let data = '';

  try {
    log('decode stream');
    data = txt;
  } catch (error) {
    //   @ts-ignore
    data = error?.bufferedData ?? '';
  }

  const err = new Error(`Clickhouse error`);
  //   @ts-ignore
  err.body = data;
  //   @ts-ignore
  err.statusCode = statusCode;

  if (data) {
    const m = data.match(ERRORED);
    if (m) {
      if (m[1] && isNaN(parseInt(m[1])) === false) {
        //   @ts-ignore
        err.code = parseInt(m[1]);
      }

      if (m[2]) {
        //   @ts-ignore
        err.message = m[2];
      }
    }
  }
  //   @ts-ignore
  return err;
}
