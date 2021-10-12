const ERRORED = new RegExp('Code: ([0-9]{2}), .*Exception:');

interface ErrorProps {
  statusCode: number;
  data: string;
}
export function getErrorObj(opts: ErrorProps): Error {
  const {statusCode, data} = opts;
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
