import type { Response } from 'express';

export type DataMode = 'production' | 'demo';

export interface StandardErrorResponse {
  status: 'error';
  code: string;
  message: string;
  request_id: string;
}

export interface StandardSuccessResponse<T extends Record<string, unknown>> {
  status: 'success';
  request_id?: string;
}

export function getRequestId(res: Response): string {
  return String(res.locals.requestId || 'unknown');
}

export function sendApiError(
  res: Response,
  httpStatus: number,
  code: string,
  message: string,
): void {
  const payload: StandardErrorResponse = {
    status: 'error',
    code,
    message,
    request_id: getRequestId(res),
  };
  res.status(httpStatus).json(payload);
}

export function sendApiSuccess<T extends Record<string, unknown>>(
  res: Response,
  payload: T,
  httpStatus = 200,
): void {
  res.status(httpStatus).json({
    status: 'success',
    ...payload,
  });
}

export function resolveDataMode(hasDatabase: boolean): DataMode {
  const configured = String(process.env.APP_DATA_MODE || '').trim().toLowerCase();
  if (configured === 'demo') return 'demo';
  if (configured === 'production') return 'production';
  return hasDatabase ? 'production' : 'demo';
}
