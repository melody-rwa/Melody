'use client';
import { useEffect, useState, useCallback } from 'react';
export type FetchStatus = 'loading' | 'succeeded' | 'failed';

export interface FetchResult<T> {
  data: T | null;
  status: FetchStatus;
  error?: string;
}

export function useFetch<T>(
  url: string,
  xmethod: string,
  deps: any[] = [],
  initialData: T | null = null
): FetchResult<T> & { refetch: () => void } {
  const [result, setResult] = useState<FetchResult<T>>({
    data: initialData,
    status: 'loading',
  });

  const fetchData = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const res = await fetch(url, {
          signal,
          next: { revalidate: 600 },
          headers: { 'x-method': xmethod },
        });
        if (!res.ok) {
          setResult({
            data: initialData,
            status: 'failed',
            error: res.statusText || 'Fetch error',
          });
        } else {
          const data = (await res.json()) as T;
          setResult({ data, status: 'succeeded' });
        }
      } catch (err: any) {
        if (err.name === 'AbortError') return;
        setResult({
          data: initialData,
          status: 'failed',
          error: err.message || 'Request failed',
        });
      }
    },
    [url, xmethod, initialData]
  );

  useEffect(() => {
    if (!url || !xmethod) {
      setResult({
        data: initialData,
        status: 'failed',
        error: 'No account',
      });
      return;
    }

    const controller = new AbortController();
    setResult({ data: initialData, status: 'loading' });
    fetchData(controller.signal);

    return () => controller.abort();
     // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchData, ...deps]);

  return { ...result, refetch: () => fetchData() };
}
