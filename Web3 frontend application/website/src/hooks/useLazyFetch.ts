'use client';
import { useEffect, useState, useCallback } from 'react';

export type FetchStatus = 'idle' | 'loading' | 'succeeded' | 'failed';

export interface UseLazyFetchOptions<T> {
  autoRefreshOnVisible?: boolean;
  autoRefreshOnRouteChange?: boolean;
  useCache?: boolean;
  initialData?: T;
}

const cacheMap = new Map<string, any>();

export function useLazyFetch<T>(
  key: string,
  fetcher: () => Promise<T>,
  options?: UseLazyFetchOptions<T>
) {
  const { autoRefreshOnVisible = false, autoRefreshOnRouteChange = false, useCache = false, initialData } = options || {};

  const [data, setData] = useState<T | undefined>(initialData);
  const [status, setStatus] = useState<FetchStatus>('idle');
  const [error, setError] = useState<string | undefined>();
  const [isRefreshing, setIsRefreshing] = useState(false);

  const fetchData = useCallback(async (force: boolean = false) => {
    if (useCache && !force && cacheMap.has(key)) {
      setData(cacheMap.get(key));
      setStatus('succeeded');
      return cacheMap.get(key);
    }

    try {
      setStatus('loading');
      setIsRefreshing(true);
      const result = await fetcher();
      setData(result);

      setStatus('succeeded');
      setError(undefined);
      if (useCache) cacheMap.set(key, result);
      return result;
    } catch (err: any) {
      setStatus('failed');
      setError(err?.message || 'Fetch failed');
      return null;
    } finally {
      setIsRefreshing(false);
    }
  }, [key, fetcher, useCache]);

  useEffect(() => {
    const handleVisibility = () => {
      if (document.visibilityState === 'visible' && autoRefreshOnVisible) {
        fetchData(true);
      }
    };
    if (autoRefreshOnVisible) {
      document.addEventListener('visibilitychange', handleVisibility);
    }
    return () => {
      if (autoRefreshOnVisible) {
        document.removeEventListener('visibilitychange', handleVisibility);
      }
    };
  }, [fetchData, autoRefreshOnVisible]);


  return { data, status, error, isRefreshing, fetchData };
}
