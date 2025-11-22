'use client';
import { useEffect, useState, useCallback, useMemo } from 'react';
import { usePathname } from 'next/navigation';

export type FetchStatus = 'loading' | 'succeeded' | 'failed';

export interface PageDataType<T> {
  rows: T;
  total: number;
  pages: number;
}

export interface FetchResult<T> extends PageDataType<T> {
  status: FetchStatus;
  error?: string;
}


export function usePageFetch<T>(
  url: string,
  xmethod: string,
  deps: any[] = [],
  initialData: T | null = null,
  options: {
    autoRefreshOnVisible?: boolean;       
    autoRefreshOnRouteChange?: boolean;  
    useCache?: boolean;                  
  } = {}
): FetchResult<T> & {
  refetch: () => void;
  clearCache: () => void;
  clearCacheAndFetch: () => void;
} {
  const {
    autoRefreshOnVisible = true,
    autoRefreshOnRouteChange = true,
    useCache = true,
  } = options;

  const [result, setResult] = useState<FetchResult<T>>({
    rows: initialData as T,
    pages: 0,
    total: 0,
    status: 'loading',
  });

  const pathname = usePathname();
  const cache = useMemo(() => new Map<string, PageDataType<T>>(), []); 
  const cacheKey = `${url}::${xmethod}`;

  const fetchData = useCallback(
    async (signal?: AbortSignal, useCacheNow = true) => {
      if (!url || !xmethod) return;

 
      if (useCache && useCacheNow && cache.has(cacheKey)) {
        const cached = cache.get(cacheKey)!;
        setResult({ ...cached, status: 'succeeded' });
        return;
      }

      try {
        const res = await fetch(url, {
          signal,
          headers: { 'x-method': xmethod },
        });

        if (!res.ok) {
          setResult({
            rows: initialData as T,
            pages: 0,
            total: 0,
            status: 'failed',
            error: res.statusText || 'Fetch error',
          });
          return;
        }

        const data = (await res.json()) as PageDataType<T>;
        if (useCache) cache.set(cacheKey, data);
        setResult({ ...data, status: 'succeeded' });
      } catch (err: any) {
        if (err.name === 'AbortError') return;
        setResult({
          rows: initialData as T,
          pages: 0,
          total: 0,
          status: 'failed',
          error: err.message || 'Request failed',
        });
      }
    },
    [url, xmethod, initialData, useCache, cache, cacheKey]
  );

  //  const depKey = useMemo(() => JSON.stringify(deps), [deps]);

  useEffect(() => {
    const controller = new AbortController();
    setResult((prev) => ({ ...prev, status: 'loading' }));
    fetchData(controller.signal, true);
    return () => controller.abort();
  }, [fetchData]);


  const refetch = useCallback(() => {
    setResult((prev) => ({ ...prev, status: 'loading' }));
    fetchData(undefined, false);
  }, [fetchData]);

  const clearCache = useCallback(() => {
    cache.delete(cacheKey);
  }, [cacheKey, cache]);

  const clearCacheAndFetch = useCallback(() => {
    cache.delete(cacheKey);
    refetch();
  }, [cacheKey, cache, refetch]);

  useEffect(() => {
    if (!autoRefreshOnVisible) return;
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        console.log('[usePageFetch] The page becomes visible again and refreshes automatically');
        refetch();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [refetch, autoRefreshOnVisible]);

  useEffect(() => {
    if (autoRefreshOnRouteChange) {
      console.log('[usePageFetch]Route changes, automatically refresh data');
      refetch();
    }
  }, [pathname, autoRefreshOnRouteChange, refetch]);

  return {
    ...result,
    refetch,
    clearCache,
    clearCacheAndFetch,
  };
}
