'use client';

import { useState, useEffect, useCallback } from 'react';

interface SessionCheckResult {
  isAuthenticated: boolean;
  user: any;
  isLoading: boolean;
  error: string | null;
  refetch: () => Promise<void>;
}

export function useSessionCheck(): SessionCheckResult {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [user, setUser] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const checkSession = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      
      const response = await fetch('/api/siwe/getLoginUser', {
        credentials: 'include', 
      });
      
      if (response.ok) {
        const data = await response.json();
        setIsAuthenticated(data.isAuthenticated);
        setUser(data.user);
      } else {
        setIsAuthenticated(false);
        setUser(null);
      }
    } catch  {
      setError('Failed to check session');
      setIsAuthenticated(false);
      setUser(null);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    checkSession();
  }, [checkSession]);

  return {
    isAuthenticated,
    user,
    isLoading,
    error,
    refetch: checkSession
  };
}