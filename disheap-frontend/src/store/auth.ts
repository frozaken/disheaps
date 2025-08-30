import React from 'react';
import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import type { User, LoginRequest } from '../lib/types';
import { api, type DisheapAPIError } from '../lib/api';
import { isAuthError } from '../lib/errors';

export interface AuthState {
  // State
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
  
  // Actions
  login: (credentials: LoginRequest) => Promise<void>;
  logout: () => Promise<void>;
  refreshToken: () => Promise<void>;
  getCurrentUser: () => Promise<void>;
  clearError: () => void;
  
  // Internal methods
  setUser: (user: User | null) => void;
  setToken: (token: string | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
}

// Token storage key
const TOKEN_STORAGE_KEY = 'disheap_auth_token';

// Auto-refresh interval (5 minutes before expiry)
const REFRESH_BUFFER_MS = 5 * 60 * 1000;

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      // Initial state
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,

      // Actions
      login: async (credentials: LoginRequest) => {
        set({ isLoading: true, error: null });
        
        try {
          const response = await api.login(credentials);
          
          // Store token and user info
          set({
            user: response.user,
            token: response.token,
            isAuthenticated: true,
            isLoading: false,
            error: null,
          });
          
          // Schedule token refresh
          scheduleTokenRefresh(response.expires_at, get().refreshToken);
          
        } catch (error: any) {
          const errorMessage = (error as DisheapAPIError)?.message 
            ? (error as DisheapAPIError).message
            : 'Login failed. Please try again.';
            
          set({
            user: null,
            token: null,
            isAuthenticated: false,
            isLoading: false,
            error: errorMessage,
          });
          
          throw error;
        }
      },

      logout: async () => {
        set({ isLoading: true });
        
        try {
          await api.logout();
        } catch (error) {
          // Continue with logout even if API call fails
          console.warn('Logout API call failed:', error);
        }
        
        // Clear all auth state
        set({
          user: null,
          token: null,
          isAuthenticated: false,
          isLoading: false,
          error: null,
        });
        
        // Clear token refresh timeout
        clearTokenRefresh();
      },

      refreshToken: async () => {
        const { token } = get();
        
        if (!token) {
          set({ isAuthenticated: false, user: null, token: null });
          return;
        }
        
        try {
          const response = await api.refreshToken();
          
          set({
            token: response.token,
            error: null,
          });
          
          // Schedule next refresh
          scheduleTokenRefresh(response.expires_at, get().refreshToken);
          
        } catch (error) {
          // If refresh fails, user needs to log in again
          if (isAuthError(error as Error)) {
            set({
              user: null,
              token: null,
              isAuthenticated: false,
              error: 'Session expired. Please log in again.',
            });
          }
          
          throw error;
        }
      },

      getCurrentUser: async () => {
        set({ isLoading: true });
        
        try {
          const user = await api.getCurrentUser();
          set({
            user,
            isLoading: false,
            error: null,
          });
        } catch (error: any) {
          if (isAuthError(error)) {
            set({
              user: null,
              token: null,
              isAuthenticated: false,
              isLoading: false,
              error: 'Authentication required. Please log in.',
            });
          } else {
            set({
              isLoading: false,
              error: (error as DisheapAPIError)?.message 
                ? (error as DisheapAPIError).message
                : 'Failed to get user info.',
            });
          }
          
          throw error;
        }
      },

      clearError: () => {
        set({ error: null });
      },

      // Internal methods
      setUser: (user: User | null) => {
        set({ user });
      },

      setToken: (token: string | null) => {
        set({ token, isAuthenticated: !!token });
      },

      setLoading: (loading: boolean) => {
        set({ isLoading: loading });
      },

      setError: (error: string | null) => {
        set({ error });
      },
    }),
    {
      name: TOKEN_STORAGE_KEY,
      storage: createJSONStorage(() => localStorage),
      // Only persist token and user, not loading/error states
      partialize: (state) => ({
        token: state.token,
        user: state.user,
        isAuthenticated: state.isAuthenticated,
      }),
      // Rehydrate auth state on app load
      onRehydrateStorage: () => (state) => {
        if (state?.token) {
          // Validate token on app start
          state.getCurrentUser().catch(() => {
            // If validation fails, clear auth state
            state.logout();
          });
        }
      },
    }
  )
);

// Token refresh scheduling
let refreshTimeoutId: NodeJS.Timeout | null = null;

function scheduleTokenRefresh(expiresAt: string, refreshFn: () => Promise<void>): void {
  // Clear existing timeout
  clearTokenRefresh();
  
  const expiryTime = new Date(expiresAt).getTime();
  const refreshTime = expiryTime - REFRESH_BUFFER_MS;
  const now = Date.now();
  
  if (refreshTime <= now) {
    // Token expires soon, refresh immediately
    refreshFn().catch(console.error);
    return;
  }
  
  // Schedule refresh
  const delay = refreshTime - now;
  refreshTimeoutId = setTimeout(() => {
    refreshFn().catch(console.error);
  }, delay);
}

function clearTokenRefresh(): void {
  if (refreshTimeoutId) {
    clearTimeout(refreshTimeoutId);
    refreshTimeoutId = null;
  }
}

// Auth hook for components
export function useAuth() {
  const auth = useAuthStore();
  
  return {
    // State
    user: auth.user,
    isAuthenticated: auth.isAuthenticated,
    isLoading: auth.isLoading,
    error: auth.error,
    
    // Actions
    login: auth.login,
    logout: auth.logout,
    refreshToken: auth.refreshToken,
    getCurrentUser: auth.getCurrentUser,
    clearError: auth.clearError,
  };
}

// Higher-order hook for protecting routes
export function useRequireAuth(): User {
  const { user, isAuthenticated, isLoading, getCurrentUser } = useAuth();
  
  // Try to get user if not loaded
  React.useEffect(() => {
    if (!isLoading && !user && isAuthenticated) {
      getCurrentUser().catch(() => {
        // Error handled by the store
      });
    }
  }, [user, isAuthenticated, isLoading, getCurrentUser]);
  
  if (!isAuthenticated) {
    throw new Error('Authentication required');
  }
  
  if (!user) {
    throw new Error('User information not available');
  }
  
  return user;
}
