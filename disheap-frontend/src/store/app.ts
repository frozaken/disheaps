import React from 'react';
import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import type { 
  Notification, 
  Theme, 
  UserPreferences
} from '../lib/types';

export interface AppState {
  // Theme and preferences
  theme: Theme;
  preferences: UserPreferences;
  
  // Notifications
  notifications: Notification[];
  
  // UI state
  sidebarOpen: boolean;
  isOnline: boolean;
  
  // Actions
  setTheme: (theme: Theme) => void;
  updatePreferences: (preferences: Partial<UserPreferences>) => void;
  
  // Notification actions
  addNotification: (notification: Omit<Notification, 'id'>) => void;
  removeNotification: (id: string) => void;
  clearNotifications: () => void;
  
  // UI actions
  setSidebarOpen: (open: boolean) => void;
  toggleSidebar: () => void;
  setOnlineStatus: (online: boolean) => void;
}

// Default preferences
const defaultPreferences: UserPreferences = {
  theme: 'system',
  autoRefreshInterval: 30, // seconds
  timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  notifications: {
    email: true,
    browser: true,
  },
};

export const useAppStore = create<AppState>()(
  persist(
    (set, get) => ({
      // Initial state
      theme: 'system',
      preferences: defaultPreferences,
      notifications: [],
      sidebarOpen: false,
      isOnline: navigator.onLine,

      // Theme actions
      setTheme: (theme: Theme) => {
        set({ theme });
        
        // Update preferences as well
        const { preferences } = get();
        set({
          preferences: {
            ...preferences,
            theme,
          },
        });
        
        // Apply theme to document
        applyTheme(theme);
      },

      updatePreferences: (newPreferences: Partial<UserPreferences>) => {
        const { preferences } = get();
        const updated = { ...preferences, ...newPreferences };
        
        set({ preferences: updated });
        
        // Apply theme if it changed
        if (newPreferences.theme && newPreferences.theme !== preferences.theme) {
          set({ theme: newPreferences.theme });
          applyTheme(newPreferences.theme);
        }
      },

      // Notification actions
      addNotification: (notification: Omit<Notification, 'id'>) => {
        const id = crypto.randomUUID();
        const newNotification: Notification = {
          ...notification,
          id,
        };
        
        set((state) => ({
          notifications: [...state.notifications, newNotification],
        }));
        
        // Auto-remove notification after duration
        if (notification.duration && notification.duration > 0) {
          setTimeout(() => {
            get().removeNotification(id);
          }, notification.duration);
        }
      },

      removeNotification: (id: string) => {
        set((state) => ({
          notifications: state.notifications.filter(n => n.id !== id),
        }));
      },

      clearNotifications: () => {
        set({ notifications: [] });
      },

      // UI actions
      setSidebarOpen: (open: boolean) => {
        set({ sidebarOpen: open });
      },

      toggleSidebar: () => {
        set((state) => ({ sidebarOpen: !state.sidebarOpen }));
      },

      setOnlineStatus: (online: boolean) => {
        set({ isOnline: online });
      },
    }),
    {
      name: 'disheap_app_state',
      storage: createJSONStorage(() => localStorage),
      // Don't persist notifications and UI state
      partialize: (state) => ({
        theme: state.theme,
        preferences: state.preferences,
      }),
      onRehydrateStorage: () => (state) => {
        if (state) {
          // Apply theme on app start
          applyTheme(state.theme);
        }
      },
    }
  )
);

// Theme application logic
function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  
  if (theme === 'dark') {
    root.classList.add('dark');
  } else if (theme === 'light') {
    root.classList.remove('dark');
  } else {
    // System theme
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    if (prefersDark) {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
  }
}

// Convenience hooks
export function useTheme() {
  const { theme, setTheme } = useAppStore();
  return { theme, setTheme };
}

export function useNotifications() {
  const {
    notifications,
    addNotification,
    removeNotification,
    clearNotifications,
  } = useAppStore();
  
  return {
    notifications,
    addNotification,
    removeNotification,
    clearNotifications,
    // Convenience methods
    showSuccess: (title: string, message?: string, duration = 5000) => {
      addNotification({ type: 'success', title, message, duration });
    },
    showError: (title: string, message?: string, duration = 8000) => {
      addNotification({ type: 'error', title, message, duration });
    },
    showWarning: (title: string, message?: string, duration = 6000) => {
      addNotification({ type: 'warning', title, message, duration });
    },
    showInfo: (title: string, message?: string, duration = 4000) => {
      addNotification({ type: 'info', title, message, duration });
    },
  };
}

export function usePreferences() {
  const { preferences, updatePreferences } = useAppStore();
  return { preferences, updatePreferences };
}

export function useSidebar() {
  const { sidebarOpen, setSidebarOpen, toggleSidebar } = useAppStore();
  return { sidebarOpen, setSidebarOpen, toggleSidebar };
}

export function useOnlineStatus() {
  const { isOnline, setOnlineStatus } = useAppStore();
  
  // Set up online/offline listeners
  React.useEffect(() => {
    const handleOnline = () => setOnlineStatus(true);
    const handleOffline = () => setOnlineStatus(false);
    
    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);
    
    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, [setOnlineStatus]);
  
  return isOnline;
}

// Initialize theme on import
if (typeof window !== 'undefined') {
  const storedState = localStorage.getItem('disheap_app_state');
  const theme = storedState ? JSON.parse(storedState).state?.theme || 'system' : 'system';
  applyTheme(theme);
  
  // Listen for system theme changes
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  mediaQuery.addEventListener('change', () => {
    const currentTheme = useAppStore.getState().theme;
    if (currentTheme === 'system') {
      applyTheme('system');
    }
  });
}
