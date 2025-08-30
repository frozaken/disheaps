import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { PrivateRoute, PublicRoute, AuthGuard } from './auth/PrivateRoute';
import { LoginForm } from './auth/LoginForm';
import { AppLayout } from './components/layout/AppLayout';
import { NotificationContainer } from './components/ui/NotificationContainer';
import { useAppStore, useOnlineStatus } from './store/app';
import { setupGlobalErrorHandling } from './lib/errors';
import './index.css';

// Lazy load page components for better performance
const Dashboard = React.lazy(() => import('./pages/dashboard/Dashboard').then(m => ({ default: m.Dashboard })));
const HeapsList = React.lazy(() => import('./pages/heaps/HeapsList').then(m => ({ default: m.HeapsList })));
const HeapDetail = React.lazy(() => import('./pages/heaps/HeapDetail').then(m => ({ default: m.HeapDetail })));
const APIKeysList = React.lazy(() => import('./pages/keys/APIKeysList').then(m => ({ default: m.APIKeysList })));
const DLQBrowser = React.lazy(() => import('./pages/dlq/DLQBrowser').then(m => ({ default: m.DLQBrowser })));
const Settings = React.lazy(() => import('./pages/settings/Settings').then(m => ({ default: m.Settings })));

// Loading fallback component
function PageLoader() {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <div className="text-center">
        <div className="loading-spinner h-8 w-8 mx-auto mb-4" />
        <p className="text-gray-600 dark:text-gray-400">Loading...</p>
      </div>
    </div>
  );
}

// Suspense wrapper for lazy-loaded components
function SuspenseWrapper({ children }: { children: React.ReactNode }) {
  return (
    <React.Suspense fallback={<PageLoader />}>
      {children}
    </React.Suspense>
  );
}

export default function App() {
  const isOnline = useOnlineStatus();
  const { addNotification } = useAppStore();

  // Set up global error handling
  useEffect(() => {
    setupGlobalErrorHandling();
  }, []);

  // Handle online/offline status changes
  useEffect(() => {
    if (!isOnline) {
      addNotification({
        type: 'warning',
        title: 'Connection Lost',
        message: 'You are currently offline. Some features may not work properly.',
      });
    }
  }, [isOnline, addNotification]);

  return (
    <Router>
      <AuthGuard>
        <div className="app">
          <Routes>
            {/* Public routes (login) */}
            <Route
              path="/login"
              element={
                <PublicRoute>
                  <LoginForm />
                </PublicRoute>
              }
            />

            {/* Protected routes */}
            <Route
              path="/*"
              element={
                <PrivateRoute>
                  <AppLayout>
                    <SuspenseWrapper>
                      <Routes>
                        {/* Dashboard */}
                        <Route path="/dashboard" element={<Dashboard />} />

                        {/* Heaps management */}
                        <Route path="/heaps" element={<HeapsList />} />
                        <Route path="/heaps/:topic" element={<HeapDetail />} />

                        {/* API Keys */}
                        <Route path="/keys" element={<APIKeysList />} />

                        {/* Dead Letter Queue */}
                        <Route path="/dlq" element={<DLQBrowser />} />
                        <Route path="/dlq/:topic" element={<DLQBrowser />} />

                        {/* Settings */}
                        <Route path="/settings" element={<Settings />} />
                        <Route path="/settings/:section" element={<Settings />} />

                        {/* Default redirect */}
                        <Route path="/" element={<Navigate to="/dashboard" replace />} />

                        {/* 404 page */}
                        <Route
                          path="*"
                          element={
                            <div className="flex items-center justify-center min-h-screen">
                              <div className="text-center">
                                <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-4">
                                  404
                                </h1>
                                <p className="text-gray-600 dark:text-gray-400 mb-4">
                                  Page not found
                                </p>
                                <button
                                  onClick={() => window.history.back()}
                                  className="btn btn-primary"
                                >
                                  Go Back
                                </button>
                              </div>
                            </div>
                          }
                        />
                      </Routes>
                    </SuspenseWrapper>
                  </AppLayout>
                </PrivateRoute>
              }
            />
          </Routes>

          {/* Global notification container */}
          <NotificationContainer />

          {/* Offline indicator */}
          {!isOnline && (
            <div className="fixed bottom-4 left-4 bg-warning-500 text-white px-4 py-2 rounded-md shadow-lg z-50">
              <div className="flex items-center space-x-2">
                <svg className="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.732-.833-2.5 0L3.334 16.5c-.77.833.192 2.5 1.732 2.5z"
                  />
                </svg>
                <span className="text-sm font-medium">You are offline</span>
              </div>
            </div>
          )}

          {/* Application version indicator (development only) */}
          {import.meta.env.DEV && (
            <div className="fixed bottom-4 right-4 text-xs text-gray-500 dark:text-gray-400 bg-white dark:bg-gray-800 px-2 py-1 rounded shadow">
              v{import.meta.env.VITE_APP_VERSION || '1.0.0'}
            </div>
          )}
        </div>
      </AuthGuard>
    </Router>
  );
}