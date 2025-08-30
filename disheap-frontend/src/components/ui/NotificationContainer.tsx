import React from 'react';
import { Transition } from '@headlessui/react';
import {
  CheckCircleIcon,
  ExclamationTriangleIcon,
  ExclamationCircleIcon,
  InformationCircleIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { useNotifications } from '../../store/app';
import type { Notification, NotificationType } from '../../lib/types';

// Icon mapping for different notification types
const iconMap: Record<NotificationType, React.ComponentType<{ className?: string }>> = {
  success: CheckCircleIcon,
  error: ExclamationCircleIcon,
  warning: ExclamationTriangleIcon,
  info: InformationCircleIcon,
};

// Color classes for different notification types
const colorClasses: Record<NotificationType, string> = {
  success: 'alert-success',
  error: 'alert-error',
  warning: 'alert-warning',
  info: 'alert-info',
};

interface NotificationItemProps {
  notification: Notification;
  onRemove: (id: string) => void;
}

function NotificationItem({ notification, onRemove }: NotificationItemProps) {
  const Icon = iconMap[notification.type];
  const colorClass = colorClasses[notification.type];

  // Auto-remove after duration
  React.useEffect(() => {
    if (notification.duration && notification.duration > 0) {
      const timer = setTimeout(() => {
        onRemove(notification.id);
      }, notification.duration);

      return () => clearTimeout(timer);
    }
  }, [notification.id, notification.duration, onRemove]);

  return (
    <Transition
      show={true}
      enter="transform ease-out duration-300 transition"
      enterFrom="translate-y-2 opacity-0 sm:translate-y-0 sm:translate-x-2"
      enterTo="translate-y-0 opacity-100 sm:translate-x-0"
      leave="transition ease-in duration-100"
      leaveFrom="opacity-100"
      leaveTo="opacity-0"
    >
      <div className={`alert ${colorClass} max-w-sm w-full shadow-lg pointer-events-auto`}>
        <div className="flex-shrink-0">
          <Icon className="h-5 w-5" />
        </div>
        <div className="ml-3 w-0 flex-1">
          <p className="text-sm font-medium">
            {notification.title}
          </p>
          {notification.message && (
            <p className="mt-1 text-sm opacity-90">
              {notification.message}
            </p>
          )}
        </div>
        <div className="ml-4 flex-shrink-0 flex">
          <button
            type="button"
            className="inline-flex rounded-md p-1.5 hover:bg-black/5 dark:hover:bg-white/5 focus:outline-none focus:ring-2 focus:ring-offset-2"
            onClick={() => onRemove(notification.id)}
          >
            <span className="sr-only">Dismiss</span>
            <XMarkIcon className="h-4 w-4" />
          </button>
        </div>
      </div>
    </Transition>
  );
}

export function NotificationContainer() {
  const { notifications, removeNotification } = useNotifications();

  if (notifications.length === 0) {
    return null;
  }

  return (
    <div
      aria-live="assertive"
      className="fixed inset-0 flex items-end px-4 py-6 pointer-events-none sm:p-6 sm:items-start z-50"
    >
      <div className="w-full flex flex-col items-center space-y-4 sm:items-end">
        {notifications.map((notification) => (
          <NotificationItem
            key={notification.id}
            notification={notification}
            onRemove={removeNotification}
          />
        ))}
      </div>
    </div>
  );
}

// Custom hook for showing notifications with common patterns
export function useNotificationPatterns() {
  const { showSuccess, showError, showWarning, showInfo } = useNotifications();

  return {
    showLoadingSuccess: (action: string) => {
      showSuccess(`${action} completed`, 'The operation was successful.');
    },
    
    showLoadingError: (action: string, error?: string) => {
      showError(
        `${action} failed`,
        error || 'An unexpected error occurred. Please try again.'
      );
    },
    
    showSaveSuccess: (itemType: string) => {
      showSuccess(`${itemType} saved`, 'Your changes have been saved successfully.');
    },
    
    showDeleteSuccess: (itemType: string) => {
      showSuccess(`${itemType} deleted`, `The ${itemType.toLowerCase()} has been deleted.`);
    },
    
    showDeleteWarning: (itemType: string) => {
      showWarning(
        `Delete ${itemType}?`,
        'This action cannot be undone. Are you sure you want to continue?'
      );
    },
    
    showNetworkError: () => {
      showError(
        'Connection Error',
        'Unable to connect to the server. Please check your internet connection.'
      );
    },
    
    showPermissionError: () => {
      showError(
        'Permission Denied',
        'You do not have permission to perform this action.'
      );
    },
    
    showValidationError: (message: string) => {
      showWarning('Validation Error', message);
    },
    
    showMaintenanceNotice: () => {
      showInfo(
        'Maintenance Mode',
        'The system is currently undergoing maintenance. Some features may be unavailable.',
        0 // Don't auto-dismiss
      );
    },
    
    showUpdateAvailable: (version: string) => {
      showInfo(
        'Update Available',
        `Version ${version} is now available. Refresh to update.`,
        0 // Don't auto-dismiss
      );
    },
  };
}
