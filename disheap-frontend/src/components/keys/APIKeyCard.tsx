import { useState } from 'react';
import { 
  KeyIcon, 
  EyeIcon, 
  EyeSlashIcon, 
  ClipboardIcon, 
  CalendarIcon,
  ChartBarIcon,
  ExclamationTriangleIcon,
  EllipsisVerticalIcon,
} from '@heroicons/react/24/outline';
import { Menu } from '@headlessui/react';
import type { APIKey } from '../../lib/types';
import { useNotifications } from '../../store/app';

interface APIKeyCardProps {
  apiKey: APIKey;
  onRevoke?: () => void;
  onViewUsage?: () => void;
  className?: string;
}

function formatLastUsed(lastUsed: string | null | undefined): string {
  if (!lastUsed) return 'Never used';
  
  const date = new Date(lastUsed);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  
  if (diffDays === 0) {
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    if (diffHours === 0) {
      const diffMins = Math.floor(diffMs / (1000 * 60));
      return diffMins < 1 ? 'Just now' : `${diffMins}m ago`;
    }
    return `${diffHours}h ago`;
  }
  
  if (diffDays === 1) return 'Yesterday';
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  
  return date.toLocaleDateString();
}

function formatUsageCount(count: number): string {
  if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`;
  if (count >= 1000) return `${(count / 1000).toFixed(1)}K`;
  return count.toString();
}

function getStatusBadge(apiKey: APIKey) {
  const isExpired = apiKey.expires_at && new Date(apiKey.expires_at) < new Date();
  const isRecent = apiKey.last_used_at && 
    new Date().getTime() - new Date(apiKey.last_used_at).getTime() < 7 * 24 * 60 * 60 * 1000; // 7 days
  
  if (isExpired) {
    return {
      label: 'Expired',
      classes: 'bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300',
      icon: ExclamationTriangleIcon,
    };
  }
  
  if (isRecent) {
    return {
      label: 'Active',
      classes: 'bg-success-100 text-success-800 dark:bg-success-900/20 dark:text-success-300',
      icon: ChartBarIcon,
    };
  }
  
  return {
    label: 'Inactive',
    classes: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
    icon: CalendarIcon,
  };
}

export function APIKeyCard({ apiKey, onRevoke, onViewUsage, className = '' }: APIKeyCardProps) {
  const [showKey, setShowKey] = useState(false);
  const { showSuccess, showError } = useNotifications();
  
  const status = getStatusBadge(apiKey);
  const StatusIcon = status.icon;
  
  const copyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(apiKey.key);
      showSuccess('Copied!', 'API key copied to clipboard');
    } catch (err) {
      showError('Copy failed', 'Unable to copy to clipboard');
    }
  };
  
  const maskKey = (key: string) => {
    if (key.length <= 8) return '•'.repeat(key.length);
    return key.slice(0, 4) + '•'.repeat(key.length - 8) + key.slice(-4);
  };

  return (
    <div className={`bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm hover:shadow-md transition-shadow duration-200 ${className}`}>
      {/* Header */}
      <div className="p-6 pb-4">
        <div className="flex items-start justify-between">
          <div className="flex items-center space-x-3">
            <div className="flex-shrink-0">
              <div className="w-10 h-10 bg-primary-100 dark:bg-primary-900/20 rounded-lg flex items-center justify-center">
                <KeyIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
              </div>
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                {apiKey.name}
              </h3>
              <div className="flex items-center space-x-2 mt-1">
                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${status.classes}`}>
                  <StatusIcon className="w-3 h-3 mr-1" />
                  {status.label}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-400">
                  Created {new Date(apiKey.created_at).toLocaleDateString()}
                </span>
              </div>
            </div>
          </div>

          {/* Actions Menu */}
          <Menu as="div" className="relative">
            <Menu.Button className="flex items-center justify-center w-8 h-8 rounded-md text-gray-400 hover:text-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 dark:hover:text-gray-300">
              <EllipsisVerticalIcon className="w-5 h-5" />
            </Menu.Button>
            <Menu.Items className="absolute right-0 z-10 mt-2 w-48 origin-top-right rounded-md bg-white dark:bg-gray-800 shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
              <div className="py-1">
                {onViewUsage && (
                  <Menu.Item>
                    {({ active }) => (
                      <button
                        onClick={onViewUsage}
                        className={`${
                          active ? 'bg-gray-100 dark:bg-gray-700' : ''
                        } group flex w-full items-center px-4 py-2 text-sm text-gray-700 dark:text-gray-300`}
                      >
                        <ChartBarIcon className="w-4 h-4 mr-3" />
                        View Usage
                      </button>
                    )}
                  </Menu.Item>
                )}
                <Menu.Item>
                  {({ active }) => (
                    <button
                      onClick={copyToClipboard}
                      className={`${
                        active ? 'bg-gray-100 dark:bg-gray-700' : ''
                      } group flex w-full items-center px-4 py-2 text-sm text-gray-700 dark:text-gray-300`}
                    >
                      <ClipboardIcon className="w-4 h-4 mr-3" />
                      Copy Key
                    </button>
                  )}
                </Menu.Item>
                {onRevoke && (
                  <Menu.Item>
                    {({ active }) => (
                      <button
                        onClick={onRevoke}
                        className={`${
                          active ? 'bg-gray-100 dark:bg-gray-700' : ''
                        } group flex w-full items-center px-4 py-2 text-sm text-error-600 dark:text-error-400`}
                      >
                        <ExclamationTriangleIcon className="w-4 h-4 mr-3" />
                        Revoke Key
                      </button>
                    )}
                  </Menu.Item>
                )}
              </div>
            </Menu.Items>
          </Menu>
        </div>
      </div>

      {/* API Key Display */}
      <div className="px-6 pb-4">
        <div className="flex items-center space-x-3 p-3 bg-gray-50 dark:bg-gray-700/50 rounded-md">
          <code className="flex-1 text-sm font-mono text-gray-900 dark:text-gray-100 break-all">
            {showKey ? apiKey.key : maskKey(apiKey.key)}
          </code>
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setShowKey(!showKey)}
              className="p-1 rounded text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
              title={showKey ? 'Hide key' : 'Show key'}
            >
              {showKey ? <EyeSlashIcon className="w-4 h-4" /> : <EyeIcon className="w-4 h-4" />}
            </button>
            <button
              onClick={copyToClipboard}
              className="p-1 rounded text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
              title="Copy to clipboard"
            >
              <ClipboardIcon className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Usage Statistics */}
      <div className="px-6 pb-4">
        <div className="grid grid-cols-3 gap-4 text-sm">
          <div className="text-center">
            <div className="text-lg font-semibold text-gray-900 dark:text-white">
              {formatUsageCount(apiKey.usage?.total_requests || 0)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">Total Requests</div>
          </div>
          
          <div className="text-center">
            <div className="text-lg font-semibold text-warning-600 dark:text-warning-400">
              {formatUsageCount(apiKey.usage?.requests_last_24h || 0)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">Last 24h</div>
          </div>
          
          <div className="text-center">
            <div className="text-lg font-semibold text-error-600 dark:text-error-400">
              {formatUsageCount(apiKey.usage?.error_count || 0)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">Errors</div>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="px-6 py-3 bg-gray-50 dark:bg-gray-700/50 rounded-b-lg">
        <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
          <div>
            Last used: {formatLastUsed(apiKey.last_used_at)}
          </div>
          <div>
            {apiKey.expires_at && (
              <span className="inline-flex items-center">
                <CalendarIcon className="w-3 h-3 mr-1" />
                Expires {new Date(apiKey.expires_at).toLocaleDateString()}
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
