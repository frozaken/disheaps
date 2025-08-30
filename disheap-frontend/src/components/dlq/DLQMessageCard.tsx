import { useState } from 'react';
import {
  ClockIcon,
  ExclamationTriangleIcon,

  ArrowPathIcon,
  TrashIcon,
  EyeIcon,
  DocumentDuplicateIcon,
  TagIcon,
  ServerIcon,
} from '@heroicons/react/24/outline';
import type { DLQMessage } from '../../lib/types';
import { useNotifications } from '../../store/app';

interface DLQMessageCardProps {
  message: DLQMessage;
  isSelected?: boolean;
  onSelect?: (selected: boolean) => void;
  onReplay?: () => void;
  onDelete?: () => void;
  onViewDetails?: () => void;
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  
  // If less than 24 hours ago, show relative time
  if (diffMs < 24 * 60 * 60 * 1000) {
    const hours = Math.floor(diffMs / (60 * 60 * 1000));
    const minutes = Math.floor((diffMs % (60 * 60 * 1000)) / (60 * 1000));
    
    if (hours === 0) {
      return minutes === 0 ? 'Just now' : `${minutes}m ago`;
    }
    return `${hours}h ${minutes}m ago`;
  }
  
  // Otherwise show the date
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function truncatePayload(payload: string, maxLength: 100): string {
  if (payload.length <= maxLength) return payload;
  return payload.substring(0, maxLength) + '...';
}

function getReasonIcon(reason?: string) {
  if (!reason) return ExclamationTriangleIcon;
  
  const lowerReason = reason.toLowerCase();
  if (lowerReason.includes('timeout')) return ClockIcon;
  if (lowerReason.includes('retry') || lowerReason.includes('attempt')) return ArrowPathIcon;
  return ExclamationTriangleIcon;
}

function getReasonColor(reason?: string): string {
  if (!reason) return 'text-error-600 dark:text-error-400';
  
  const lowerReason = reason.toLowerCase();
  if (lowerReason.includes('timeout')) return 'text-warning-600 dark:text-warning-400';
  if (lowerReason.includes('retry')) return 'text-blue-600 dark:text-blue-400';
  return 'text-error-600 dark:text-error-400';
}

export function DLQMessageCard({
  message,
  isSelected = false,
  onSelect,
  onReplay,
  onDelete,
  onViewDetails,
}: DLQMessageCardProps) {
  const [showFullPayload, setShowFullPayload] = useState(false);
  const { showSuccess } = useNotifications();
  
  const ReasonIcon = getReasonIcon(message.dlq_reason);
  const reasonColor = getReasonColor(message.dlq_reason);
  
  // Decode payload (assuming it's base64)
  const decodedPayload = (() => {
    try {
      return atob(message.payload);
    } catch {
      return message.payload;
    }
  })();
  
  const copyMessageId = async () => {
    try {
      await navigator.clipboard.writeText(message.message_id);
      showSuccess('Copied!', 'Message ID copied to clipboard');
    } catch (err) {
      // Silently fail - clipboard might not be available
    }
  };
  
  const copyPayload = async () => {
    try {
      await navigator.clipboard.writeText(decodedPayload);
      showSuccess('Copied!', 'Message payload copied to clipboard');
    } catch (err) {
      // Silently fail
    }
  };

  return (
    <div
      className={`bg-white dark:bg-gray-800 border-2 rounded-lg shadow-sm hover:shadow-md transition-all duration-200 ${
        isSelected
          ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/10'
          : 'border-gray-200 dark:border-gray-700'
      }`}
    >
      {/* Header */}
      <div className="p-4 pb-0">
        <div className="flex items-start justify-between">
          <div className="flex items-start space-x-3">
            {onSelect && (
              <div className="flex items-center pt-1">
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={(e) => onSelect(e.target.checked)}
                  className="form-checkbox text-primary-600 rounded focus:ring-primary-500 focus:ring-offset-0"
                />
              </div>
            )}
            
            <div className="flex-1">
              <div className="flex items-center space-x-2 mb-2">
                <span className="text-sm font-mono text-gray-600 dark:text-gray-400">
                  {message.message_id.slice(0, 8)}...
                </span>
                <button
                  onClick={copyMessageId}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                  title="Copy message ID"
                >
                  <DocumentDuplicateIcon className="w-4 h-4" />
                </button>
                <div className="flex items-center space-x-1 text-xs text-gray-500 dark:text-gray-400">
                  <TagIcon className="w-3 h-3" />
                  <span>{message.topic}</span>
                </div>
                <div className="flex items-center space-x-1 text-xs text-gray-500 dark:text-gray-400">
                  <ServerIcon className="w-3 h-3" />
                  <span>P{message.partition_id}</span>
                </div>
              </div>
              
              {/* Failure Reason */}
              <div className="flex items-center space-x-2 mb-2">
                <div className={`flex items-center space-x-1.5 ${reasonColor}`}>
                  <ReasonIcon className="w-4 h-4" />
                  <span className="text-sm font-medium">
                    {message.dlq_reason || 'Max retries exceeded'}
                  </span>
                </div>
                <span className="text-xs text-gray-500 dark:text-gray-400">
                  {message.attempts}/{message.max_retries} attempts
                </span>
              </div>
            </div>
          </div>
          
          {/* Actions */}
          <div className="flex items-center space-x-1">
            {onViewDetails && (
              <button
                onClick={onViewDetails}
                className="p-1.5 rounded-md text-gray-400 hover:text-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 dark:hover:text-gray-300 transition-colors"
                title="View details"
              >
                <EyeIcon className="w-4 h-4" />
              </button>
            )}
            {onReplay && (
              <button
                onClick={onReplay}
                className="p-1.5 rounded-md text-blue-600 hover:text-blue-700 hover:bg-blue-50 dark:hover:bg-blue-900/20 dark:text-blue-400 dark:hover:text-blue-300 transition-colors"
                title="Replay message"
              >
                <ArrowPathIcon className="w-4 h-4" />
              </button>
            )}
            {onDelete && (
              <button
                onClick={onDelete}
                className="p-1.5 rounded-md text-error-600 hover:text-error-700 hover:bg-error-50 dark:hover:bg-error-900/20 dark:text-error-400 dark:hover:text-error-300 transition-colors"
                title="Delete message"
              >
                <TrashIcon className="w-4 h-4" />
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Payload */}
      <div className="px-4 pb-2">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">
              Payload
            </label>
            <div className="bg-gray-50 dark:bg-gray-700/50 rounded-md p-3">
              <code className="text-sm text-gray-900 dark:text-gray-100 font-mono block whitespace-pre-wrap break-all">
                {showFullPayload ? decodedPayload : truncatePayload(decodedPayload, 100)}
              </code>
              {decodedPayload.length > 100 && (
                <button
                  onClick={() => setShowFullPayload(!showFullPayload)}
                  className="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300"
                >
                  {showFullPayload ? 'Show less' : 'Show more'}
                </button>
              )}
            </div>
          </div>
          <button
            onClick={copyPayload}
            className="ml-2 p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
            title="Copy payload"
          >
            <DocumentDuplicateIcon className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Footer with metadata */}
      <div className="px-4 py-3 bg-gray-50 dark:bg-gray-700/50 rounded-b-lg">
        <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-1">
              <ClockIcon className="w-3 h-3" />
              <span>Enqueued: {formatTimestamp(message.enqueued_time)}</span>
            </div>
            <div className="flex items-center space-x-1">
              <ExclamationTriangleIcon className="w-3 h-3" />
              <span>Failed: {formatTimestamp(message.dlq_timestamp)}</span>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300">
              Priority: {message.priority}
            </span>
            {message.producer_id && (
              <span title={`Producer: ${message.producer_id}`} className="truncate max-w-20">
                {message.producer_id}
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
