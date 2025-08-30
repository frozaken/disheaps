import { useState } from 'react';
import {
  DocumentDuplicateIcon,
  ArrowPathIcon,
  TrashIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  TagIcon,
  ServerIcon,
  UserIcon,
  CodeBracketIcon,
  EyeIcon,
  XCircleIcon,
} from '@heroicons/react/24/outline';
import { Modal } from '../ui/Modal';
import { Tabs } from '../ui/Tabs';
import { useNotifications } from '../../store/app';
import type { DLQMessage } from '../../lib/types';

interface DLQMessageDetailsModalProps {
  isOpen: boolean;
  onClose: () => void;
  message: DLQMessage;
  onReplay?: () => void;
  onDelete?: () => void;
}

interface RetryHistory {
  attempt: number;
  timestamp: string;
  error: string;
  duration: number;
}

function generateRetryHistory(message: DLQMessage): RetryHistory[] {
  const history: RetryHistory[] = [];
  const baseTime = new Date(message.enqueued_time).getTime();
  
  const errors = [
    'Connection timeout after 30s',
    'Invalid JSON payload format',
    'Rate limit exceeded (429)',
    'External service unavailable',
    'Authentication failed',
    'Validation error: missing required field',
    'Database connection lost',
    'Circuit breaker open',
  ];

  for (let i = 1; i <= message.attempts; i++) {
    // Exponential backoff timing
    const delay = Math.pow(2, i - 1) * 1000 * 60; // Start with 1 minute, double each time
    const timestamp = new Date(baseTime + delay + Math.random() * 30000);
    
    history.push({
      attempt: i,
      timestamp: timestamp.toISOString(),
      error: errors[Math.floor(Math.random() * errors.length)],
      duration: Math.floor(Math.random() * 5000 + 100), // 100ms to 5s
    });
  }
  
  return history;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
}

export function DLQMessageDetailsModal({
  isOpen,
  onClose,
  message,
  onReplay,
  onDelete,
}: DLQMessageDetailsModalProps) {
  const [showRawPayload, setShowRawPayload] = useState(false);
  const [replayLoading, setReplayLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const { showSuccess, showError } = useNotifications();

  const retryHistory = generateRetryHistory(message);

  // Decode payload
  const decodedPayload = (() => {
    try {
      const decoded = atob(message.payload);
      return JSON.stringify(JSON.parse(decoded), null, 2);
    } catch {
      try {
        return atob(message.payload);
      } catch {
        return message.payload;
      }
    }
  })();

  const rawPayload = (() => {
    try {
      return atob(message.payload);
    } catch {
      return message.payload;
    }
  })();

  const isJsonPayload = (() => {
    try {
      JSON.parse(rawPayload);
      return true;
    } catch {
      return false;
    }
  })();

  const payloadSize = new Blob([rawPayload]).size;

  const handleCopyPayload = async () => {
    try {
      await navigator.clipboard.writeText(showRawPayload ? rawPayload : decodedPayload);
      showSuccess('Copied!', 'Payload copied to clipboard');
    } catch (err) {
      showError('Copy failed', 'Unable to copy to clipboard');
    }
  };

  const handleCopyMessageId = async () => {
    try {
      await navigator.clipboard.writeText(message.message_id);
      showSuccess('Copied!', 'Message ID copied to clipboard');
    } catch (err) {
      showError('Copy failed', 'Unable to copy to clipboard');
    }
  };

  const handleReplay = async () => {
    setReplayLoading(true);
    try {
      await new Promise(resolve => setTimeout(resolve, 1500));
      showSuccess('Message Replayed', 'Message has been sent back for reprocessing');
      onReplay?.();
      onClose();
    } catch (error) {
      showError('Replay Failed', 'Unable to replay message');
    } finally {
      setReplayLoading(false);
    }
  };

  const handleDelete = async () => {
    setDeleteLoading(true);
    try {
      await new Promise(resolve => setTimeout(resolve, 1000));
      showSuccess('Message Deleted', 'Message has been permanently removed from DLQ');
      onDelete?.();
      onClose();
    } catch (error) {
      showError('Delete Failed', 'Unable to delete message');
    } finally {
      setDeleteLoading(false);
    }
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  const getStatusIcon = (attempt: number) => {
    return attempt === message.attempts ? (
      <XCircleIcon className="w-4 h-4 text-error-500" />
    ) : (
      <ExclamationTriangleIcon className="w-4 h-4 text-warning-500" />
    );
  };

  const tabs = [
    {
      id: 'overview',
      label: 'Overview',
      content: (
        <div className="space-y-6">
          {/* Message Info */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Message Information
            </h3>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Message ID
                  </label>
                  <div className="flex items-center space-x-2">
                    <code className="flex-1 text-sm font-mono text-gray-900 dark:text-white bg-gray-50 dark:bg-gray-700 rounded px-3 py-2">
                      {message.message_id}
                    </code>
                    <button
                      onClick={handleCopyMessageId}
                      className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                      title="Copy message ID"
                    >
                      <DocumentDuplicateIcon className="w-4 h-4" />
                    </button>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Topic
                  </label>
                  <div className="flex items-center space-x-2 text-sm text-gray-900 dark:text-white">
                    <TagIcon className="w-4 h-4 text-gray-400" />
                    <span>{message.topic}</span>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Partition
                  </label>
                  <div className="flex items-center space-x-2 text-sm text-gray-900 dark:text-white">
                    <ServerIcon className="w-4 h-4 text-gray-400" />
                    <span>Partition {message.partition_id}</span>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Priority
                  </label>
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300">
                    {message.priority}
                  </span>
                </div>
              </div>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Enqueued Time
                  </label>
                  <div className="flex items-center space-x-2 text-sm text-gray-900 dark:text-white">
                    <ClockIcon className="w-4 h-4 text-gray-400" />
                    <span>{formatTimestamp(message.enqueued_time)}</span>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Failed Time
                  </label>
                  <div className="flex items-center space-x-2 text-sm text-gray-900 dark:text-white">
                    <ExclamationTriangleIcon className="w-4 h-4 text-error-500" />
                    <span>{formatTimestamp(message.dlq_timestamp)}</span>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Attempts
                  </label>
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300">
                    {message.attempts}/{message.max_retries}
                  </span>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Producer
                  </label>
                  <div className="flex items-center space-x-2 text-sm text-gray-900 dark:text-white">
                    <UserIcon className="w-4 h-4 text-gray-400" />
                    <span>{message.producer_id || 'Unknown'}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Failure Information */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Failure Information
            </h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  DLQ Reason
                </label>
                <div className="flex items-start space-x-3 p-3 bg-error-50 dark:bg-error-900/20 border border-error-200 dark:border-error-800 rounded-lg">
                  <ExclamationTriangleIcon className="w-5 h-5 text-error-600 dark:text-error-400 mt-0.5" />
                  <div>
                    <div className="text-sm font-medium text-error-800 dark:text-error-200">
                      {message.dlq_reason || 'Max retries exceeded'}
                    </div>
                    {message.original_failure_reason && (
                      <div className="text-sm text-error-700 dark:text-error-300 mt-1">
                        Original error: {message.original_failure_reason}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      ),
    },
    {
      id: 'payload',
      label: 'Payload',
      content: (
        <div className="space-y-6">
          {/* Payload Controls */}
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                Message Payload
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                {formatBytes(payloadSize)} • {isJsonPayload ? 'JSON' : 'Text'} format
              </p>
            </div>
            <div className="flex items-center space-x-2">
              {isJsonPayload && (
                <button
                  onClick={() => setShowRawPayload(!showRawPayload)}
                  className="btn btn-secondary btn-sm inline-flex items-center"
                >
                  {showRawPayload ? (
                    <>
                      <CodeBracketIcon className="w-4 h-4 mr-2" />
                      Pretty Print
                    </>
                  ) : (
                    <>
                      <EyeIcon className="w-4 h-4 mr-2" />
                      Raw
                    </>
                  )}
                </button>
              )}
              <button
                onClick={handleCopyPayload}
                className="btn btn-secondary btn-sm inline-flex items-center"
              >
                <DocumentDuplicateIcon className="w-4 h-4 mr-2" />
                Copy
              </button>
            </div>
          </div>

          {/* Payload Display */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 rounded-t-lg">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  {showRawPayload ? 'Raw Payload' : 'Formatted Payload'}
                </span>
                <div className="flex items-center space-x-2 text-xs text-gray-500 dark:text-gray-400">
                  <span>{formatBytes(payloadSize)}</span>
                  <span>•</span>
                  <span>{(showRawPayload ? rawPayload : decodedPayload).split('\n').length} lines</span>
                </div>
              </div>
            </div>
            <div className="p-4">
              <pre className="text-sm text-gray-900 dark:text-white font-mono whitespace-pre-wrap break-all bg-gray-50 dark:bg-gray-700/50 rounded p-4 max-h-96 overflow-y-auto">
                {showRawPayload ? rawPayload : decodedPayload}
              </pre>
            </div>
          </div>
        </div>
      ),
    },
    {
      id: 'history',
      label: 'Retry History',
      content: (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                Processing Attempts
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                History of all {message.attempts} processing attempts
              </p>
            </div>
            
            <div className="px-6 py-4">
              <div className="flow-root">
                <ul className="-mb-8">
                  {retryHistory.map((attempt, index) => (
                    <li key={index}>
                      <div className="relative pb-8">
                        {index !== retryHistory.length - 1 && (
                          <span
                            className="absolute top-5 left-5 -ml-px h-full w-0.5 bg-gray-200 dark:bg-gray-700"
                            aria-hidden="true"
                          />
                        )}

                        <div className="relative flex items-start space-x-3">
                          <div className="relative px-1">
                            <div className="h-10 w-10 rounded-full bg-white dark:bg-gray-800 border-2 border-gray-300 dark:border-gray-600 flex items-center justify-center ring-8 ring-white dark:ring-gray-800">
                              {getStatusIcon(attempt.attempt)}
                            </div>
                          </div>

                          <div className="min-w-0 flex-1">
                            <div className="flex items-center justify-between">
                              <div>
                                <span className="text-sm font-medium text-gray-900 dark:text-white">
                                  Attempt #{attempt.attempt}
                                </span>
                                {attempt.attempt === message.attempts && (
                                  <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300">
                                    Final Failure
                                  </span>
                                )}
                              </div>
                              <div className="text-sm text-gray-500 dark:text-gray-400">
                                {formatTimestamp(attempt.timestamp)}
                              </div>
                            </div>

                            <div className="mt-2 text-sm text-error-600 dark:text-error-400">
                              {attempt.error}
                            </div>

                            <div className="mt-2 flex items-center space-x-4 text-xs text-gray-500 dark:text-gray-400">
                              <div className="flex items-center space-x-1">
                                <ClockIcon className="w-3 h-3" />
                                <span>Duration: {formatDuration(attempt.duration)}</span>
                              </div>
                              {index > 0 && (
                                <div>
                                  Backoff: {formatDuration(
                                    new Date(attempt.timestamp).getTime() - 
                                    new Date(retryHistory[index - 1].timestamp).getTime()
                                  )}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>
      ),
    },
  ];

  return (
    <Modal 
      isOpen={isOpen} 
      onClose={onClose} 
      title="DLQ Message Details"
      size="2xl"
    >
      <div className="space-y-6">
        {/* Action Buttons */}
        <div className="flex items-center justify-between bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-error-100 dark:bg-error-900/20 rounded-lg flex items-center justify-center">
              <ExclamationTriangleIcon className="w-6 h-6 text-error-600 dark:text-error-400" />
            </div>
            <div>
              <div className="font-medium text-gray-900 dark:text-white">
                Failed Message
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                {message.topic} • Failed after {message.attempts} attempts
              </div>
            </div>
          </div>
          
          <div className="flex items-center space-x-3">
            {onReplay && (
              <button
                onClick={handleReplay}
                disabled={replayLoading}
                className="btn btn-primary inline-flex items-center"
              >
                {replayLoading ? (
                  <>
                    <div className="loading-spinner h-4 w-4 mr-2" />
                    Replaying...
                  </>
                ) : (
                  <>
                    <ArrowPathIcon className="w-4 h-4 mr-2" />
                    Replay Message
                  </>
                )}
              </button>
            )}
            
            {onDelete && (
              <button
                onClick={handleDelete}
                disabled={deleteLoading}
                className="btn btn-danger inline-flex items-center"
              >
                {deleteLoading ? (
                  <>
                    <div className="loading-spinner h-4 w-4 mr-2" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <TrashIcon className="w-4 h-4 mr-2" />
                    Delete
                  </>
                )}
              </button>
            )}
          </div>
        </div>

        {/* Tabs */}
        <Tabs 
          tabs={tabs} 
          defaultTab="overview" 
        />
      </div>
    </Modal>
  );
}
