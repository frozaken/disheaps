import { Link } from 'react-router-dom';
import { 
  QueueListIcon, 
  ServerIcon,
  ClockIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  EllipsisVerticalIcon,
} from '@heroicons/react/24/outline';
import { Menu } from '@headlessui/react';
import type { HeapInfo } from '../../lib/types';
import { formatDuration, formatBytes } from '../../lib/validation';

interface HeapCardProps {
  heap: HeapInfo;
  onEdit?: () => void;
  onDelete?: () => void;
  onPurge?: () => void;
}

function formatNumber(num: number): string {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
}

function getHealthStatus(heap: HeapInfo): { status: 'healthy' | 'warning' | 'error'; message: string } {
  const { stats } = heap;
  
  // Check if any partitions are unhealthy
  const unhealthyPartitions = Object.values(stats.partition_stats || {}).filter(p => !p.is_healthy);
  if (unhealthyPartitions.length > 0) {
    return { 
      status: 'error', 
      message: `${unhealthyPartitions.length} partition${unhealthyPartitions.length > 1 ? 's' : ''} unhealthy` 
    };
  }
  
  // Check for high DLQ message count
  if (stats.dlq_messages > stats.total_messages * 0.1) {
    return { 
      status: 'warning', 
      message: 'High DLQ message count' 
    };
  }
  
  // Check for high retry rate
  const totalOps = stats.total_pops + stats.total_acks + stats.total_nacks;
  if (totalOps > 0 && stats.total_retries / totalOps > 0.2) {
    return { 
      status: 'warning', 
      message: 'High retry rate' 
    };
  }
  
  return { status: 'healthy', message: 'All systems normal' };
}

export function HeapCard({ heap, onEdit, onDelete, onPurge }: HeapCardProps) {
  const health = getHealthStatus(heap);
  
  const healthClasses = {
    healthy: 'status-healthy',
    warning: 'status-warning',
    error: 'status-unhealthy',
  };

  const modeIcon = heap.mode === 'MIN' ? ArrowTrendingDownIcon : ArrowTrendingUpIcon;
  const ModeIcon = modeIcon;

  return (
    <div className="card hover:shadow-md transition-shadow duration-200">
      {/* Header */}
      <div className="p-6 pb-4">
        <div className="flex items-start justify-between">
          <div className="flex items-center space-x-3">
            <div className="flex-shrink-0">
              <div className="w-10 h-10 bg-primary-100 dark:bg-primary-900/20 rounded-lg flex items-center justify-center">
                <QueueListIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
              </div>
            </div>
            <div>
              <Link
                to={`/heaps/${encodeURIComponent(heap.topic)}`}
                className="text-lg font-semibold text-gray-900 dark:text-white hover:text-primary-600 dark:hover:text-primary-400"
              >
                {heap.topic}
              </Link>
              <div className="flex items-center space-x-2 mt-1">
                <span className={`status-indicator ${healthClasses[health.status]}`}>
                  {health.status === 'healthy' && '●'}
                  {health.status === 'warning' && '▲'}
                  {health.status === 'error' && '✕'}
                  {health.status.charAt(0).toUpperCase() + health.status.slice(1)}
                </span>
                <span className="text-sm text-gray-500 dark:text-gray-400">
                  {health.message}
                </span>
              </div>
            </div>
          </div>

          {/* Actions Menu */}
          {(onEdit || onDelete || onPurge) && (
            <Menu as="div" className="relative">
              <Menu.Button className="flex items-center justify-center w-8 h-8 rounded-md text-gray-400 hover:text-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 dark:hover:text-gray-300">
                <EllipsisVerticalIcon className="w-5 h-5" />
              </Menu.Button>
              <Menu.Items className="absolute right-0 z-10 mt-2 w-48 origin-top-right rounded-md bg-white dark:bg-gray-800 shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
                <div className="py-1">
                  {onEdit && (
                    <Menu.Item>
                      {({ active }) => (
                        <button
                          onClick={onEdit}
                          className={`${
                            active ? 'bg-gray-100 dark:bg-gray-700' : ''
                          } group flex w-full items-center px-4 py-2 text-sm text-gray-700 dark:text-gray-300`}
                        >
                          Edit Configuration
                        </button>
                      )}
                    </Menu.Item>
                  )}
                  {onPurge && (
                    <Menu.Item>
                      {({ active }) => (
                        <button
                          onClick={onPurge}
                          className={`${
                            active ? 'bg-gray-100 dark:bg-gray-700' : ''
                          } group flex w-full items-center px-4 py-2 text-sm text-warning-600 dark:text-warning-400`}
                        >
                          Purge Messages
                        </button>
                      )}
                    </Menu.Item>
                  )}
                  {onDelete && (
                    <Menu.Item>
                      {({ active }) => (
                        <button
                          onClick={onDelete}
                          className={`${
                            active ? 'bg-gray-100 dark:bg-gray-700' : ''
                          } group flex w-full items-center px-4 py-2 text-sm text-error-600 dark:text-error-400`}
                        >
                          Delete Heap
                        </button>
                      )}
                    </Menu.Item>
                  )}
                </div>
              </Menu.Items>
            </Menu>
          )}
        </div>
      </div>

      {/* Configuration Info */}
      <div className="px-6 pb-4">
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div className="flex items-center space-x-2">
            <ModeIcon className="w-4 h-4 text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Mode:</span>
            <span className="font-medium text-gray-900 dark:text-white">
              {heap.mode}
            </span>
          </div>
          <div className="flex items-center space-x-2">
            <ServerIcon className="w-4 h-4 text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Partitions:</span>
            <span className="font-medium text-gray-900 dark:text-white">
              {heap.partitions}
            </span>
          </div>
          <div className="flex items-center space-x-2">
            <ClockIcon className="w-4 h-4 text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Retention:</span>
            <span className="font-medium text-gray-900 dark:text-white">
              {formatDuration(heap.retention_time)}
            </span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="w-4 h-4 text-center text-gray-400 font-bold">RF</span>
            <span className="text-gray-600 dark:text-gray-400">RF:</span>
            <span className="font-medium text-gray-900 dark:text-white">
              {heap.replication_factor}
            </span>
          </div>
        </div>
      </div>

      {/* Statistics */}
      <div className="px-6 pb-6">
        <div className="grid grid-cols-4 gap-4">
          {/* Total Messages */}
          <div className="text-center">
            <div className="text-lg font-semibold text-gray-900 dark:text-white">
              {formatNumber(heap.stats.total_messages)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">
              Messages
            </div>
          </div>
          
          {/* In Flight */}
          <div className="text-center">
            <div className="text-lg font-semibold text-warning-600 dark:text-warning-400">
              {formatNumber(heap.stats.inflight_messages)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">
              In Flight
            </div>
          </div>
          
          {/* DLQ Messages */}
          <div className="text-center">
            <div className="text-lg font-semibold text-error-600 dark:text-error-400">
              {formatNumber(heap.stats.dlq_messages)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">
              DLQ
            </div>
          </div>
          
          {/* Total Operations */}
          <div className="text-center">
            <div className="text-lg font-semibold text-success-600 dark:text-success-400">
              {formatNumber(heap.stats.total_enqueues + heap.stats.total_pops)}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">
              Total Ops
            </div>
          </div>
        </div>
      </div>

      {/* Footer with additional info */}
      <div className="px-6 py-3 bg-white dark:bg-gray-700/50 rounded-b-lg">
        <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400">
          <div>
            Max payload: {formatBytes(heap.max_payload_bytes)}
          </div>
          <div>
            {heap.compression_enabled && (
              <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-300">
                Compression ON
              </span>
            )}
            {heap.dlq_policy.enabled && (
              <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300 ml-2">
                DLQ Enabled
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
