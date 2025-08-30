import { 
  ClockIcon, 
  ServerIcon, 
  CpuChipIcon,
  DocumentTextIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/outline';
import type { HeapInfo } from '../../lib/types';
import { formatBytes, formatDuration } from '../../lib/validation';

interface HeapOverviewTabProps {
  heap: HeapInfo;
  onRefresh?: () => void;
  isRefreshing?: boolean;
}

function StatusBadge({ status }: { status: 'healthy' | 'warning' | 'error' }) {
  const configs = {
    healthy: {
      icon: CheckCircleIcon,
      classes: 'bg-success-100 text-success-800 dark:bg-success-900/20 dark:text-success-400',
      label: 'Healthy'
    },
    warning: {
      icon: ExclamationTriangleIcon,
      classes: 'bg-warning-100 text-warning-800 dark:bg-warning-900/20 dark:text-warning-400',
      label: 'Warning'
    },
    error: {
      icon: ExclamationTriangleIcon,
      classes: 'bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-400',
      label: 'Error'
    }
  };

  const config = configs[status];
  const Icon = config.icon;

  return (
    <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${config.classes}`}>
      <Icon className="w-4 h-4 mr-1.5" />
      {config.label}
    </span>
  );
}

export function HeapOverviewTab({ heap, onRefresh, isRefreshing = false }: HeapOverviewTabProps) {
  const ModeIcon = heap.mode === 'MIN' ? ArrowTrendingDownIcon : ArrowTrendingUpIcon;
  
  // Calculate health status
  const getHealthStatus = (): 'healthy' | 'warning' | 'error' => {
    const { stats } = heap;
    
    // Check partition health
    const unhealthyPartitions = Object.values(stats.partition_stats || {}).filter(p => !p.is_healthy);
    if (unhealthyPartitions.length > 0) {
      return 'error';
    }
    
    // Check for high DLQ rate
    if (stats.dlq_messages > stats.total_messages * 0.1) {
      return 'warning';
    }
    
    // Check for high retry rate
    const totalOps = stats.total_pops + stats.total_acks + stats.total_nacks;
    if (totalOps > 0 && stats.total_retries / totalOps > 0.2) {
      return 'warning';
    }
    
    return 'healthy';
  };

  const healthStatus = getHealthStatus();

  return (
    <div className="space-y-6">
      {/* Status and Quick Actions */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <StatusBadge status={healthStatus} />
          <div className="text-sm text-gray-500 dark:text-gray-400">
            Last updated: {new Date().toLocaleString()}
          </div>
        </div>
        
        <button
          onClick={onRefresh}
          disabled={isRefreshing}
          className="btn btn-secondary"
        >
          {isRefreshing && <div className="loading-spinner h-4 w-4 mr-2" />}
          Refresh
        </button>
      </div>

      {/* Basic Configuration */}
      <div className="card">
        <div className="card-header">
          <h3 className="card-title">Configuration</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div className="flex items-center space-x-3">
              <ModeIcon className="w-8 h-8 text-primary-600 dark:text-primary-400" />
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Mode</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {heap.mode} Heap
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <ServerIcon className="w-8 h-8 text-primary-600 dark:text-primary-400" />
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Partitions</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {heap.partitions} partitions
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <CpuChipIcon className="w-8 h-8 text-primary-600 dark:text-primary-400" />
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Replication</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  RF: {heap.replication_factor}
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <ClockIcon className="w-8 h-8 text-primary-600 dark:text-primary-400" />
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Retention</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {formatDuration(heap.retention_time)}
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <DocumentTextIcon className="w-8 h-8 text-primary-600 dark:text-primary-400" />
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Max Payload</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {formatBytes(heap.max_payload_bytes)}
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <div className="w-8 h-8 flex items-center justify-center bg-primary-100 dark:bg-primary-900/20 rounded-lg">
                <span className="text-sm font-bold text-primary-600 dark:text-primary-400">K</span>
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900 dark:text-white">Top-K Bound</div>
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {heap.top_k_bound}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Message Statistics */}
      <div className="card">
        <div className="card-header">
          <h3 className="card-title">Message Statistics</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900 dark:text-white">
                {heap.stats.total_messages.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Messages</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600 dark:text-warning-400">
                {heap.stats.inflight_messages.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">In Flight</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-error-600 dark:text-error-400">
                {heap.stats.dlq_messages.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">DLQ Messages</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600 dark:text-success-400">
                {((heap.stats.total_acks / Math.max(heap.stats.total_pops, 1)) * 100).toFixed(1)}%
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Success Rate</div>
            </div>
          </div>
        </div>
      </div>

      {/* Operation Statistics */}
      <div className="card">
        <div className="card-header">
          <h3 className="card-title">Operation Statistics</h3>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                {heap.stats.total_enqueues.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Enqueues</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                {heap.stats.total_pops.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Pops</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600 dark:text-success-400">
                {heap.stats.total_acks.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Acks</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600 dark:text-warning-400">
                {heap.stats.total_retries.toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Retries</div>
            </div>
          </div>
        </div>
      </div>

      {/* Features */}
      <div className="card">
        <div className="card-header">
          <h3 className="card-title">Features</h3>
        </div>
        <div className="p-6">
          <div className="flex flex-wrap gap-2">
            {heap.compression_enabled && (
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-300">
                Compression Enabled
              </span>
            )}
            
            {heap.dlq_policy.enabled && (
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300">
                DLQ Enabled ({formatDuration(heap.dlq_policy.retention_time)})
              </span>
            )}
            
            <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">
              Visibility Timeout: {formatDuration(heap.visibility_timeout_default)}
            </span>
            
            <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">
              Max Retries: {heap.max_retries_default}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
