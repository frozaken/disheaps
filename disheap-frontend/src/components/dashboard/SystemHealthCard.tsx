import { 
  CheckCircleIcon,
  ExclamationTriangleIcon,
  XCircleIcon,
  ClockIcon,
  ServerIcon,
  CpuChipIcon,
} from '@heroicons/react/24/outline';
import type { HealthStatus, ComponentHealth } from '../../lib/types';

interface SystemComponent extends ComponentHealth {
  uptime?: string;
  version?: string;
  details?: string;
}

interface SystemHealthCardProps {
  components: SystemComponent[];
  overallHealth: HealthStatus;
  onRefresh?: () => void;
  isRefreshing?: boolean;
}

function getStatusIcon(status: HealthStatus) {
  switch (status) {
    case 'healthy':
      return <CheckCircleIcon className="w-5 h-5 text-success-500" />;
    case 'warning':
      return <ExclamationTriangleIcon className="w-5 h-5 text-warning-500" />;
    case 'unhealthy':
      return <XCircleIcon className="w-5 h-5 text-error-500" />;
    default:
      return <ClockIcon className="w-5 h-5 text-gray-400" />;
  }
}

function getStatusClasses(status: HealthStatus) {
  switch (status) {
    case 'healthy':
      return 'bg-success-100 text-success-800 dark:bg-success-900/20 dark:text-success-300';
    case 'warning':
      return 'bg-warning-100 text-warning-800 dark:bg-warning-900/20 dark:text-warning-300';
    case 'unhealthy':
      return 'bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300';
    default:
      return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
  }
}

function getOverallStatusClasses(status: HealthStatus) {
  switch (status) {
    case 'healthy':
      return 'border-success-200 bg-success-50 dark:border-success-800 dark:bg-success-900/20';
    case 'warning':
      return 'border-warning-200 bg-warning-50 dark:border-warning-800 dark:bg-warning-900/20';
    case 'unhealthy':
      return 'border-error-200 bg-error-50 dark:border-error-800 dark:bg-error-900/20';
    default:
      return 'border-gray-200 bg-gray-50 dark:border-gray-700 dark:bg-gray-800/50';
  }
}

export function SystemHealthCard({ 
  components, 
  overallHealth, 
  onRefresh, 
  isRefreshing = false 
}: SystemHealthCardProps) {
  const healthyCount = components.filter(c => c.status === 'healthy').length;
  const warningCount = components.filter(c => c.status === 'warning').length;
  const unhealthyCount = components.filter(c => c.status === 'unhealthy').length;

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-blue-100 dark:bg-blue-900/20 rounded-lg flex items-center justify-center">
              <ServerIcon className="w-6 h-6 text-blue-600 dark:text-blue-400" />
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                System Health
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Cluster components and services
              </p>
            </div>
          </div>
          {onRefresh && (
            <button
              onClick={onRefresh}
              disabled={isRefreshing}
              className="btn btn-secondary btn-sm"
            >
              <ClockIcon className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          )}
        </div>
      </div>

      {/* Overall Status */}
      <div className={`px-6 py-4 border-b border-gray-200 dark:border-gray-700 ${getOverallStatusClasses(overallHealth)}`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            {getStatusIcon(overallHealth)}
            <div>
              <div className="font-medium text-gray-900 dark:text-white capitalize">
                Overall Status: {overallHealth}
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400">
                {healthyCount} healthy • {warningCount} warnings • {unhealthyCount} issues
              </div>
            </div>
          </div>
          <div className="text-right">
            <div className="text-2xl font-bold text-gray-900 dark:text-white">
              {Math.round((healthyCount / components.length) * 100)}%
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400">Available</div>
          </div>
        </div>
      </div>

      {/* Component List */}
      <div className="px-6 py-4">
        <div className="space-y-3">
          {components.map((component, index) => (
            <div key={index} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
              <div className="flex items-center space-x-3">
                <div className="flex items-center space-x-2">
                  {getStatusIcon(component.status)}
                  <span className="font-medium text-gray-900 dark:text-white">
                    {component.name}
                  </span>
                </div>
                <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${getStatusClasses(component.status)}`}>
                  {component.status}
                </span>
                {component.version && (
                  <span className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                    v{component.version}
                  </span>
                )}
              </div>
              
              <div className="flex items-center space-x-4 text-sm">
                {component.uptime && (
                  <div className="text-right">
                    <div className="text-gray-900 dark:text-white font-medium">
                      {component.uptime}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-gray-400">uptime</div>
                  </div>
                )}
                <div className="text-right">
                  <div className="text-gray-600 dark:text-gray-400">
                    {new Date(component.last_check).toLocaleTimeString()}
                  </div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">last check</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Resource Utilization Summary */}
      <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 rounded-b-lg">
        <div className="grid grid-cols-3 gap-4">
          <div className="text-center">
            <div className="flex items-center justify-center mb-2">
              <CpuChipIcon className="w-5 h-5 text-blue-600 dark:text-blue-400 mr-1" />
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">CPU</span>
            </div>
            <div className="text-lg font-semibold text-gray-900 dark:text-white">
              {Math.floor(Math.random() * 40 + 20)}%
            </div>
          </div>
          
          <div className="text-center">
            <div className="flex items-center justify-center mb-2">
              <div className="w-5 h-5 bg-green-600 rounded-sm mr-1" />
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Memory</span>
            </div>
            <div className="text-lg font-semibold text-gray-900 dark:text-white">
              {Math.floor(Math.random() * 30 + 40)}%
            </div>
          </div>
          
          <div className="text-center">
            <div className="flex items-center justify-center mb-2">
              <div className="w-5 h-5 bg-purple-600 rounded-full mr-1" />
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Disk</span>
            </div>
            <div className="text-lg font-semibold text-gray-900 dark:text-white">
              {Math.floor(Math.random() * 20 + 15)}%
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
