import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useWebSocket } from '../../lib/websocket';
import { api } from '../../lib/api';
import {
  PlusIcon,
  QueueListIcon,
  ExclamationTriangleIcon,
  KeyIcon,
  Cog8ToothIcon,
  ArrowPathIcon,
  BoltIcon,
  ShieldCheckIcon,
} from '@heroicons/react/24/outline';
import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { useAuth } from '../../store/auth';
import { useNotifications } from '../../store/app';
import { SystemHealthCard } from '../../components/dashboard/SystemHealthCard';
import { ClusterMetrics } from '../../components/dashboard/ClusterMetrics';
import { ActivityFeed } from '../../components/dashboard/ActivityFeed';
import { CreateHeapModal } from '../../components/heaps/CreateHeapModal';
import type { HealthStatus, ComponentHealth } from '../../lib/types';

// Mock system components data
function generateSystemComponents(): ComponentHealth[] {
  const components = [
    {
      name: 'API Gateway',
      status: 'healthy' as HealthStatus,
      uptime: '15d 4h 23m',
      version: '2.1.4',
      last_check: new Date(Date.now() - 30000).toISOString(), // 30 seconds ago
    },
    {
      name: 'Engine Cluster',
      status: 'healthy' as HealthStatus,
      uptime: '12d 18h 45m',
      version: '1.8.2',
      last_check: new Date(Date.now() - 15000).toISOString(), // 15 seconds ago
    },
    {
      name: 'Raft Consensus',
      status: Math.random() > 0.8 ? 'warning' as HealthStatus : 'healthy' as HealthStatus,
      uptime: '12d 18h 45m',
      version: '1.3.1',
      last_check: new Date(Date.now() - 45000).toISOString(), // 45 seconds ago
    },
    {
      name: 'Storage Layer',
      status: 'healthy' as HealthStatus,
      uptime: '25d 12h 8m',
      version: '3.2.1',
      last_check: new Date(Date.now() - 20000).toISOString(), // 20 seconds ago
    },
    {
      name: 'Monitoring',
      status: 'healthy' as HealthStatus,
      uptime: '8d 6h 12m',
      version: '1.9.3',
      last_check: new Date(Date.now() - 10000).toISOString(), // 10 seconds ago
    },
  ];

  // Occasionally simulate a component issue
  if (Math.random() > 0.95) {
    components[Math.floor(Math.random() * components.length)].status = 'unhealthy';
  }

  return components;
}

function getOverallHealth(components: ComponentHealth[]): HealthStatus {
  const unhealthy = components.some(c => c.status === 'unhealthy');
  const warnings = components.some(c => c.status === 'warning');
  
  if (unhealthy) return 'unhealthy';
  if (warnings) return 'warning';
  return 'healthy';
}

export function ClusterDashboard() {
  useDocumentTitle('Cluster Overview');
  const { user } = useAuth();
  const { showSuccess, showInfo, showError } = useNotifications();
  const [refreshing, setRefreshing] = useState(false);
  const [showCreateHeap, setShowCreateHeap] = useState(false);
  const [systemComponents, setSystemComponents] = useState<ComponentHealth[]>([]);
  const [lastRefresh, setLastRefresh] = useState(new Date());
  const [wsStatus, setWsStatus] = useState<'connected' | 'connecting' | 'disconnected'>('disconnected');
  
  const { subscribe, getConnectionStatus } = useWebSocket();

  // Load initial system components data
  useEffect(() => {
    loadSystemHealth();
    setWsStatus(getConnectionStatus());
  }, []);

  // Set up WebSocket subscriptions for real-time updates
  useEffect(() => {
    const unsubscribeHealth = subscribe('system_health', (healthData: ComponentHealth[]) => {
      setSystemComponents(healthData);
      setLastRefresh(new Date());
    });

    const unsubscribeMetrics = subscribe('cluster_metrics', (metricsData: any) => {
      // Update metrics data in real-time
      setLastRefresh(new Date());
    });

    const unsubscribeHeartbeat = subscribe('heartbeat', (data: any) => {
      if (data.status === 'connected') {
        setWsStatus('connected');
      }
    });

    const unsubscribeError = subscribe('error', (error: any) => {
      console.warn('WebSocket error:', error);
      setWsStatus('disconnected');
    });

    // Check connection status periodically
    const statusInterval = setInterval(() => {
      setWsStatus(getConnectionStatus());
    }, 5000);

    return () => {
      unsubscribeHealth();
      unsubscribeMetrics();
      unsubscribeHeartbeat();
      unsubscribeError();
      clearInterval(statusInterval);
    };
  }, [subscribe, getConnectionStatus]);

  const loadSystemHealth = async () => {
    try {
      const components = await api.getSystemHealth();
      setSystemComponents(components);
    } catch (error) {
      console.error('Failed to load system health:', error);
      // Fallback to mock data
      setSystemComponents(generateSystemComponents());
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    
    try {
      await loadSystemHealth();
      setLastRefresh(new Date());
      showInfo('Refreshed', 'Dashboard data has been updated');
    } catch (error) {
      showError('Refresh Failed', 'Unable to refresh dashboard data');
    } finally {
      setRefreshing(false);
    }
  };

  const handleQuickAction = (action: string) => {
    switch (action) {
      case 'create-heap':
        setShowCreateHeap(true);
        break;
      case 'system-maintenance':
        showInfo('Maintenance', 'System maintenance features coming soon');
        break;
      default:
        break;
    }
  };

  const overallHealth = getOverallHealth(systemComponents);
  const healthyCount = systemComponents.filter(c => c.status === 'healthy').length;

  return (
    <div className="space-y-6">
      {/* Header with Welcome */}
      <div className="bg-gradient-to-r from-primary-600 to-primary-700 rounded-lg shadow-sm overflow-hidden">
        <div className="px-6 py-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-white">
                Welcome back, {user?.name?.split(' ')[0] || 'User'}! 👋
              </h1>
              <p className="mt-2 text-primary-100">
                Your Disheap cluster is {overallHealth} with {healthyCount}/{systemComponents.length} components operational
              </p>
              <p className="mt-1 text-primary-200 text-sm">
                Last updated: {lastRefresh.toLocaleTimeString()}
              </p>
            </div>
            <div className="flex items-center space-x-3">
              <div className="flex items-center space-x-4">
                <div className="flex items-center space-x-2 text-primary-100 text-sm">
                  <div className={`w-2 h-2 rounded-full ${
                    wsStatus === 'connected' ? 'bg-green-400 animate-pulse' : 
                    wsStatus === 'connecting' ? 'bg-yellow-400' : 
                    'bg-red-400'
                  }`} />
                  <span className="capitalize">{wsStatus === 'connected' ? 'Live' : wsStatus}</span>
                </div>
                <button
                  onClick={handleRefresh}
                  disabled={refreshing}
                  className="inline-flex items-center px-4 py-2 border border-primary-500 text-primary-100 bg-primary-600/50 hover:bg-primary-600/70 rounded-md transition-colors disabled:opacity-50"
                >
                  <ArrowPathIcon className={`w-5 h-5 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
                  {refreshing ? 'Refreshing...' : 'Refresh All'}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Quick Actions</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400">Common tasks and shortcuts</p>
          </div>
        </div>
        
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-4">
          <button
            onClick={() => handleQuickAction('create-heap')}
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <PlusIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              Create Heap
            </span>
          </button>

          <Link
            to="/heaps"
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <QueueListIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              Browse Heaps
            </span>
          </Link>

          <Link
            to="/dlq"
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <ExclamationTriangleIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              DLQ Browser
            </span>
          </Link>

          <Link
            to="/keys"
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <KeyIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              API Keys
            </span>
          </Link>

          <Link
            to="/settings"
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <Cog8ToothIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              Settings
            </span>
          </Link>

          <button
            onClick={() => handleQuickAction('system-maintenance')}
            className="flex flex-col items-center p-4 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg hover:border-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/10 transition-colors group"
          >
            <BoltIcon className="w-8 h-8 text-gray-400 group-hover:text-primary-500 mb-2" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 group-hover:text-primary-600 dark:group-hover:text-primary-400">
              Maintenance
            </span>
          </button>
        </div>
      </div>

      {/* System Health and Activity Feed */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <SystemHealthCard
          components={systemComponents}
          overallHealth={overallHealth}
          onRefresh={() => setSystemComponents(generateSystemComponents())}
          isRefreshing={refreshing}
        />
        <ActivityFeed 
          maxItems={12}
          onRefresh={handleRefresh}
          isRefreshing={refreshing}
        />
      </div>

      {/* Cluster Metrics */}
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Cluster Performance
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Real-time metrics and system performance indicators
            </p>
          </div>
        </div>
        <ClusterMetrics 
          onRefresh={handleRefresh}
          isRefreshing={refreshing}
        />
      </div>

      {/* System Status Footer */}
      <div className="bg-gray-50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <ShieldCheckIcon className="w-5 h-5 text-success-600 dark:text-success-400" />
              <span className="text-sm font-medium text-gray-900 dark:text-white">
                System Status: All services operational
              </span>
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Cluster ID: cluster-prod-us-west-2a
            </div>
          </div>
          
          <div className="flex items-center space-x-4 text-sm text-gray-500 dark:text-gray-400">
            <span>Region: US West 2</span>
            <span>•</span>
            <span>Version: 2.1.4</span>
            <span>•</span>
            <span>Uptime: 15d 4h</span>
          </div>
        </div>
      </div>

      {/* Create Heap Modal */}
      <CreateHeapModal
        isOpen={showCreateHeap}
        onClose={() => setShowCreateHeap(false)}
        onSuccess={() => {
          setShowCreateHeap(false);
          showSuccess('Heap Created', 'Successfully created new heap');
        }}
      />
    </div>
  );
}
