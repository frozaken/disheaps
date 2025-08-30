import { useState, useEffect } from 'react';
import {
  ClockIcon,
  UserIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  InformationCircleIcon,
  XCircleIcon,
  ServerIcon,
  KeyIcon,
  TrashIcon,
  PlusIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline';

type ActivityType = 
  | 'heap_created' 
  | 'heap_deleted' 
  | 'heap_purged'
  | 'api_key_created'
  | 'api_key_revoked'
  | 'user_login'
  | 'system_alert'
  | 'dlq_replay'
  | 'config_updated'
  | 'health_check'
  | 'error';

interface Activity {
  id: string;
  type: ActivityType;
  title: string;
  description: string;
  timestamp: string;
  user?: string;
  metadata?: Record<string, any>;
  severity?: 'info' | 'warning' | 'error' | 'success';
}

interface ActivityFeedProps {
  maxItems?: number;
  onRefresh?: () => void;
  isRefreshing?: boolean;
}

function generateMockActivities(): Activity[] {
  const users = ['alice@company.com', 'bob@company.com', 'charlie@company.com', 'system'];
  const heaps = ['user-events', 'payment-processing', 'email-notifications', 'analytics-events', 'audit-logs'];
  
  const activities: Activity[] = [];
  const now = Date.now();

  // Generate 25 activities over the last 24 hours
  for (let i = 0; i < 25; i++) {
    const timestamp = new Date(now - Math.random() * 24 * 60 * 60 * 1000);
    const user = users[Math.floor(Math.random() * users.length)];
    const heap = heaps[Math.floor(Math.random() * heaps.length)];
    
    const activityTypes: { type: ActivityType; templates: Array<{ title: string; description: string; severity?: 'info' | 'warning' | 'error' | 'success' }> }[] = [
      {
        type: 'heap_created',
        templates: [
          { title: `Heap "${heap}" created`, description: `New heap with MIN mode and 8 partitions`, severity: 'success' },
        ]
      },
      {
        type: 'heap_deleted', 
        templates: [
          { title: `Heap "${heap}" deleted`, description: `Heap and all messages permanently removed`, severity: 'warning' },
        ]
      },
      {
        type: 'heap_purged',
        templates: [
          { title: `Heap "${heap}" purged`, description: `All messages cleared from heap`, severity: 'warning' },
        ]
      },
      {
        type: 'api_key_created',
        templates: [
          { title: 'New API key created', description: `API key "Production API" created with no expiration`, severity: 'info' },
        ]
      },
      {
        type: 'api_key_revoked',
        templates: [
          { title: 'API key revoked', description: `API key "Development Testing" has been revoked`, severity: 'warning' },
        ]
      },
      {
        type: 'user_login',
        templates: [
          { title: 'User signed in', description: `Successful login from ${['Chrome', 'Firefox', 'Safari'][Math.floor(Math.random() * 3)]}`, severity: 'info' },
        ]
      },
      {
        type: 'system_alert',
        templates: [
          { title: 'High memory usage detected', description: `Node ${Math.floor(Math.random() * 4) + 1} memory usage at 85%`, severity: 'warning' },
          { title: 'DLQ threshold exceeded', description: `Heap "${heap}" has 150+ messages in DLQ`, severity: 'error' },
          { title: 'Connection limit reached', description: 'Maximum concurrent connections reached (500)', severity: 'error' },
        ]
      },
      {
        type: 'dlq_replay',
        templates: [
          { title: 'DLQ messages replayed', description: `${Math.floor(Math.random() * 50 + 10)} messages replayed from "${heap}" DLQ`, severity: 'success' },
        ]
      },
      {
        type: 'config_updated',
        templates: [
          { title: `Heap "${heap}" configured`, description: 'Updated retention time and visibility timeout settings', severity: 'info' },
        ]
      },
      {
        type: 'health_check',
        templates: [
          { title: 'Health check passed', description: 'All system components are healthy', severity: 'success' },
          { title: 'Health check warning', description: `Node ${Math.floor(Math.random() * 4) + 1} response time elevated`, severity: 'warning' },
        ]
      },
      {
        type: 'error',
        templates: [
          { title: 'Connection timeout', description: `Client connection timeout after 30s`, severity: 'error' },
          { title: 'Authentication failure', description: 'Invalid API key used for request', severity: 'error' },
        ]
      },
    ];

    const activityType = activityTypes[Math.floor(Math.random() * activityTypes.length)];
    const template = activityType.templates[Math.floor(Math.random() * activityType.templates.length)];

    activities.push({
      id: `activity_${i}`,
      type: activityType.type,
      title: template.title,
      description: template.description,
      timestamp: timestamp.toISOString(),
      user: activityType.type.includes('system') || activityType.type.includes('health') ? 'system' : user,
      severity: template.severity || 'info',
      metadata: {
        heap: activityType.type.includes('heap') ? heap : undefined,
      }
    });
  }

  return activities.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
}

function getActivityIcon(type: ActivityType) {
  switch (type) {
    case 'heap_created':
      return <PlusIcon className="w-5 h-5" />;
    case 'heap_deleted':
      return <TrashIcon className="w-5 h-5" />;
    case 'heap_purged':
      return <XCircleIcon className="w-5 h-5" />;
    case 'api_key_created':
      return <KeyIcon className="w-5 h-5" />;
    case 'api_key_revoked':
      return <XCircleIcon className="w-5 h-5" />;
    case 'user_login':
      return <UserIcon className="w-5 h-5" />;
    case 'system_alert':
      return <ExclamationTriangleIcon className="w-5 h-5" />;
    case 'dlq_replay':
      return <ArrowPathIcon className="w-5 h-5" />;
    case 'config_updated':
      return <InformationCircleIcon className="w-5 h-5" />;
    case 'health_check':
      return <CheckCircleIcon className="w-5 h-5" />;
    case 'error':
      return <XCircleIcon className="w-5 h-5" />;
    default:
      return <InformationCircleIcon className="w-5 h-5" />;
  }
}

function getSeverityClasses(severity: Activity['severity'] = 'info') {
  switch (severity) {
    case 'success':
      return 'text-success-600 dark:text-success-400 bg-success-100 dark:bg-success-900/20';
    case 'warning':
      return 'text-warning-600 dark:text-warning-400 bg-warning-100 dark:bg-warning-900/20';
    case 'error':
      return 'text-error-600 dark:text-error-400 bg-error-100 dark:bg-error-900/20';
    default:
      return 'text-blue-600 dark:text-blue-400 bg-blue-100 dark:bg-blue-900/20';
  }
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  
  if (diffMs < 60 * 1000) {
    return 'Just now';
  } else if (diffMs < 60 * 60 * 1000) {
    const minutes = Math.floor(diffMs / (60 * 1000));
    return `${minutes}m ago`;
  } else if (diffMs < 24 * 60 * 60 * 1000) {
    const hours = Math.floor(diffMs / (60 * 60 * 1000));
    return `${hours}h ago`;
  } else {
    return date.toLocaleDateString();
  }
}

export function ActivityFeed({ maxItems = 15, onRefresh, isRefreshing = false }: ActivityFeedProps) {
  const [activities, setActivities] = useState<Activity[]>([]);

  useEffect(() => {
    setActivities(generateMockActivities().slice(0, maxItems));
  }, [maxItems]);

  const handleRefresh = () => {
    setActivities(generateMockActivities().slice(0, maxItems));
    onRefresh?.();
  };

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-green-100 dark:bg-green-900/20 rounded-lg flex items-center justify-center">
              <ClockIcon className="w-6 h-6 text-green-600 dark:text-green-400" />
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                Recent Activity
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                System events and user actions
              </p>
            </div>
          </div>
          <button
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="btn btn-secondary btn-sm"
          >
            <ArrowPathIcon className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Activity List */}
      <div className="px-6 py-4">
        <div className="flow-root">
          <ul className="-mb-8">
            {activities.map((activity, index) => (
              <li key={activity.id}>
                <div className="relative pb-8">
                  {/* Connecting line */}
                  {index !== activities.length - 1 && (
                    <span
                      className="absolute top-5 left-5 -ml-px h-full w-0.5 bg-gray-200 dark:bg-gray-700"
                      aria-hidden="true"
                    />
                  )}

                  <div className="relative flex items-start space-x-3">
                    {/* Activity Icon */}
                    <div className={`relative px-1`}>
                      <div className={`h-10 w-10 rounded-full flex items-center justify-center ring-8 ring-white dark:ring-gray-800 ${getSeverityClasses(activity.severity)}`}>
                        {getActivityIcon(activity.type)}
                      </div>
                    </div>

                    {/* Activity Content */}
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center justify-between">
                        <div className="text-sm">
                          <span className="font-medium text-gray-900 dark:text-white">
                            {activity.title}
                          </span>
                        </div>
                        <div className="text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap">
                          {formatTimestamp(activity.timestamp)}
                        </div>
                      </div>

                      <div className="mt-1 text-sm text-gray-600 dark:text-gray-400">
                        {activity.description}
                      </div>

                      {/* User and metadata */}
                      <div className="mt-2 flex items-center space-x-4 text-xs text-gray-500 dark:text-gray-400">
                        {activity.user && activity.user !== 'system' && (
                          <div className="flex items-center space-x-1">
                            <UserIcon className="w-3 h-3" />
                            <span>{activity.user}</span>
                          </div>
                        )}
                        {activity.user === 'system' && (
                          <div className="flex items-center space-x-1">
                            <ServerIcon className="w-3 h-3" />
                            <span>System</span>
                          </div>
                        )}
                        {activity.metadata?.heap && (
                          <div className="flex items-center space-x-1">
                            <div className="w-2 h-2 bg-primary-500 rounded-full" />
                            <span>{activity.metadata.heap}</span>
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

      {/* Footer */}
      <div className="px-6 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 rounded-b-lg">
        <div className="flex items-center justify-between text-sm">
          <span className="text-gray-500 dark:text-gray-400">
            Showing {activities.length} recent activities
          </span>
          <button className="text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300 font-medium">
            View all activity
          </button>
        </div>
      </div>
    </div>
  );
}
