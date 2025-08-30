import { useState, useEffect } from 'react';
import {
  ChartBarIcon,
  ClockIcon,
  GlobeAltIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  ArrowPathIcon,
  MapIcon,
} from '@heroicons/react/24/outline';
import { Modal } from '../ui/Modal';
import { Tabs } from '../ui/Tabs';
import { LineChart, BarChart, PieChart } from '../charts';
import type { APIKey } from '../../lib/types';

interface APIKeyUsageModalProps {
  isOpen: boolean;
  onClose: () => void;
  apiKey: APIKey;
}

// Mock data generators for API key usage analytics
function generateUsageOverTime() {
  const data = [];
  const now = Date.now();
  
  for (let i = 23; i >= 0; i--) {
    const timestamp = new Date(now - i * 60 * 60 * 1000);
    const hour = timestamp.getHours();
    
    // Simulate realistic API usage patterns
    let baseRequests = 50;
    if (hour >= 9 && hour <= 17) baseRequests = 150; // Business hours
    if (hour >= 18 && hour <= 22) baseRequests = 100; // Evening
    if (hour >= 23 || hour <= 6) baseRequests = 20;   // Night
    
    const requests = Math.floor(baseRequests + Math.random() * 50 - 25);
    const errors = Math.floor(requests * (0.005 + Math.random() * 0.01)); // 0.5-1.5% error rate
    const avgLatency = Math.floor(25 + Math.random() * 40 + (Math.random() > 0.9 ? 100 : 0)); // Occasional spikes
    
    data.push({
      time: timestamp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
      timestamp: timestamp.toISOString(),
      requests: Math.max(0, requests),
      errors: errors,
      latency: Math.max(10, avgLatency),
      successRate: Math.max(95, 100 - (errors / requests) * 100),
    });
  }
  
  return data;
}

function generateEndpointUsage() {
  const endpoints = [
    { name: '/v1/heaps', requests: 1247, avgLatency: 45 },
    { name: '/v1/heaps/:topic', requests: 892, avgLatency: 38 },
    { name: '/v1/heaps/:topic/enqueue', requests: 2341, avgLatency: 28 },
    { name: '/v1/heaps/:topic/stats', requests: 567, avgLatency: 22 },
    { name: '/v1/stats', requests: 234, avgLatency: 15 },
    { name: '/v1/keys', requests: 89, avgLatency: 35 },
  ];
  
  return endpoints.map(endpoint => ({
    ...endpoint,
    color: `hsl(${Math.floor(Math.random() * 360)}, 70%, 60%)`,
  }));
}

function generateGeographicUsage() {
  const regions = [
    { name: 'US West', requests: 3245, percentage: 45.2 },
    { name: 'US East', requests: 2156, percentage: 30.1 },
    { name: 'Europe', requests: 1023, percentage: 14.3 },
    { name: 'Asia Pacific', requests: 567, percentage: 7.9 },
    { name: 'Other', requests: 178, percentage: 2.5 },
  ];
  
  return regions.map(region => ({
    ...region,
    color: `hsl(${Math.floor(Math.random() * 360)}, 70%, 60%)`,
  }));
}

function generateDeviceTypes() {
  return [
    { name: 'Server/API', value: 4521, color: '#3B82F6' },
    { name: 'Web Application', value: 1847, color: '#10B981' },
    { name: 'Mobile App', value: 892, color: '#F59E0B' },
    { name: 'CLI/Scripts', value: 234, color: '#8B5CF6' },
  ];
}

function generateAuditTrail() {
  const activities = [
    'API request to /v1/heaps/user-events/enqueue',
    'API request to /v1/heaps/payment-processing',
    'API request to /v1/stats',
    'API request to /v1/heaps',
    'Authentication success',
    'API request to /v1/heaps/email-notifications/stats',
    'API request failed - Invalid payload',
    'API request to /v1/heaps/analytics-events/enqueue',
    'Rate limit exceeded',
    'API request to /v1/keys',
  ];

  const data = [];
  const now = Date.now();
  
  for (let i = 0; i < 20; i++) {
    const timestamp = new Date(now - Math.random() * 24 * 60 * 60 * 1000);
    const activity = activities[Math.floor(Math.random() * activities.length)];
    const isError = activity.includes('failed') || activity.includes('exceeded');
    
    data.push({
      id: `audit_${i}`,
      timestamp: timestamp.toISOString(),
      activity,
      ipAddress: `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
      userAgent: ['Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'curl/7.68.0', 'Go-http-client/1.1', 'PostmanRuntime/7.29.0'][Math.floor(Math.random() * 4)],
      responseCode: isError ? (Math.random() > 0.5 ? 400 : 429) : (Math.random() > 0.1 ? 200 : 201),
      responseTime: Math.floor(Math.random() * 200 + 20),
    });
  }
  
  return data.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
}

export function APIKeyUsageModal({ isOpen, onClose, apiKey }: APIKeyUsageModalProps) {
  const [usageData, setUsageData] = useState(generateUsageOverTime);
  const [endpointData] = useState(generateEndpointUsage);
  const [geoData] = useState(generateGeographicUsage);
  const [deviceData] = useState(generateDeviceTypes);
  const [auditData] = useState(generateAuditTrail);
  const [refreshing, setRefreshing] = useState(false);

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      setUsageData(generateUsageOverTime());
    }, 30000); // Update every 30 seconds

    return () => clearInterval(interval);
  }, []);

  const handleRefresh = async () => {
    setRefreshing(true);
    
    try {
      await new Promise(resolve => setTimeout(resolve, 1000));
      setUsageData(generateUsageOverTime());
    } finally {
      setRefreshing(false);
    }
  };

  // Calculate summary stats
  const totalRequests = usageData.reduce((sum, d) => sum + d.requests, 0);
  const totalErrors = usageData.reduce((sum, d) => sum + d.errors, 0);
  const avgLatency = Math.floor(usageData.reduce((sum, d) => sum + d.latency, 0) / usageData.length);
  const avgSuccessRate = usageData.reduce((sum, d) => sum + d.successRate, 0) / usageData.length;

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    
    if (diffMs < 60 * 1000) return 'Just now';
    if (diffMs < 60 * 60 * 1000) return `${Math.floor(diffMs / (60 * 1000))}m ago`;
    if (diffMs < 24 * 60 * 60 * 1000) return `${Math.floor(diffMs / (60 * 60 * 1000))}h ago`;
    return date.toLocaleDateString();
  };

  const getStatusColor = (code: number) => {
    if (code >= 200 && code < 300) return 'text-success-600 dark:text-success-400';
    if (code >= 400 && code < 500) return 'text-warning-600 dark:text-warning-400';
    if (code >= 500) return 'text-error-600 dark:text-error-400';
    return 'text-gray-600 dark:text-gray-400';
  };

  const tabs = [
    {
      id: 'overview',
      label: 'Overview',
      content: (
        <div className="space-y-6">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-blue-100 dark:bg-blue-900/20 rounded-full flex items-center justify-center">
                  <ChartBarIcon className="w-4 h-4 text-blue-600 dark:text-blue-400" />
                </div>
                <div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white">
                    {totalRequests.toLocaleString()}
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">Total Requests</div>
                </div>
              </div>
            </div>

            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-green-100 dark:bg-green-900/20 rounded-full flex items-center justify-center">
                  <CheckCircleIcon className="w-4 h-4 text-green-600 dark:text-green-400" />
                </div>
                <div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white">
                    {avgSuccessRate.toFixed(1)}%
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">Success Rate</div>
                </div>
              </div>
            </div>

            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-yellow-100 dark:bg-yellow-900/20 rounded-full flex items-center justify-center">
                  <ClockIcon className="w-4 h-4 text-yellow-600 dark:text-yellow-400" />
                </div>
                <div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white">
                    {avgLatency}ms
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">Avg Latency</div>
                </div>
              </div>
            </div>

            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-red-100 dark:bg-red-900/20 rounded-full flex items-center justify-center">
                  <ExclamationTriangleIcon className="w-4 h-4 text-red-600 dark:text-red-400" />
                </div>
                <div>
                  <div className="text-2xl font-bold text-gray-900 dark:text-white">
                    {totalErrors}
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">Total Errors</div>
                </div>
              </div>
            </div>
          </div>

          {/* Usage Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <LineChart
              title="Request Volume"
              subtitle="Requests per hour over the last 24 hours"
              data={usageData}
              lines={[
                { key: 'requests', name: 'Requests', color: '#3B82F6' },
                { key: 'errors', name: 'Errors', color: '#EF4444' },
              ]}
              height={280}
            />

            <LineChart
              title="Response Times & Success Rate"
              subtitle="Average latency and success rate over time"
              data={usageData}
              lines={[
                { key: 'latency', name: 'Latency (ms)', color: '#F59E0B' },
                { key: 'successRate', name: 'Success Rate (%)', color: '#10B981' },
              ]}
              height={280}
            />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <BarChart
              title="Top Endpoints"
              subtitle="Most frequently accessed API endpoints"
              data={endpointData}
              bars={[
                { key: 'requests', name: 'Requests', color: '#3B82F6' },
              ]}
              height={280}
            />

            <PieChart
              title="Geographic Usage"
              subtitle="Requests by geographic region"
              data={geoData.map(item => ({ name: item.name, value: item.requests, color: item.color }))}
              height={280}
              innerRadius={50}
            />
          </div>
        </div>
      ),
    },
    {
      id: 'analytics',
      label: 'Analytics',
      content: (
        <div className="space-y-6">
          {/* Device Types and Usage Patterns */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <PieChart
              title="Client Types"
              subtitle="Breakdown of API access by client type"
              data={deviceData}
              height={280}
              innerRadius={60}
            />

            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
              <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  Endpoint Performance
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Average latency by endpoint
                </p>
              </div>
              <div className="px-6 py-4">
                <div className="space-y-4">
                  {endpointData.map((endpoint, index) => (
                    <div key={index} className="flex items-center justify-between">
                      <div className="flex items-center space-x-3">
                        <div className="w-3 h-3 rounded-full" style={{ backgroundColor: endpoint.color }} />
                        <code className="text-sm font-mono text-gray-900 dark:text-white">
                          {endpoint.name}
                        </code>
                      </div>
                      <div className="flex items-center space-x-4 text-sm">
                        <div className="text-right">
                          <div className="font-medium text-gray-900 dark:text-white">
                            {endpoint.requests.toLocaleString()}
                          </div>
                          <div className="text-xs text-gray-500 dark:text-gray-400">requests</div>
                        </div>
                        <div className="text-right">
                          <div className={`font-medium ${
                            endpoint.avgLatency > 50 ? 'text-warning-600 dark:text-warning-400' :
                            endpoint.avgLatency > 100 ? 'text-error-600 dark:text-error-400' :
                            'text-success-600 dark:text-success-400'
                          }`}>
                            {endpoint.avgLatency}ms
                          </div>
                          <div className="text-xs text-gray-500 dark:text-gray-400">avg latency</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

          {/* Geographic Details */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center space-x-3">
                <GlobeAltIcon className="w-6 h-6 text-blue-600 dark:text-blue-400" />
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                    Geographic Distribution
                  </h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">
                    Request volume by region
                  </p>
                </div>
              </div>
            </div>
            <div className="px-6 py-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {geoData.map((region, index) => (
                  <div key={index} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                    <div className="flex items-center space-x-3">
                      <MapIcon className="w-5 h-5 text-gray-400" />
                      <div>
                        <div className="font-medium text-gray-900 dark:text-white">
                          {region.name}
                        </div>
                        <div className="text-sm text-gray-500 dark:text-gray-400">
                          {region.percentage}%
                        </div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-lg font-semibold text-gray-900 dark:text-white">
                        {region.requests.toLocaleString()}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      ),
    },
    {
      id: 'audit',
      label: 'Audit Trail',
      content: (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                    Recent Activity
                  </h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">
                    Last 20 API requests and activities
                  </p>
                </div>
                <button
                  onClick={handleRefresh}
                  disabled={refreshing}
                  className="btn btn-secondary btn-sm"
                >
                  <ArrowPathIcon className={`w-4 h-4 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
                  Refresh
                </button>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800/50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Timestamp
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Activity
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      IP Address
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                      Response Time
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                  {auditData.map((entry) => (
                    <tr key={entry.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                        {formatTimestamp(entry.timestamp)}
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-900 dark:text-white">
                        <div className="max-w-xs truncate" title={entry.activity}>
                          {entry.activity}
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                          {entry.userAgent}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-600 dark:text-gray-400">
                        {entry.ipAddress}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(entry.responseCode)} bg-gray-100 dark:bg-gray-700`}>
                          {entry.responseCode}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                        {entry.responseTime}ms
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
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
      title={`Usage Analytics: ${apiKey.name}`}
      size="2xl"
    >
      <div className="space-y-6">
        {/* Key Info Header */}
        <div className="bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="w-12 h-12 bg-primary-100 dark:bg-primary-900/20 rounded-lg flex items-center justify-center">
                <ChartBarIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
              </div>
              <div>
                <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                  {apiKey.name}
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Created {new Date(apiKey.created_at).toLocaleDateString()} • 
                  Last used {apiKey.last_used_at ? formatTimestamp(apiKey.last_used_at) : 'Never'}
                </p>
              </div>
            </div>
            <div className="text-right">
              <div className="text-2xl font-bold text-gray-900 dark:text-white">
                {(apiKey.usage?.total_requests || 0).toLocaleString()}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Total Requests</div>
            </div>
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
