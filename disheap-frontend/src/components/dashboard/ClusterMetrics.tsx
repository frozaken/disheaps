import { useState, useEffect } from 'react';
import {
  ChartBarIcon,
  ClockIcon,
  ArrowTrendingUpIcon,
  UsersIcon,
  ServerIcon,
} from '@heroicons/react/24/outline';
import { LineChart, AreaChart, BarChart } from '../charts';

interface ClusterMetricsProps {
  onRefresh?: () => void;
  isRefreshing?: boolean;
}

// Mock data generators for cluster-wide metrics
function generateClusterThroughput() {
  const now = Date.now();
  const data = [];
  
  for (let i = 59; i >= 0; i--) {
    const timestamp = new Date(now - i * 60 * 1000);
    const hour = timestamp.getHours();
    
    // Simulate daily patterns with some randomness
    let baseOps = 1000;
    if (hour >= 9 && hour <= 17) baseOps = 2500; // Business hours
    if (hour >= 18 && hour <= 22) baseOps = 1800; // Evening
    if (hour >= 23 || hour <= 6) baseOps = 600;   // Night
    
    const ops = Math.floor(baseOps + Math.random() * 500 - 250);
    const latency = Math.floor(15 + Math.random() * 20 + (Math.random() > 0.9 ? 50 : 0)); // Occasional spikes
    
    data.push({
      time: timestamp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
      timestamp: timestamp.toISOString(),
      throughput: Math.max(0, ops),
      latency: Math.max(5, latency),
      errors: Math.floor(ops * 0.001 + Math.random() * 2), // ~0.1% error rate
    });
  }
  
  return data;
}

function generateConnectionMetrics() {
  const now = Date.now();
  const data = [];
  
  for (let i = 29; i >= 0; i--) {
    const timestamp = new Date(now - i * 2 * 60 * 1000); // Every 2 minutes
    const connections = Math.floor(150 + Math.random() * 50);
    const activeStreams = Math.floor(connections * 0.3 + Math.random() * 20);
    
    data.push({
      time: timestamp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
      timestamp: timestamp.toISOString(),
      connections,
      activeStreams,
    });
  }
  
  return data;
}

function generateResourceUtilization() {
  const data: Array<{
    name: string;
    cpu: number;
    memory: number;
    disk: number;
    network: number;
  }> = [];
  const nodes = ['node-1', 'node-2', 'node-3', 'node-4'];
  
  nodes.forEach(node => {
    data.push({
      name: node,
      cpu: Math.floor(Math.random() * 40 + 20),
      memory: Math.floor(Math.random() * 30 + 40),
      disk: Math.floor(Math.random() * 20 + 15),
      network: Math.floor(Math.random() * 60 + 20),
    });
  });
  
  return data;
}

function generateHeapActivity() {
  const topics = ['user-events', 'payment-processing', 'email-notifications', 'analytics-events', 'audit-logs'];
  const data: Array<{
    name: string;
    messages: number;
    operations: number;
    growth: number;
    color: string;
  }> = [];
  
  topics.forEach(topic => {
    const messages = Math.floor(Math.random() * 50000 + 10000);
    const ops = Math.floor(Math.random() * 1000 + 200);
    
    data.push({
      name: topic,
      messages,
      operations: ops,
      growth: Math.floor((Math.random() - 0.5) * 30), // -15% to +15% growth
      color: `hsl(${Math.floor(Math.random() * 360)}, 70%, 60%)`,
    });
  });
  
  return data.sort((a, b) => b.operations - a.operations);
}

export function ClusterMetrics({ onRefresh, isRefreshing = false }: ClusterMetricsProps) {
  const [throughputData, setThroughputData] = useState(generateClusterThroughput);
  const [connectionData, setConnectionData] = useState(generateConnectionMetrics);
  const [resourceData, setResourceData] = useState(generateResourceUtilization);
  const [heapData, setHeapData] = useState(generateHeapActivity);

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      setThroughputData(generateClusterThroughput());
      setConnectionData(generateConnectionMetrics());
      setResourceData(generateResourceUtilization());
      setHeapData(generateHeapActivity());
    }, 30000); // Update every 30 seconds

    return () => clearInterval(interval);
  }, []);

  const currentThroughput = throughputData[throughputData.length - 1];
  const currentConnections = connectionData[connectionData.length - 1];
  
  // Calculate averages and trends
  const avgThroughput = Math.floor(throughputData.reduce((sum, d) => sum + d.throughput, 0) / throughputData.length);
  const avgLatency = Math.floor(throughputData.reduce((sum, d) => sum + d.latency, 0) / throughputData.length);
  const totalErrors = throughputData.reduce((sum, d) => sum + d.errors, 0);

  return (
    <div className="space-y-6">
      {/* Key Metrics Summary */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <ArrowTrendingUpIcon className="h-6 w-6 text-blue-600 dark:text-blue-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Current Throughput
                  </dt>
                  <dd className="flex items-baseline">
                    <div className="text-2xl font-semibold text-gray-900 dark:text-white">
                      {currentThroughput?.throughput?.toLocaleString() || '0'}
                    </div>
                    <div className="ml-2 flex items-baseline text-sm font-semibold text-green-600 dark:text-green-400">
                      ops/min
                    </div>
                  </dd>
                </dl>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-5 py-3">
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Avg: </span>
              <span className="text-gray-900 dark:text-white font-medium">
                {avgThroughput.toLocaleString()} ops/min
              </span>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <ClockIcon className="h-6 w-6 text-yellow-600 dark:text-yellow-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Average Latency
                  </dt>
                  <dd className="flex items-baseline">
                    <div className="text-2xl font-semibold text-gray-900 dark:text-white">
                      {currentThroughput?.latency || '0'}
                    </div>
                    <div className="ml-2 flex items-baseline text-sm font-semibold text-yellow-600 dark:text-yellow-400">
                      ms
                    </div>
                  </dd>
                </dl>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-5 py-3">
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Avg: </span>
              <span className="text-gray-900 dark:text-white font-medium">
                {avgLatency}ms
              </span>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <UsersIcon className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Active Connections
                  </dt>
                  <dd className="flex items-baseline">
                    <div className="text-2xl font-semibold text-gray-900 dark:text-white">
                      {currentConnections?.connections || '0'}
                    </div>
                    <div className="ml-2 flex items-baseline text-sm font-semibold text-green-600 dark:text-green-400">
                      clients
                    </div>
                  </dd>
                </dl>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-5 py-3">
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Streams: </span>
              <span className="text-gray-900 dark:text-white font-medium">
                {currentConnections?.activeStreams || '0'}
              </span>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <ChartBarIcon className="h-6 w-6 text-red-600 dark:text-red-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Error Count
                  </dt>
                  <dd className="flex items-baseline">
                    <div className="text-2xl font-semibold text-gray-900 dark:text-white">
                      {totalErrors}
                    </div>
                    <div className="ml-2 flex items-baseline text-sm font-semibold text-red-600 dark:text-red-400">
                      last hour
                    </div>
                  </dd>
                </dl>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-5 py-3">
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Rate: </span>
              <span className="text-gray-900 dark:text-white font-medium">
                {((totalErrors / avgThroughput / 60) * 100).toFixed(3)}%
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Performance Charts */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <LineChart
          title="Cluster Throughput & Latency"
          subtitle="Operations per minute and average latency over the last hour"
          data={throughputData}
          lines={[
            { key: 'throughput', name: 'Throughput (ops/min)', color: '#3B82F6' },
            { key: 'latency', name: 'Avg Latency (ms)', color: '#F59E0B' },
          ]}
          height={320}

        />

        <AreaChart
          title="Connection Activity"
          subtitle="Active connections and streaming clients over time"
          data={connectionData}
          areas={[
            { key: 'connections', name: 'Total Connections', color: '#10B981' },
            { key: 'activeStreams', name: 'Active Streams', color: '#8B5CF6' },
          ]}
          height={320}

        />
      </div>

      {/* Resource Utilization and Heap Activity */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <BarChart
          title="Node Resource Utilization"
          subtitle="CPU, Memory, Disk, and Network usage per node"
          data={resourceData}
          bars={[
            { key: 'cpu', name: 'CPU (%)', color: '#3B82F6' },
            { key: 'memory', name: 'Memory (%)', color: '#10B981' },
            { key: 'disk', name: 'Disk (%)', color: '#8B5CF6' },
            { key: 'network', name: 'Network (%)', color: '#F59E0B' },
          ]}
          height={320}
          stacked={false}

        />

        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm">
          <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  Top Active Heaps
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Heaps sorted by current operation volume
                </p>
              </div>
              {onRefresh && (
                <button
                  onClick={onRefresh}
                  disabled={isRefreshing}
                  className="btn btn-secondary btn-sm"
                >
                  <ServerIcon className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
                  Refresh
                </button>
              )}
            </div>
          </div>

          <div className="px-6 py-4">
            <div className="space-y-4">
              {heapData.map((heap, index) => (
                <div key={heap.name} className="flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    <div className="w-8 h-8 rounded-full bg-gray-100 dark:bg-gray-700 flex items-center justify-center text-sm font-medium text-gray-600 dark:text-gray-400">
                      {index + 1}
                    </div>
                    <div>
                      <div className="font-medium text-gray-900 dark:text-white">
                        {heap.name}
                      </div>
                      <div className="text-sm text-gray-500 dark:text-gray-400">
                        {heap.messages.toLocaleString()} messages
                      </div>
                    </div>
                  </div>

                  <div className="flex items-center space-x-4 text-right">
                    <div>
                      <div className="text-lg font-semibold text-gray-900 dark:text-white">
                        {heap.operations.toLocaleString()}
                      </div>
                      <div className="text-xs text-gray-500 dark:text-gray-400">ops/min</div>
                    </div>
                    <div className={`flex items-center text-sm font-medium ${
                      heap.growth > 0 
                        ? 'text-success-600 dark:text-success-400'
                        : heap.growth < 0
                        ? 'text-error-600 dark:text-error-400'
                        : 'text-gray-500 dark:text-gray-400'
                    }`}>
                      <ArrowTrendingUpIcon className={`w-4 h-4 mr-1 ${heap.growth < 0 ? 'rotate-180' : ''}`} />
                      {Math.abs(heap.growth)}%
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
