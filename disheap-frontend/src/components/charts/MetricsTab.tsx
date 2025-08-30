import { useState, useEffect } from 'react';
import { ArrowPathIcon } from '@heroicons/react/24/outline';
import { LineChart } from './LineChart';
import { BarChart } from './BarChart';
import { PieChart } from './PieChart';
import { AreaChart } from './AreaChart';
import type { HeapInfo } from '../../lib/types';

interface MetricsTabProps {
  heap: HeapInfo;
  onRefresh?: () => void;
  isRefreshing?: boolean;
}

// Generate mock time series data
function generateTimeSeriesData(points: number = 20): any[] {
  const data = [];
  const now = new Date();
  
  for (let i = points - 1; i >= 0; i--) {
    const timestamp = new Date(now.getTime() - i * 30000); // 30-second intervals
    const baseEnqueue = 1000 + Math.random() * 500;
    const basePop = baseEnqueue * (0.8 + Math.random() * 0.3);
    
    data.push({
      timestamp: timestamp.toISOString(),
      enqueues: Math.floor(baseEnqueue + Math.sin(i / 3) * 200),
      pops: Math.floor(basePop + Math.sin((i + 1) / 3) * 150),
      acks: Math.floor(basePop * (0.85 + Math.random() * 0.1)),
      nacks: Math.floor(basePop * (0.05 + Math.random() * 0.05)),
    });
  }
  
  return data;
}

// Generate cumulative message data
function generateCumulativeData(points: number = 20): any[] {
  const data = [];
  const now = new Date();
  let totalMessages = 10000;
  let inflightMessages = 50;
  let dlqMessages = 25;
  
  for (let i = points - 1; i >= 0; i--) {
    const timestamp = new Date(now.getTime() - i * 60000); // 1-minute intervals
    
    // Simulate message flow
    const newMessages = Math.floor(Math.random() * 200 + 50);
    const processedMessages = Math.floor(Math.random() * 180 + 40);
    const failedMessages = Math.floor(Math.random() * 10 + 2);
    
    totalMessages += newMessages - processedMessages;
    inflightMessages = Math.max(0, inflightMessages + Math.floor(Math.random() * 20 - 10));
    dlqMessages += failedMessages;
    
    data.push({
      timestamp: timestamp.toISOString(),
      total: totalMessages,
      inflight: inflightMessages,
      dlq: dlqMessages,
    });
  }
  
  return data;
}

export function MetricsTab({ heap, onRefresh, isRefreshing = false }: MetricsTabProps) {
  const [timeSeriesData, setTimeSeriesData] = useState(generateTimeSeriesData);
  const [cumulativeData, setCumulativeData] = useState(generateCumulativeData);
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Simulate real-time updates
  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      // Add new data point and remove oldest
      setTimeSeriesData(prev => {
        const newData = [...prev];
        const lastPoint = newData[newData.length - 1];
        const timestamp = new Date(new Date(lastPoint.timestamp).getTime() + 30000);
        
        const baseEnqueue = 1000 + Math.random() * 500;
        const basePop = baseEnqueue * (0.8 + Math.random() * 0.3);
        
        newData.push({
          timestamp: timestamp.toISOString(),
          enqueues: Math.floor(baseEnqueue + Math.sin(Date.now() / 10000) * 200),
          pops: Math.floor(basePop + Math.sin(Date.now() / 12000) * 150),
          acks: Math.floor(basePop * (0.85 + Math.random() * 0.1)),
          nacks: Math.floor(basePop * (0.05 + Math.random() * 0.05)),
        });
        
        return newData.slice(-20); // Keep last 20 points
      });

      setCumulativeData(prev => {
        const newData = [...prev];
        const lastPoint = newData[newData.length - 1];
        const timestamp = new Date(new Date(lastPoint.timestamp).getTime() + 60000);
        
        const newMessages = Math.floor(Math.random() * 200 + 50);
        const processedMessages = Math.floor(Math.random() * 180 + 40);
        const failedMessages = Math.floor(Math.random() * 10 + 2);
        
        newData.push({
          timestamp: timestamp.toISOString(),
          total: lastPoint.total + newMessages - processedMessages,
          inflight: Math.max(0, lastPoint.inflight + Math.floor(Math.random() * 20 - 10)),
          dlq: lastPoint.dlq + failedMessages,
        });
        
        return newData.slice(-20); // Keep last 20 points
      });
    }, 5000); // Update every 5 seconds

    return () => clearInterval(interval);
  }, [autoRefresh]);

  // Operation statistics from heap data
  const operationData = [
    { name: 'Enqueues', value: heap.stats.total_enqueues, color: '#3B82F6' },
    { name: 'Pops', value: heap.stats.total_pops, color: '#10B981' },
    { name: 'Acks', value: heap.stats.total_acks, color: '#22C55E' },
    { name: 'Nacks', value: heap.stats.total_nacks, color: '#F59E0B' },
    { name: 'Retries', value: heap.stats.total_retries, color: '#EF4444' },
  ];

  // Message status distribution
  const messageStatusData = [
    { name: 'Active', value: heap.stats.total_messages - heap.stats.inflight_messages - heap.stats.dlq_messages, color: '#22C55E' },
    { name: 'In Flight', value: heap.stats.inflight_messages, color: '#F59E0B' },
    { name: 'DLQ', value: heap.stats.dlq_messages, color: '#EF4444' },
  ].filter(item => item.value > 0);

  // Partition performance data
  const partitionData = Object.entries(heap.stats.partition_stats || {}).map(([partition, stats]) => ({
    name: `P${partition}`,
    messages: stats.messages || 0,
    throughput: Math.floor(Math.random() * 1000 + 200), // Mock throughput
    latency: Math.floor(Math.random() * 50 + 10), // Mock latency
    color: stats.is_healthy ? '#22C55E' : '#EF4444',
  }));

  const refreshActions = (
    <div className="flex items-center space-x-2">
      <label className="flex items-center">
        <input
          type="checkbox"
          checked={autoRefresh}
          onChange={(e) => setAutoRefresh(e.target.checked)}
          className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
        />
        <span className="ml-2 text-sm text-gray-600 dark:text-gray-400">Auto-refresh</span>
      </label>
      
      <button
        onClick={onRefresh}
        disabled={isRefreshing}
        className="btn btn-secondary btn-sm"
      >
        <ArrowPathIcon className={`w-4 h-4 ${isRefreshing ? 'animate-spin' : ''}`} />
      </button>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Real-time Throughput */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <LineChart
          title="Real-time Throughput"
          subtitle="Operations per minute over the last 10 minutes"
          data={timeSeriesData}
          lines={[
            { key: 'enqueues', name: 'Enqueues', color: '#3B82F6' },
            { key: 'pops', name: 'Pops', color: '#10B981' },
            { key: 'acks', name: 'Acks', color: '#22C55E' },
            { key: 'nacks', name: 'Nacks', color: '#F59E0B' },
          ]}
          actions={refreshActions}
        />

        <AreaChart
          title="Message Volume"
          subtitle="Cumulative message counts over time"
          data={cumulativeData}
          areas={[
            { key: 'total', name: 'Total Messages', color: '#3B82F6', fillOpacity: 0.6 },
            { key: 'inflight', name: 'In-Flight', color: '#F59E0B', fillOpacity: 0.4 },
            { key: 'dlq', name: 'DLQ', color: '#EF4444', fillOpacity: 0.3 },
          ]}
        />
      </div>

      {/* Operation Statistics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <BarChart
          title="Operation Counts"
          subtitle="Total operations since heap creation"
          data={operationData}
          bars={[
            { key: 'value', name: 'Count', color: '#3B82F6' }
          ]}
          showLegend={false}
          yAxisLabel="Operations"
        />

        <PieChart
          title="Message Status Distribution"
          subtitle="Current message states in the heap"
          data={messageStatusData}
          innerRadius={60}
          showLabels={true}
        />
      </div>

      {/* Partition Performance */}
      {partitionData.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <BarChart
            title="Partition Message Counts"
            subtitle="Messages per partition"
            data={partitionData}
            bars={[
              { key: 'messages', name: 'Messages', color: '#3B82F6' }
            ]}
            showLegend={false}
            yAxisLabel="Message Count"
          />

          <BarChart
            title="Partition Throughput"
            subtitle="Operations per minute by partition"
            data={partitionData}
            bars={[
              { key: 'throughput', name: 'Ops/min', color: '#10B981' }
            ]}
            showLegend={false}
            yAxisLabel="Operations/min"
          />
        </div>
      )}

      {/* Performance Metrics Summary */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm">
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            Performance Summary
          </h3>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Key performance indicators for this heap
          </p>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="text-2xl font-bold text-primary-600 dark:text-primary-400">
                {((heap.stats.total_acks / Math.max(heap.stats.total_pops, 1)) * 100).toFixed(1)}%
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Success Rate</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-success-600 dark:text-success-400">
                {(heap.stats.total_enqueues / Math.max(Math.floor((Date.now() - new Date(heap.created_at || Date.now()).getTime()) / 60000), 1)).toFixed(0)}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Avg Enqueues/min</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-warning-600 dark:text-warning-400">
                {heap.stats.inflight_messages}
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">In-Flight Messages</div>
            </div>
            
            <div className="text-center">
              <div className="text-2xl font-bold text-error-600 dark:text-error-400">
                {((heap.stats.total_retries / Math.max(heap.stats.total_pops, 1)) * 100).toFixed(1)}%
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">Retry Rate</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
