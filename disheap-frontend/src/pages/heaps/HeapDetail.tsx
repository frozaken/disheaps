
import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';
import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { Tabs } from '../../components/ui/Tabs';
import { HeapOverviewTab } from '../../components/heaps/HeapOverviewTab';
import { MetricsTab } from '../../components/charts/MetricsTab';
import { useNotifications } from '../../store/app';
import { api } from '../../lib/api';
import { formatAPIError } from '../../lib/errors';
import type { HeapInfo } from '../../lib/types';

export function HeapDetail() {
  const { topic } = useParams<{ topic: string }>();
  const navigate = useNavigate();
  const { showError } = useNotifications();
  
  const [heap, setHeap] = useState<HeapInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  useDocumentTitle(heap ? `Heap: ${heap.topic}` : 'Loading...');

  // Load heap details
  useEffect(() => {
    if (!topic) {
      navigate('/heaps');
      return;
    }
    loadHeapDetails();
  }, [topic, navigate]);

  const loadHeapDetails = async (isRefresh = false) => {
    if (!topic) return;
    
    try {
      if (isRefresh) {
        setRefreshing(true);
      } else {
        setLoading(true);
      }
      
      // Get heap details from the list (since we don't have a single heap endpoint yet)
      const response = await api.listHeaps();
      const heapDetails = response.heaps.find(h => h.topic === topic);
      
      if (!heapDetails) {
        showError('Heap Not Found', `The heap "${topic}" was not found.`);
        navigate('/heaps');
        return;
      }
      
      setHeap(heapDetails);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = () => {
    loadHeapDetails(true);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-center">
          <div className="loading-spinner h-8 w-8 mx-auto mb-4" />
          <p className="text-gray-600 dark:text-gray-400">Loading heap details...</p>
        </div>
      </div>
    );
  }

  if (!heap) {
    return (
      <div className="card p-8 text-center">
        <h3 className="text-lg font-medium text-gray-900 dark:text-white">Heap Not Found</h3>
        <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
          The requested heap could not be found.
        </p>
        <div className="mt-4">
          <button
            onClick={() => navigate('/heaps')}
            className="btn btn-primary"
          >
            Back to Heaps
          </button>
        </div>
      </div>
    );
  }

  const tabs = [
    {
      id: 'overview',
      label: 'Overview',
      content: (
        <HeapOverviewTab 
          heap={heap} 
          onRefresh={handleRefresh}
          isRefreshing={refreshing}
        />
      ),
    },
    {
      id: 'config',
      label: 'Configuration',
      content: (
        <div className="card p-8 text-center">
          <h3 className="text-lg font-medium text-gray-900 dark:text-white">Coming Soon</h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            Configuration management is under development.
          </p>
        </div>
      ),
    },
    {
      id: 'partitions',
      label: 'Partitions',
      badge: heap.partitions,
      content: (
        <div className="card p-8 text-center">
          <h3 className="text-lg font-medium text-gray-900 dark:text-white">Coming Soon</h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            Partition monitoring is under development.
          </p>
        </div>
      ),
    },
    {
      id: 'metrics',
      label: 'Metrics',
      content: (
        <MetricsTab 
          heap={heap} 
          onRefresh={handleRefresh}
          isRefreshing={refreshing}
        />
      ),
    },
    {
      id: 'dlq',
      label: 'Dead Letter Queue',
      badge: heap.stats.dlq_messages > 0 ? heap.stats.dlq_messages : undefined,
      content: (
        <div className="card p-8 text-center">
          <h3 className="text-lg font-medium text-gray-900 dark:text-white">Coming Soon</h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            DLQ message browser is under development.
          </p>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      {/* Header with Back Button */}
      <div className="flex items-center space-x-4">
        <button
          onClick={() => navigate('/heaps')}
          className="btn btn-secondary inline-flex items-center"
        >
          <ArrowLeftIcon className="w-4 h-4 mr-2" />
          Back to Heaps
        </button>
        
        <div className="flex-1">
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">
            {heap.topic}
          </h1>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            {heap.mode} heap • {heap.partitions} partition{heap.partitions !== 1 ? 's' : ''} • 
            RF {heap.replication_factor}
          </p>
        </div>
      </div>

      {/* Tabs */}
      <Tabs 
        tabs={tabs} 
        defaultTab="overview"
        className="mt-6"
      />
    </div>
  );
}
