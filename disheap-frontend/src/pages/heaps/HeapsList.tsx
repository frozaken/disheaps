
import { useState, useEffect } from 'react';
import { MagnifyingGlassIcon, FunnelIcon } from '@heroicons/react/24/outline';
import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { HeapCard } from '../../components/heaps/HeapCard';
import { CreateHeapModal } from '../../components/heaps/CreateHeapModal';
import { ConfirmationModal } from '../../components/ui/Modal';
import { useNotifications } from '../../store/app';
import { api, handleDestructiveOperation } from '../../lib/api';
import { formatAPIError } from '../../lib/errors';
import type { HeapInfo } from '../../lib/types';

export function HeapsList() {
  useDocumentTitle('Heaps');
  
  const [heaps, setHeaps] = useState<HeapInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [deleteHeap, setDeleteHeap] = useState<HeapInfo | null>(null);
  const [purgeHeap, setPurgeHeap] = useState<HeapInfo | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  
  const { showSuccess, showError } = useNotifications();

  // Load heaps on component mount
  useEffect(() => {
    loadHeaps();
  }, []);

  const loadHeaps = async () => {
    try {
      setLoading(true);
      const response = await api.listHeaps();
      setHeaps(response.heaps);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteHeap = async (heap: HeapInfo) => {
    if (!deleteHeap) return;
    
    setActionLoading(`delete-${heap.topic}`);
    
    try {
      await handleDestructiveOperation(
        (confirmationToken) => api.deleteHeap(heap.topic, confirmationToken),
        'heap deletion'
      );
      
      showSuccess(
        'Heap Deleted',
        `Successfully deleted heap "${heap.topic}"`
      );
      
      // Reload heaps list
      await loadHeaps();
      setDeleteHeap(null);
    } catch (error: any) {
      if (error.message !== 'Operation cancelled by user') {
        const formattedError = formatAPIError(error);
        showError(formattedError.title, formattedError.message);
      }
    } finally {
      setActionLoading(null);
    }
  };

  const handlePurgeHeap = async (heap: HeapInfo) => {
    if (!purgeHeap) return;
    
    setActionLoading(`purge-${heap.topic}`);
    
    try {
      await handleDestructiveOperation(
        (confirmationToken) => api.purgeHeap(heap.topic, confirmationToken),
        'heap purge'
      );
      
      showSuccess(
        'Heap Purged',
        `Successfully purged all messages from heap "${heap.topic}"`
      );
      
      // Reload heaps list
      await loadHeaps();
      setPurgeHeap(null);
    } catch (error: any) {
      if (error.message !== 'Operation cancelled by user') {
        const formattedError = formatAPIError(error);
        showError(formattedError.title, formattedError.message);
      }
    } finally {
      setActionLoading(null);
    }
  };

  // Filter heaps based on search term
  const filteredHeaps = heaps.filter(heap =>
    heap.topic.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">Heaps</h1>
          <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
            Manage your priority message queues.
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
          <button
            type="button"
            onClick={() => setShowCreateModal(true)}
            className="btn btn-primary"
          >
            Create Heap
          </button>
        </div>
      </div>

      {/* Search and Filters */}
      {heaps.length > 0 && (
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-3 sm:space-y-0 sm:space-x-4">
          {/* Search */}
          <div className="relative flex-1 max-w-md">
            <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
              <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search heaps..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="input pl-10"
            />
          </div>

          {/* Filter button (placeholder for future filters) */}
          <div className="flex items-center space-x-2">
            <button
              type="button"
              className="btn btn-secondary inline-flex items-center"
              onClick={() => {
                // TODO: Implement filtering modal
              }}
            >
              <FunnelIcon className="h-4 w-4 mr-2" />
              Filters
            </button>
          </div>
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <div className="text-center">
            <div className="loading-spinner h-8 w-8 mx-auto mb-4" />
            <p className="text-gray-600 dark:text-gray-400">Loading heaps...</p>
          </div>
        </div>
      ) : filteredHeaps.length === 0 ? (
        <div className="card p-8 text-center">
          <div className="mx-auto h-12 w-12 text-gray-400 dark:text-gray-500">
            <svg fill="none" stroke="currentColor" viewBox="0 0 48 48">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1}
                d="M34 40h10v-4a6 6 0 00-10.712-3.714M34 40H14m20 0v-4a9.971 9.971 0 00-.712-3.714M14 40H4v-4a6 6 0 0110.713-3.714M14 40v-4c0-1.313.253-2.555.713-3.714m0 0A9.971 9.971 0 0124 24c4.21 0 7.813 2.602 9.288 6.286"
              />
            </svg>
          </div>
          <h3 className="mt-4 text-sm font-medium text-gray-900 dark:text-white">
            {heaps.length === 0 ? 'No heaps' : 'No matching heaps'}
          </h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            {heaps.length === 0 
              ? 'Get started by creating your first priority message heap.'
              : 'Try adjusting your search criteria.'
            }
          </p>
          {heaps.length === 0 && (
            <div className="mt-6">
              <button
                type="button"
                onClick={() => setShowCreateModal(true)}
                className="btn btn-primary"
              >
                Create Your First Heap
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2 xl:grid-cols-3">
          {filteredHeaps.map((heap) => (
            <HeapCard
              key={heap.topic}
              heap={heap}
              onEdit={() => {
                // TODO: Implement edit modal
                showError('Not implemented', 'Heap editing is not yet implemented');
              }}
              onDelete={() => setDeleteHeap(heap)}
              onPurge={() => setPurgeHeap(heap)}
            />
          ))}
        </div>
      )}

      {/* Create Heap Modal */}
      <CreateHeapModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onSuccess={loadHeaps}
      />

      {/* Delete Confirmation Modal */}
      <ConfirmationModal
        isOpen={!!deleteHeap}
        onClose={() => setDeleteHeap(null)}
        onConfirm={() => deleteHeap && handleDeleteHeap(deleteHeap)}
        title="Delete Heap"
        message={`Are you sure you want to delete the heap "${deleteHeap?.topic}"? This action cannot be undone and will permanently remove all messages and configuration.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        isLoading={actionLoading === `delete-${deleteHeap?.topic}`}
      />

      {/* Purge Confirmation Modal */}
      <ConfirmationModal
        isOpen={!!purgeHeap}
        onClose={() => setPurgeHeap(null)}
        onConfirm={() => purgeHeap && handlePurgeHeap(purgeHeap)}
        title="Purge Heap Messages"
        message={`Are you sure you want to purge all messages from the heap "${purgeHeap?.topic}"? This will permanently delete all messages but keep the heap configuration.`}
        confirmText="Purge Messages"
        cancelText="Cancel"
        variant="warning"
        isLoading={actionLoading === `purge-${purgeHeap?.topic}`}
      />
    </div>
  );
}
