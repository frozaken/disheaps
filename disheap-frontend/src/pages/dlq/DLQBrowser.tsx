import { useState, useEffect } from 'react';
import {
  MagnifyingGlassIcon,
  FunnelIcon,
  ArrowPathIcon,
  TrashIcon,
  ExclamationTriangleIcon,
  CheckIcon,

  InformationCircleIcon,
} from '@heroicons/react/24/outline';
import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { DLQMessageCard } from '../../components/dlq/DLQMessageCard';
import { DLQFilters } from '../../components/dlq/DLQFilters';
import { DLQMessageDetailsModal } from '../../components/dlq/DLQMessageDetailsModal';
import { ConfirmationModal } from '../../components/ui/Modal';
import { useNotifications } from '../../store/app';
import { formatAPIError } from '../../lib/errors';
import type { DLQMessage, DLQBrowserFilters } from '../../lib/types';

// Generate mock DLQ messages for demonstration
function generateMockDLQMessages(): DLQMessage[] {
  const topics = ['user-events', 'payment-processing', 'email-notifications', 'analytics-events'];
  const reasons = [
    'Max retries exceeded',
    'Processing timeout',
    'Validation failed',
    'External service unavailable',
    'Invalid payload format',
    'Rate limit exceeded',
    'Authentication failure',
  ];
  const producers = ['web-app-1', 'mobile-api', 'batch-processor', 'webhook-handler', 'cron-service'];

  const messages: DLQMessage[] = [];
  
  for (let i = 0; i < 25; i++) {
    const topic = topics[Math.floor(Math.random() * topics.length)];
    const reason = reasons[Math.floor(Math.random() * reasons.length)];
    const producer = producers[Math.floor(Math.random() * producers.length)];
    const attempts = Math.floor(Math.random() * 10) + 1;
    const maxRetries = Math.max(attempts, Math.floor(Math.random() * 5) + 3);
    
    // Generate timestamps
    const enqueuedTime = new Date(Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000);
    const dlqTime = new Date(enqueuedTime.getTime() + Math.random() * 24 * 60 * 60 * 1000);
    
    // Generate realistic payloads
    const payloads = [
      JSON.stringify({ userId: Math.floor(Math.random() * 10000), action: 'purchase', amount: Math.random() * 1000 }),
      JSON.stringify({ email: `user${i}@example.com`, template: 'welcome', vars: { name: `User ${i}` } }),
      JSON.stringify({ event: 'page_view', url: `/page-${i}`, timestamp: Date.now() }),
      JSON.stringify({ orderId: `order-${i}`, status: 'pending', items: [{ id: 1, qty: 2 }] }),
      `{"malformed": json without closing brace, "id": ${i}`,
      JSON.stringify({ webhook: { url: 'https://api.example.com/callback', payload: { id: i } } }),
    ];
    
    messages.push({
      message_id: `msg_${Date.now()}_${i}_${Math.random().toString(36).substr(2, 9)}`,
      topic,
      partition_id: Math.floor(Math.random() * 8),
      payload: btoa(payloads[Math.floor(Math.random() * payloads.length)]),
      priority: Math.floor(Math.random() * 1000),
      enqueued_time: enqueuedTime.toISOString(),
      attempts,
      max_retries: maxRetries,
      is_leased: false,
      in_dlq: true,
      producer_id: producer,
      epoch: Math.floor(Math.random() * 1000),
      sequence: Math.floor(Math.random() * 10000),
      dlq_reason: reason,
      dlq_timestamp: dlqTime.toISOString(),
      original_failure_reason: `${reason}: ${['Connection refused', 'Timeout after 30s', 'Invalid JSON', 'HTTP 500'][Math.floor(Math.random() * 4)]}`,
    });
  }
  
  return messages.sort((a, b) => new Date(b.dlq_timestamp).getTime() - new Date(a.dlq_timestamp).getTime());
}

export function DLQBrowser() {
  useDocumentTitle('Dead Letter Queue');
  
  const [messages, setMessages] = useState<DLQMessage[]>([]);
  const [filteredMessages, setFilteredMessages] = useState<DLQMessage[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filters, setFilters] = useState<DLQBrowserFilters>({});
  const [selectedMessages, setSelectedMessages] = useState<Set<string>>(new Set());
  const [showFilters, setShowFilters] = useState(false);
  const [bulkAction, setBulkAction] = useState<'replay' | 'delete' | null>(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [selectedMessageForDetails, setSelectedMessageForDetails] = useState<DLQMessage | null>(null);
  
  const { showSuccess, showError, showWarning } = useNotifications();

  // Load messages on component mount
  useEffect(() => {
    loadMessages();
  }, []);

  // Apply search and filters when they change
  useEffect(() => {
    applyFiltersAndSearch();
  }, [messages, searchTerm, filters]);

  const loadMessages = async () => {
    try {
      setLoading(true);
      
      // In a real app, this would be: const response = await api.listDLQMessages(filters);
      // For now, we use mock data
      await new Promise(resolve => setTimeout(resolve, 500)); // Simulate API delay
      const mockMessages = generateMockDLQMessages();
      setMessages(mockMessages);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setLoading(false);
    }
  };

  const applyFiltersAndSearch = () => {
    let filtered = messages;

    // Apply text search
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      filtered = filtered.filter(msg => 
        msg.message_id.toLowerCase().includes(term) ||
        msg.topic.toLowerCase().includes(term) ||
        (msg.dlq_reason && msg.dlq_reason.toLowerCase().includes(term)) ||
        (msg.producer_id && msg.producer_id.toLowerCase().includes(term)) ||
        // Search in decoded payload
        (() => {
          try {
            return atob(msg.payload).toLowerCase().includes(term);
          } catch {
            return msg.payload.toLowerCase().includes(term);
          }
        })()
      );
    }

    // Apply filters
    if (filters.topic) {
      filtered = filtered.filter(msg => msg.topic.includes(filters.topic!));
    }
    
    if (filters.reason) {
      filtered = filtered.filter(msg => 
        msg.dlq_reason && msg.dlq_reason.toLowerCase().includes(filters.reason!.toLowerCase())
      );
    }
    
    if (filters.producer) {
      filtered = filtered.filter(msg => 
        msg.producer_id && msg.producer_id.includes(filters.producer!)
      );
    }
    
    if (filters.dateFrom) {
      const fromDate = new Date(filters.dateFrom);
      filtered = filtered.filter(msg => new Date(msg.dlq_timestamp) >= fromDate);
    }
    
    if (filters.dateTo) {
      const toDate = new Date(filters.dateTo);
      filtered = filtered.filter(msg => new Date(msg.dlq_timestamp) <= toDate);
    }
    
    if (filters.minAttempts !== undefined) {
      filtered = filtered.filter(msg => msg.attempts >= filters.minAttempts!);
    }
    
    if (filters.maxAttempts !== undefined) {
      filtered = filtered.filter(msg => msg.attempts <= filters.maxAttempts!);
    }

    setFilteredMessages(filtered);
  };

  const handleSelectMessage = (messageId: string, selected: boolean) => {
    const newSelected = new Set(selectedMessages);
    if (selected) {
      newSelected.add(messageId);
    } else {
      newSelected.delete(messageId);
    }
    setSelectedMessages(newSelected);
  };

  const handleSelectAll = (selected: boolean) => {
    if (selected) {
      setSelectedMessages(new Set(filteredMessages.map(msg => msg.message_id)));
    } else {
      setSelectedMessages(new Set());
    }
  };

  const handleBulkAction = async (action: 'replay' | 'delete') => {
    if (selectedMessages.size === 0) {
      showWarning('No messages selected', 'Please select at least one message to perform this action.');
      return;
    }
    
    setBulkAction(action);
  };

  const confirmBulkAction = async () => {
    if (!bulkAction || selectedMessages.size === 0) return;
    
    setActionLoading(true);
    
    try {
      // In a real app: 
      // if (bulkAction === 'replay') await api.replayDLQMessages(Array.from(selectedMessages));
      // if (bulkAction === 'delete') await api.deleteDLQMessages(Array.from(selectedMessages));
      
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      const actionName = bulkAction === 'replay' ? 'replayed' : 'deleted';
      showSuccess(
        `Messages ${actionName}`,
        `Successfully ${actionName} ${selectedMessages.size} message${selectedMessages.size > 1 ? 's' : ''}`
      );
      
      // Remove messages from local state if deleted
      if (bulkAction === 'delete') {
        setMessages(prev => prev.filter(msg => !selectedMessages.has(msg.message_id)));
      }
      
      setSelectedMessages(new Set());
      setBulkAction(null);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleSingleAction = async (messageId: string, action: 'replay' | 'delete') => {
    setActionLoading(true);
    
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      const message = messages.find(msg => msg.message_id === messageId);
      const actionName = action === 'replay' ? 'replayed' : 'deleted';
      
      showSuccess(
        `Message ${actionName}`,
        `Successfully ${actionName} message from ${message?.topic || 'topic'}`
      );
      
      // Remove from local state if deleted
      if (action === 'delete') {
        setMessages(prev => prev.filter(msg => msg.message_id !== messageId));
      }
      
      setSelectedMessages(prev => {
        const newSet = new Set(prev);
        newSet.delete(messageId);
        return newSet;
      });
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleViewDetails = (message: DLQMessage) => {
    setSelectedMessageForDetails(message);
  };

  // Get unique values for filter dropdowns
  const uniqueTopics = Array.from(new Set(messages.map(msg => msg.topic))).sort();
  const uniqueReasons = Array.from(new Set(messages.map(msg => msg.dlq_reason).filter(Boolean))) as string[];
  const uniqueProducers = Array.from(new Set(messages.map(msg => msg.producer_id).filter(Boolean))) as string[];

  const hasActiveFilters = Object.values(filters).some(value => 
    value !== undefined && value !== '' && value !== null
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">Dead Letter Queue</h1>
          <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
            Browse and manage messages that failed processing and were moved to the DLQ.
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
          <button
            type="button"
            onClick={loadMessages}
            className="btn btn-secondary inline-flex items-center"
            disabled={loading}
          >
            <ArrowPathIcon className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Summary Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <ExclamationTriangleIcon className="h-6 w-6 text-error-600 dark:text-error-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Total DLQ Messages
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {messages.length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-6 w-6 text-blue-600 dark:text-blue-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Filtered Results
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {filteredMessages.length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <CheckIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Selected
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {selectedMessages.size}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="w-6 h-6 bg-warning-600 rounded-full flex items-center justify-center">
                  <div className="w-2 h-2 bg-white rounded-full" />
                </div>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Unique Topics
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {uniqueTopics.length}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Search and Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-3 sm:space-y-0 sm:space-x-4">
        {/* Search */}
        <div className="relative flex-1 max-w-md">
          <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
            <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
          </div>
          <input
            type="text"
            placeholder="Search messages, topics, reasons..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="input pl-10"
          />
        </div>

        {/* Controls */}
        <div className="flex items-center space-x-2">
          <button
            type="button"
            className={`btn btn-secondary inline-flex items-center ${hasActiveFilters ? 'border-primary-500 text-primary-600' : ''}`}
            onClick={() => setShowFilters(true)}
          >
            <FunnelIcon className="h-4 w-4 mr-2" />
            Filters
            {hasActiveFilters && (
              <span className="ml-1 bg-primary-100 text-primary-800 text-xs rounded-full px-2 py-0.5">
                {Object.keys(filters).length}
              </span>
            )}
          </button>

          {selectedMessages.size > 0 && (
            <>
              <button
                type="button"
                onClick={() => handleBulkAction('replay')}
                className="btn btn-primary inline-flex items-center"
                disabled={actionLoading}
              >
                <ArrowPathIcon className="h-4 w-4 mr-2" />
                Replay ({selectedMessages.size})
              </button>
              <button
                type="button"
                onClick={() => handleBulkAction('delete')}
                className="btn btn-danger inline-flex items-center"
                disabled={actionLoading}
              >
                <TrashIcon className="h-4 w-4 mr-2" />
                Delete ({selectedMessages.size})
              </button>
            </>
          )}
        </div>
      </div>

      {/* Bulk Selection */}
      {filteredMessages.length > 0 && (
        <div className="flex items-center justify-between bg-gray-50 dark:bg-gray-800 rounded-lg px-4 py-3">
          <label className="flex items-center space-x-2">
            <input
              type="checkbox"
              checked={selectedMessages.size === filteredMessages.length}
              onChange={(e) => handleSelectAll(e.target.checked)}
              className="form-checkbox text-primary-600"
            />
            <span className="text-sm text-gray-700 dark:text-gray-300">
              Select all {filteredMessages.length} messages
            </span>
          </label>
          
          {selectedMessages.size > 0 && (
            <button
              onClick={() => setSelectedMessages(new Set())}
              className="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300"
            >
              Clear selection
            </button>
          )}
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <div className="text-center">
            <div className="loading-spinner h-8 w-8 mx-auto mb-4" />
            <p className="text-gray-600 dark:text-gray-400">Loading DLQ messages...</p>
          </div>
        </div>
      ) : filteredMessages.length === 0 ? (
        <div className="card p-8 text-center">
          <div className="mx-auto h-12 w-12 text-gray-400 dark:text-gray-500">
            <ExclamationTriangleIcon className="w-12 h-12" />
          </div>
          <h3 className="mt-4 text-sm font-medium text-gray-900 dark:text-white">
            {messages.length === 0 ? 'No DLQ messages' : 'No matching messages'}
          </h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            {messages.length === 0
              ? 'There are currently no messages in the dead letter queue.'
              : 'Try adjusting your search criteria or filters.'
            }
          </p>
          {hasActiveFilters && (
            <div className="mt-6">
              <button
                type="button"
                onClick={() => {
                  setSearchTerm('');
                  setFilters({});
                }}
                className="btn btn-primary"
              >
                Clear filters
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="space-y-4">
          {filteredMessages.map((message) => (
            <DLQMessageCard
              key={message.message_id}
              message={message}
              isSelected={selectedMessages.has(message.message_id)}
              onSelect={(selected) => handleSelectMessage(message.message_id, selected)}
              onReplay={() => handleSingleAction(message.message_id, 'replay')}
              onDelete={() => handleSingleAction(message.message_id, 'delete')}
              onViewDetails={() => handleViewDetails(message)}
            />
          ))}
        </div>
      )}

      {/* Filters Modal */}
      <DLQFilters
        isOpen={showFilters}
        onClose={() => setShowFilters(false)}
        currentFilters={filters}
        onApplyFilters={setFilters}
        onClearFilters={() => setFilters({})}
        availableTopics={uniqueTopics}
        availableReasons={uniqueReasons}
        availableProducers={uniqueProducers}
      />

      {/* Message Details Modal */}
      {selectedMessageForDetails && (
        <DLQMessageDetailsModal
          isOpen={!!selectedMessageForDetails}
          onClose={() => setSelectedMessageForDetails(null)}
          message={selectedMessageForDetails}
          onReplay={() => {
            handleSingleAction(selectedMessageForDetails.message_id, 'replay');
            setSelectedMessageForDetails(null);
          }}
          onDelete={() => {
            handleSingleAction(selectedMessageForDetails.message_id, 'delete');
            setSelectedMessageForDetails(null);
          }}
        />
      )}

      {/* Bulk Action Confirmation Modal */}
      <ConfirmationModal
        isOpen={!!bulkAction}
        onClose={() => setBulkAction(null)}
        onConfirm={confirmBulkAction}
        title={`${bulkAction === 'replay' ? 'Replay' : 'Delete'} Messages`}
        message={`Are you sure you want to ${bulkAction} ${selectedMessages.size} selected message${selectedMessages.size > 1 ? 's' : ''}? ${bulkAction === 'delete' ? 'This action cannot be undone.' : 'They will be reprocessed immediately.'}`}
        confirmText={bulkAction === 'replay' ? 'Replay Messages' : 'Delete Messages'}
        cancelText="Cancel"
        variant={bulkAction === 'delete' ? 'danger' : undefined}
        isLoading={actionLoading}
      />
    </div>
  );
}