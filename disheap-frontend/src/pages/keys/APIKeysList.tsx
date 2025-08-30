import { useState, useEffect } from 'react';
import { MagnifyingGlassIcon, FunnelIcon, ShieldCheckIcon } from '@heroicons/react/24/outline';
import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { APIKeyCard } from '../../components/keys/APIKeyCard';
import { CreateAPIKeyModal } from '../../components/keys/CreateAPIKeyModal';
import { APIKeyUsageModal } from '../../components/keys/APIKeyUsageModal';
import { ConfirmationModal } from '../../components/ui/Modal';
import { useNotifications } from '../../store/app';
import { formatAPIError } from '../../lib/errors';
import type { APIKey } from '../../lib/types';

// Generate mock API keys for demonstration
function generateMockAPIKeys(): APIKey[] {
  const keys: APIKey[] = [
    {
      id: 'key_1',
      name: 'Production API',
      key: 'dsh_live_1234567890abcdef1234567890abcdef12345678',
      created_at: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(), // 30 days ago
      expires_at: new Date(Date.now() + 335 * 24 * 60 * 60 * 1000).toISOString(), // ~1 year from now
      last_used_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(), // 2 hours ago
      usage: {
        total_requests: 125847,
        requests_last_24h: 2341,
        error_count: 23,
      },
    },
    {
      id: 'key_2',
      name: 'Mobile App',
      key: 'dsh_live_abcdef1234567890abcdef1234567890abcdef12',
      created_at: new Date(Date.now() - 15 * 24 * 60 * 60 * 1000).toISOString(), // 15 days ago
      expires_at: null,
      last_used_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(), // 30 minutes ago
      usage: {
        total_requests: 67892,
        requests_last_24h: 1876,
        error_count: 12,
      },
    },
    {
      id: 'key_3',
      name: 'Development Testing',
      key: 'dsh_test_9876543210fedcba9876543210fedcba98765432',
      created_at: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), // 7 days ago
      expires_at: new Date(Date.now() + 23 * 24 * 60 * 60 * 1000).toISOString(), // 23 days from now
      last_used_at: new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString(), // 45 days ago (inactive)
      usage: {
        total_requests: 234,
        requests_last_24h: 0,
        error_count: 5,
      },
    },
    {
      id: 'key_4',
      name: 'Analytics Service',
      key: 'dsh_live_fedcba9876543210fedcba9876543210fedcba98',
      created_at: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString(), // 60 days ago
      expires_at: new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString(), // Expired 5 days ago
      last_used_at: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(), // 10 days ago
      usage: {
        total_requests: 45123,
        requests_last_24h: 0,
        error_count: 234,
      },
    },
  ];
  
  return keys;
}

export function APIKeysList() {
  useDocumentTitle('API Keys');
  
  const [apiKeys, setApiKeys] = useState<APIKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [revokeKey, setRevokeKey] = useState<APIKey | null>(null);
  const [usageKey, setUsageKey] = useState<APIKey | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  
  const { showSuccess, showError } = useNotifications();

  // Load API keys on component mount
  useEffect(() => {
    loadAPIKeys();
  }, []);

  const loadAPIKeys = async () => {
    try {
      setLoading(true);
      
      // In a real app, this would be: const response = await api.listAPIKeys();
      // For now, we use mock data
      await new Promise(resolve => setTimeout(resolve, 500)); // Simulate API delay
      const mockKeys = generateMockAPIKeys();
      setApiKeys(mockKeys);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRevokeKey = async (key: APIKey) => {
    if (!revokeKey) return;
    
    setActionLoading(`revoke-${key.id}`);
    
    try {
      // In a real app: await api.revokeAPIKey(key.id);
      await new Promise(resolve => setTimeout(resolve, 1000)); // Simulate API call
      
      showSuccess(
        'API Key Revoked',
        `Successfully revoked API key "${key.name}"`
      );
      
      // Remove from local state
      setApiKeys(prev => prev.filter(k => k.id !== key.id));
      setRevokeKey(null);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setActionLoading(null);
    }
  };

  const handleViewUsage = (key: APIKey) => {
    setUsageKey(key);
  };

  // Filter API keys based on search term
  const filteredKeys = apiKeys.filter(key =>
    key.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    key.key.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Calculate summary statistics
  const totalRequests = apiKeys.reduce((sum, key) => sum + (key.usage?.total_requests || 0), 0);
  const activeKeys = apiKeys.filter(key => {
    const isExpired = key.expires_at && new Date(key.expires_at) < new Date();
    return !isExpired;
  }).length;
  const recentlyUsedKeys = apiKeys.filter(key => {
    if (!key.last_used_at) return false;
    const daysSinceUse = (Date.now() - new Date(key.last_used_at).getTime()) / (1000 * 60 * 60 * 24);
    return daysSinceUse <= 7;
  }).length;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">API Keys</h1>
          <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
            Manage your API keys for secure access to the Disheap API.
          </p>
        </div>
        <div className="mt-4 sm:mt-0 sm:ml-16 sm:flex-none">
          <button
            type="button"
            onClick={() => setShowCreateModal(true)}
            className="btn btn-primary"
          >
            Create API Key
          </button>
        </div>
      </div>

      {/* Summary Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-white dark:bg-gray-800 overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <ShieldCheckIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Total Keys
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {apiKeys.length}
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
                <div className="w-6 h-6 bg-success-600 rounded-full flex items-center justify-center">
                  <div className="w-2 h-2 bg-white rounded-full" />
                </div>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Active Keys
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {activeKeys}
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
                    Recently Used
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {recentlyUsedKeys}
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
                <div className="w-6 h-6 bg-blue-600 rounded-full flex items-center justify-center">
                  <div className="w-2 h-2 bg-white rounded-full" />
                </div>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                    Total Requests
                  </dt>
                  <dd className="text-lg font-medium text-gray-900 dark:text-white">
                    {totalRequests.toLocaleString()}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Search and Filters */}
      {apiKeys.length > 0 && (
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-3 sm:space-y-0 sm:space-x-4">
          {/* Search */}
          <div className="relative flex-1 max-w-md">
            <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
              <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search API keys..."
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
                showError('Not implemented', 'Advanced filtering is not yet implemented');
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
            <p className="text-gray-600 dark:text-gray-400">Loading API keys...</p>
          </div>
        </div>
      ) : filteredKeys.length === 0 ? (
        <div className="card p-8 text-center">
          <div className="mx-auto h-12 w-12 text-gray-400 dark:text-gray-500">
            <ShieldCheckIcon className="w-12 h-12" />
          </div>
          <h3 className="mt-4 text-sm font-medium text-gray-900 dark:text-white">
            {apiKeys.length === 0 ? 'No API keys' : 'No matching keys'}
          </h3>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
            {apiKeys.length === 0 
              ? 'Get started by creating your first API key for secure access.'
              : 'Try adjusting your search criteria.'
            }
          </p>
          {apiKeys.length === 0 && (
            <div className="mt-6">
              <button
                type="button"
                onClick={() => setShowCreateModal(true)}
                className="btn btn-primary"
              >
                Create Your First API Key
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          {filteredKeys.map((key) => (
            <APIKeyCard
              key={key.id}
              apiKey={key}
              onRevoke={() => setRevokeKey(key)}
              onViewUsage={() => handleViewUsage(key)}
            />
          ))}
        </div>
      )}

      {/* Create API Key Modal */}
      <CreateAPIKeyModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onSuccess={loadAPIKeys}
      />

      {/* Usage Analytics Modal */}
      {usageKey && (
        <APIKeyUsageModal
          isOpen={!!usageKey}
          onClose={() => setUsageKey(null)}
          apiKey={usageKey}
        />
      )}

      {/* Revoke Confirmation Modal */}
      <ConfirmationModal
        isOpen={!!revokeKey}
        onClose={() => setRevokeKey(null)}
        onConfirm={() => revokeKey && handleRevokeKey(revokeKey)}
        title="Revoke API Key"
        message={`Are you sure you want to revoke the API key "${revokeKey?.name}"? This action cannot be undone and will immediately stop all applications using this key.`}
        confirmText="Revoke Key"
        cancelText="Cancel"
        variant="danger"
        isLoading={actionLoading === `revoke-${revokeKey?.id}`}
      />
    </div>
  );
}