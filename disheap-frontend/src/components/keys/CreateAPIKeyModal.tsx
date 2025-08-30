import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { CalendarIcon } from '@heroicons/react/24/outline';
import { Modal } from '../ui/Modal';
import { useNotifications } from '../../store/app';
import { api } from '../../lib/api';
import { formatAPIError } from '../../lib/errors';
import { createAPIKeySchema, type CreateAPIKeyFormData } from '../../lib/validation';

interface CreateAPIKeyModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateAPIKeyModal({ isOpen, onClose, onSuccess }: CreateAPIKeyModalProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const { showSuccess, showError } = useNotifications();

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
    reset,
  } = useForm<CreateAPIKeyFormData>({
    resolver: zodResolver(createAPIKeySchema),
    defaultValues: {
      name: '',
    },
  });

  const [customExpiry, setCustomExpiry] = useState(false);
  const [expiryDays, setExpiryDays] = useState<number>(30);

  const handleSubmit_ = async (data: CreateAPIKeyFormData) => {
    setIsLoading(true);
    
    try {
      const expiryDate = customExpiry && expiryDays > 0 
        ? new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000).toISOString()
        : undefined;
      
      const response = await api.createAPIKey({
        name: data.name,
        expires_at: expiryDate,
      });

      setCreatedKey(response.key);
      showSuccess(
        'API Key Created',
        `Successfully created API key "${data.name}"`
      );
      
      onSuccess();
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    if (!isLoading && !isSubmitting) {
      reset();
      setCreatedKey(null);
      setCustomExpiry(false);
      setExpiryDays(30);
      onClose();
    }
  };

  const copyKeyToClipboard = async () => {
    if (createdKey) {
      try {
        await navigator.clipboard.writeText(createdKey);
        showSuccess('Copied!', 'API key copied to clipboard');
      } catch (err) {
        showError('Copy failed', 'Unable to copy to clipboard');
      }
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={createdKey ? 'API Key Created' : 'Create New API Key'}
      size="md"
      showCloseButton={!isLoading && !isSubmitting}
    >
      {createdKey ? (
        // Success view - show the created key
        <div className="space-y-6">
          <div className="bg-success-50 dark:bg-success-900/20 border border-success-200 dark:border-success-800 rounded-lg p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-success-400" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-success-800 dark:text-success-200">
                  API Key Created Successfully
                </h3>
                <p className="mt-2 text-sm text-success-700 dark:text-success-300">
                  Your API key has been created. Make sure to copy it now - you won't be able to see it again!
                </p>
              </div>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Your API Key
            </label>
            <div className="flex items-center space-x-2">
              <code className="flex-1 p-3 bg-gray-50 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-mono break-all">
                {createdKey}
              </code>
              <button
                onClick={copyKeyToClipboard}
                className="btn btn-secondary btn-sm"
              >
                Copy
              </button>
            </div>
          </div>

          <div className="bg-warning-50 dark:bg-warning-900/20 border border-warning-200 dark:border-warning-800 rounded-lg p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-warning-400" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-warning-800 dark:text-warning-200">
                  Important Security Notice
                </h3>
                <p className="mt-2 text-sm text-warning-700 dark:text-warning-300">
                  Store this key securely. Anyone with this key can access your account. If you lose it, you'll need to create a new one.
                </p>
              </div>
            </div>
          </div>

          <div className="flex justify-end">
            <button
              onClick={handleClose}
              className="btn btn-primary"
            >
              Done
            </button>
          </div>
        </div>
      ) : (
        // Creation form
        <form onSubmit={handleSubmit(handleSubmit_)} className="space-y-6">
          {/* API Key Name */}
          <div>
            <label htmlFor="name" className="label">
              API Key Name *
            </label>
            <input
              {...register('name')}
              type="text"
              className={`input ${errors.name ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              placeholder="e.g., Production API, Mobile App Key"
              disabled={isLoading || isSubmitting}
            />
            {errors.name && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.name.message}
              </p>
            )}
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Choose a descriptive name to help you identify this key later.
            </p>
          </div>

          {/* Expiry Options */}
          <div>
            <label className="label">Expiration</label>
            <div className="space-y-3">
              <label className="flex items-center">
                <input
                  type="radio"
                  name="expiry"
                  checked={!customExpiry}
                  onChange={() => setCustomExpiry(false)}
                  className="form-radio text-primary-600"
                  disabled={isLoading || isSubmitting}
                />
                <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                  Never expires (not recommended for production)
                </span>
              </label>
              
              <label className="flex items-center">
                <input
                  type="radio"
                  name="expiry"
                  checked={customExpiry}
                  onChange={() => setCustomExpiry(true)}
                  className="form-radio text-primary-600"
                  disabled={isLoading || isSubmitting}
                />
                <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                  Custom expiration
                </span>
              </label>
              
              {customExpiry && (
                <div className="ml-6 flex items-center space-x-2">
                  <input
                    type="number"
                    min="1"
                    max="365"
                    value={expiryDays}
                    onChange={(e) => setExpiryDays(parseInt(e.target.value) || 30)}
                    className="input w-20"
                    disabled={isLoading || isSubmitting}
                  />
                  <span className="text-sm text-gray-700 dark:text-gray-300">days</span>
                  <div className="flex items-center text-sm text-gray-500 dark:text-gray-400">
                    <CalendarIcon className="w-4 h-4 mr-1" />
                    Expires on {new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000).toLocaleDateString()}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Form Actions */}
          <div className="flex justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700">
            <button
              type="button"
              onClick={handleClose}
              className="btn btn-secondary"
              disabled={isLoading || isSubmitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="btn btn-primary"
              disabled={isLoading || isSubmitting}
            >
              {isLoading || isSubmitting ? (
                <div className="flex items-center">
                  <div className="loading-spinner h-4 w-4 mr-2" />
                  Creating...
                </div>
              ) : (
                'Create API Key'
              )}
            </button>
          </div>
        </form>
      )}
    </Modal>
  );
}
