import { useForm, Controller } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { Switch } from '@headlessui/react';
import { 
  createHeapSchema,
  formatBytes,
  type CreateHeapFormData
} from '../../lib/validation';
import type { HeapInfo } from '../../lib/types';

interface HeapFormProps {
  mode: 'create' | 'edit';
  initialData?: HeapInfo;
  onSubmit: (data: CreateHeapFormData) => Promise<void>;
  onCancel: () => void;
  isLoading?: boolean;
}

// Default values for create mode
const defaultValues: CreateHeapFormData = {
  topic: '',
  mode: 'MIN',
  partitions: 3,
  replication_factor: 2,
  top_k_bound: 100,
  retention_time: '24h',
  visibility_timeout_default: '30s',
  max_retries_default: 3,
  max_payload_bytes: 1024 * 1024, // 1MB
  compression_enabled: true,
  dlq_policy: {
    enabled: true,
    retention_time: '7d',
  },
};

export function HeapForm({ mode, initialData, onSubmit, onCancel, isLoading = false }: HeapFormProps) {
  const {
    register,
    handleSubmit,
    control,
    watch,
    formState: { errors, isSubmitting },
  } = useForm<CreateHeapFormData>({
    resolver: zodResolver(createHeapSchema),
    defaultValues: mode === 'create' ? defaultValues : {
      topic: initialData?.topic || '',
      mode: initialData?.mode || 'MIN',
      partitions: initialData?.partitions || 3,
      replication_factor: initialData?.replication_factor || 2,
      top_k_bound: initialData?.top_k_bound || 100,
      retention_time: initialData?.retention_time || '24h',
      visibility_timeout_default: initialData?.visibility_timeout_default || '30s',
      max_retries_default: initialData?.max_retries_default || 3,
      max_payload_bytes: initialData?.max_payload_bytes || 1024 * 1024,
      compression_enabled: initialData?.compression_enabled ?? true,
      dlq_policy: {
        enabled: initialData?.dlq_policy?.enabled ?? true,
        retention_time: initialData?.dlq_policy?.retention_time || '7d',
      },
    },
  });

  const dlqEnabled = watch('dlq_policy.enabled');

  const onFormSubmit = async (data: CreateHeapFormData) => {
    try {
      await onSubmit(data);
    } catch (error) {
      // Error handling is done by the parent component
      console.error('Form submission failed:', error);
    }
  };

  return (
    <form onSubmit={handleSubmit(onFormSubmit)} className="space-y-6">
      {/* Basic Configuration */}
      <div className="space-y-4">
        <div>
          <h4 className="text-lg font-medium text-gray-900 dark:text-white mb-4">
            Basic Configuration
          </h4>
        </div>

        {/* Topic Name */}
        <div>
          <label className="label" htmlFor="topic">
            Topic Name *
          </label>
          <input
            {...register('topic')}
            type="text"
            className={`input ${errors.topic ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            placeholder="my-work-queue"
            disabled={mode === 'edit' || isLoading}
          />
          {errors.topic && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.topic.message}
            </p>
          )}
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Topic name can only contain letters, numbers, underscores, and hyphens.
          </p>
        </div>

        {/* Mode */}
        <div>
          <label className="label" htmlFor="mode">
            Heap Mode *
          </label>
          <select
            {...register('mode')}
            className={`input ${errors.mode ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            disabled={mode === 'edit' || isLoading}
          >
            <option value="MIN">MIN - Lower priority values come first</option>
            <option value="MAX">MAX - Higher priority values come first</option>
          </select>
          {errors.mode && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.mode.message}
            </p>
          )}
        </div>

        {/* Partitions (create only) */}
        {mode === 'create' && (
          <div>
            <label className="label" htmlFor="partitions">
              Partitions *
            </label>
            <input
              {...register('partitions', { valueAsNumber: true })}
              type="number"
              min="1"
              max="1000"
              className={`input ${errors.partitions ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              disabled={isLoading}
            />
            {errors.partitions && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.partitions.message}
              </p>
            )}
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Number of partitions for distributing messages. Cannot be changed after creation.
            </p>
          </div>
        )}

        {/* Replication Factor (create only) */}
        {mode === 'create' && (
          <div>
            <label className="label" htmlFor="replication_factor">
              Replication Factor *
            </label>
            <select
              {...register('replication_factor', { valueAsNumber: true })}
              className={`input ${errors.replication_factor ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              disabled={isLoading}
            >
              <option value={2}>2 - Standard reliability</option>
              <option value={3}>3 - High reliability (recommended)</option>
              <option value={5}>5 - Maximum reliability</option>
            </select>
            {errors.replication_factor && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.replication_factor.message}
              </p>
            )}
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Number of replicas for each partition. Cannot be changed after creation.
            </p>
          </div>
        )}

        {/* Top-K Bound (create only) */}
        {mode === 'create' && (
          <div>
            <label className="label" htmlFor="top_k_bound">
              Top-K Bound *
            </label>
            <input
              {...register('top_k_bound', { valueAsNumber: true })}
              type="number"
              min="1"
              max="10000"
              className={`input ${errors.top_k_bound ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              disabled={isLoading}
            />
            {errors.top_k_bound && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.top_k_bound.message}
              </p>
            )}
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Maximum staleness bound for global priority ordering.
            </p>
          </div>
        )}
      </div>

      {/* Message Configuration */}
      <div className="space-y-4">
        <div>
          <h4 className="text-lg font-medium text-gray-900 dark:text-white mb-4">
            Message Configuration
          </h4>
        </div>

        {/* Retention Time */}
        <div>
          <label className="label" htmlFor="retention_time">
            Retention Time *
          </label>
          <input
            {...register('retention_time')}
            type="text"
            className={`input ${errors.retention_time ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            placeholder="24h"
            disabled={isLoading}
          />
          {errors.retention_time && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.retention_time.message}
            </p>
          )}
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            How long messages are kept. Examples: "1h30m", "24h", "7d"
          </p>
        </div>

        {/* Visibility Timeout */}
        <div>
          <label className="label" htmlFor="visibility_timeout_default">
            Default Visibility Timeout *
          </label>
          <input
            {...register('visibility_timeout_default')}
            type="text"
            className={`input ${errors.visibility_timeout_default ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            placeholder="30s"
            disabled={isLoading}
          />
          {errors.visibility_timeout_default && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.visibility_timeout_default.message}
            </p>
          )}
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            How long messages are invisible after being popped. Examples: "30s", "5m"
          </p>
        </div>

        {/* Max Retries */}
        <div>
          <label className="label" htmlFor="max_retries_default">
            Default Max Retries *
          </label>
          <input
            {...register('max_retries_default', { valueAsNumber: true })}
            type="number"
            min="0"
            max="100"
            className={`input ${errors.max_retries_default ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            disabled={isLoading}
          />
          {errors.max_retries_default && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.max_retries_default.message}
            </p>
          )}
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Number of times to retry failed messages before moving to DLQ.
          </p>
        </div>

        {/* Max Payload Size */}
        <div>
          <label className="label" htmlFor="max_payload_bytes">
            Max Payload Size (bytes) *
          </label>
          <input
            {...register('max_payload_bytes', { valueAsNumber: true })}
            type="number"
            min="1"
            max={100 * 1024 * 1024}
            className={`input ${errors.max_payload_bytes ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
            disabled={isLoading}
          />
          {errors.max_payload_bytes && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.max_payload_bytes.message}
            </p>
          )}
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Maximum size of message payloads. Current: {watch('max_payload_bytes') ? formatBytes(watch('max_payload_bytes') || 0) : '0 Bytes'}
          </p>
        </div>

        {/* Compression */}
        <div>
          <div className="flex items-center justify-between">
            <div>
              <label className="label mb-0" htmlFor="compression_enabled">
                Enable Compression
              </label>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Compress message payloads to reduce storage and network usage.
              </p>
            </div>
            <Controller
              name="compression_enabled"
              control={control}
              render={({ field: { value, onChange } }) => (
                <Switch
                  checked={value}
                  onChange={onChange}
                  disabled={isLoading}
                  className={`${
                    value ? 'bg-primary-600' : 'bg-gray-200 dark:bg-gray-700'
                  } relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50`}
                >
                  <span
                    className={`${
                      value ? 'translate-x-5' : 'translate-x-0'
                    } pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out`}
                  />
                </Switch>
              )}
            />
          </div>
        </div>
      </div>

      {/* Dead Letter Queue Configuration */}
      <div className="space-y-4">
        <div>
          <h4 className="text-lg font-medium text-gray-900 dark:text-white mb-4">
            Dead Letter Queue
          </h4>
        </div>

        {/* DLQ Enabled */}
        <div>
          <div className="flex items-center justify-between">
            <div>
              <label className="label mb-0" htmlFor="dlq_policy.enabled">
                Enable Dead Letter Queue
              </label>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Store messages that exceed max retries for manual inspection.
              </p>
            </div>
            <Controller
              name="dlq_policy.enabled"
              control={control}
              render={({ field: { value, onChange } }) => (
                <Switch
                  checked={value}
                  onChange={onChange}
                  disabled={isLoading}
                  className={`${
                    value ? 'bg-primary-600' : 'bg-gray-200 dark:bg-gray-700'
                  } relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50`}
                >
                  <span
                    className={`${
                      value ? 'translate-x-5' : 'translate-x-0'
                    } pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out`}
                  />
                </Switch>
              )}
            />
          </div>
        </div>

        {/* DLQ Retention Time */}
        {dlqEnabled && (
          <div>
            <label className="label" htmlFor="dlq_policy.retention_time">
              DLQ Retention Time *
            </label>
            <input
              {...register('dlq_policy.retention_time')}
              type="text"
              className={`input ${errors.dlq_policy?.retention_time ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              placeholder="7d"
              disabled={isLoading}
            />
            {errors.dlq_policy?.retention_time && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.dlq_policy.retention_time.message}
              </p>
            )}
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              How long to keep messages in the dead letter queue.
            </p>
          </div>
        )}
      </div>

      {/* Form Actions */}
      <div className="flex justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700">
        <button
          type="button"
          onClick={onCancel}
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
              {mode === 'create' ? 'Creating...' : 'Updating...'}
            </div>
          ) : (
            mode === 'create' ? 'Create Heap' : 'Update Heap'
          )}
        </button>
      </div>
    </form>
  );
}
