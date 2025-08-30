
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import {
  FunnelIcon,
  XMarkIcon,
  CalendarIcon,
  HashtagIcon,
  UserIcon,
} from '@heroicons/react/24/outline';
import { Modal } from '../ui/Modal';
import type { DLQBrowserFilters } from '../../lib/types';

const filtersSchema = z.object({
  topic: z.string().optional(),
  reason: z.string().optional(),
  dateFrom: z.string().optional(),
  dateTo: z.string().optional(),
  producer: z.string().optional(),
  minAttempts: z.number().min(0).optional(),
  maxAttempts: z.number().min(0).max(1000).optional(),
}).refine((data) => {
  if (data.dateFrom && data.dateTo) {
    return new Date(data.dateFrom) <= new Date(data.dateTo);
  }
  return true;
}, {
  message: "End date must be after start date",
  path: ["dateTo"],
}).refine((data) => {
  if (data.minAttempts !== undefined && data.maxAttempts !== undefined) {
    return data.minAttempts <= data.maxAttempts;
  }
  return true;
}, {
  message: "Maximum attempts must be greater than or equal to minimum attempts",
  path: ["maxAttempts"],
});

type FiltersFormData = z.infer<typeof filtersSchema>;

interface DLQFiltersProps {
  isOpen: boolean;
  onClose: () => void;
  currentFilters: DLQBrowserFilters;
  onApplyFilters: (filters: DLQBrowserFilters) => void;
  onClearFilters: () => void;
  availableTopics?: string[];
  availableReasons?: string[];
  availableProducers?: string[];
}

export function DLQFilters({
  isOpen,
  onClose,
  currentFilters,
  onApplyFilters,
  onClearFilters,
  availableTopics = [],
  availableReasons = [],
  availableProducers = [],
}: DLQFiltersProps) {
  const {
    register,
    handleSubmit,
    formState: { errors },
    reset,
  } = useForm<FiltersFormData>({
    resolver: zodResolver(filtersSchema),
    defaultValues: {
      topic: currentFilters.topic || '',
      reason: currentFilters.reason || '',
      dateFrom: currentFilters.dateFrom || '',
      dateTo: currentFilters.dateTo || '',
      producer: currentFilters.producer || '',
      minAttempts: currentFilters.minAttempts || undefined,
      maxAttempts: currentFilters.maxAttempts || undefined,
    },
  });



  const onSubmit = (data: FiltersFormData) => {
    const filters: DLQBrowserFilters = {};
    
    // Only include non-empty values
    Object.entries(data).forEach(([key, value]) => {
      if (value !== undefined && value !== '') {
        (filters as any)[key] = value;
      }
    });
    
    onApplyFilters(filters);
    onClose();
  };

  const handleClear = () => {
    reset({
      topic: '',
      reason: '',
      dateFrom: '',
      dateTo: '',
      producer: '',
      minAttempts: undefined,
      maxAttempts: undefined,
    });
    onClearFilters();
    onClose();
  };

  const hasActiveFilters = Object.values(currentFilters).some(value => 
    value !== undefined && value !== '' && value !== null
  );

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="Filter DLQ Messages" size="lg">
      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        {/* Topic Filter */}
        <div>
          <label htmlFor="topic" className="label">
            Topic
          </label>
          {availableTopics.length > 0 ? (
            <select {...register('topic')} className="input">
              <option value="">All topics</option>
              {availableTopics.map((topic) => (
                <option key={topic} value={topic}>
                  {topic}
                </option>
              ))}
            </select>
          ) : (
            <input
              {...register('topic')}
              type="text"
              className="input"
              placeholder="Enter topic name"
            />
          )}
          {errors.topic && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.topic.message}
            </p>
          )}
        </div>

        {/* Reason Filter */}
        <div>
          <label htmlFor="reason" className="label">
            Failure Reason
          </label>
          {availableReasons.length > 0 ? (
            <select {...register('reason')} className="input">
              <option value="">All reasons</option>
              {availableReasons.map((reason) => (
                <option key={reason} value={reason}>
                  {reason}
                </option>
              ))}
            </select>
          ) : (
            <input
              {...register('reason')}
              type="text"
              className="input"
              placeholder="e.g., timeout, validation_error"
            />
          )}
          {errors.reason && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.reason.message}
            </p>
          )}
        </div>

        {/* Date Range */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div>
            <label htmlFor="dateFrom" className="label">
              Failed After
            </label>
            <div className="relative">
              <input
                {...register('dateFrom')}
                type="datetime-local"
                className="input"
              />
              <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                <CalendarIcon className="w-5 h-5 text-gray-400" />
              </div>
            </div>
            {errors.dateFrom && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.dateFrom.message}
              </p>
            )}
          </div>

          <div>
            <label htmlFor="dateTo" className="label">
              Failed Before
            </label>
            <div className="relative">
              <input
                {...register('dateTo')}
                type="datetime-local"
                className="input"
              />
              <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                <CalendarIcon className="w-5 h-5 text-gray-400" />
              </div>
            </div>
            {errors.dateTo && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.dateTo.message}
              </p>
            )}
          </div>
        </div>

        {/* Producer Filter */}
        <div>
          <label htmlFor="producer" className="label">
            Producer ID
          </label>
          {availableProducers.length > 0 ? (
            <select {...register('producer')} className="input">
              <option value="">All producers</option>
              {availableProducers.map((producer) => (
                <option key={producer} value={producer}>
                  {producer}
                </option>
              ))}
            </select>
          ) : (
            <div className="relative">
              <input
                {...register('producer')}
                type="text"
                className="input pl-10"
                placeholder="Enter producer ID"
              />
              <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                <UserIcon className="w-5 h-5 text-gray-400" />
              </div>
            </div>
          )}
          {errors.producer && (
            <p className="mt-1 text-sm text-error-600 dark:text-error-400">
              {errors.producer.message}
            </p>
          )}
        </div>

        {/* Attempts Range */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div>
            <label htmlFor="minAttempts" className="label">
              Min Attempts
            </label>
            <div className="relative">
              <input
                {...register('minAttempts', { valueAsNumber: true })}
                type="number"
                min="0"
                className="input pl-10"
                placeholder="0"
              />
              <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                <HashtagIcon className="w-5 h-5 text-gray-400" />
              </div>
            </div>
            {errors.minAttempts && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.minAttempts.message}
              </p>
            )}
          </div>

          <div>
            <label htmlFor="maxAttempts" className="label">
              Max Attempts
            </label>
            <div className="relative">
              <input
                {...register('maxAttempts', { valueAsNumber: true })}
                type="number"
                min="0"
                max="1000"
                className="input pl-10"
                placeholder="1000"
              />
              <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                <HashtagIcon className="w-5 h-5 text-gray-400" />
              </div>
            </div>
            {errors.maxAttempts && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.maxAttempts.message}
              </p>
            )}
          </div>
        </div>

        {/* Active Filters Summary */}
        {hasActiveFilters && (
          <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
            <div className="flex items-start justify-between">
              <div>
                <h4 className="text-sm font-medium text-blue-800 dark:text-blue-200">
                  Active Filters
                </h4>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(currentFilters).map(([key, value]) => {
                    if (!value || value === '') return null;
                    return (
                      <span
                        key={key}
                        className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-200"
                      >
                        {key}: {value.toString()}
                      </span>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Actions */}
        <div className="flex justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700">
          <button
            type="button"
            onClick={handleClear}
            className="btn btn-secondary inline-flex items-center"
          >
            <XMarkIcon className="w-4 h-4 mr-2" />
            Clear All
          </button>
          <button
            type="button"
            onClick={onClose}
            className="btn btn-secondary"
          >
            Cancel
          </button>
          <button
            type="submit"
            className="btn btn-primary inline-flex items-center"
          >
            <FunnelIcon className="w-4 h-4 mr-2" />
            Apply Filters
          </button>
        </div>
      </form>
    </Modal>
  );
}
