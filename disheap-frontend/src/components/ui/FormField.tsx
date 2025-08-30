import type { ReactNode } from 'react';
import type { FieldError, UseFormRegisterReturn } from 'react-hook-form';
import { ExclamationCircleIcon, CheckCircleIcon, InformationCircleIcon } from '@heroicons/react/24/outline';

interface FormFieldProps {
  label: string;
  name: string;
  type?: 'text' | 'email' | 'password' | 'number' | 'textarea' | 'select' | 'checkbox';
  register?: UseFormRegisterReturn;
  error?: FieldError;
  children?: ReactNode; // For custom input elements like select dropdowns
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  helpText?: string;
  success?: boolean;
  successMessage?: string;
  options?: Array<{ value: string | number; label: string }>;
  rows?: number; // For textarea
  min?: number;
  max?: number;
  step?: number;
  className?: string;
}

export function FormField({
  label,
  name,
  type = 'text',
  register,
  error,
  children,
  placeholder,
  disabled = false,
  required = false,
  helpText,
  success = false,
  successMessage,
  options = [],
  rows = 4,
  min,
  max,
  step,
  className = '',
}: FormFieldProps) {
  const hasError = !!error;
  const hasSuccess = success && !hasError;

  const inputClasses = `
    block w-full rounded-md shadow-sm
    ${hasError
      ? 'border-error-300 dark:border-error-600 text-error-900 dark:text-error-100 placeholder-error-300 dark:placeholder-error-400 focus:border-error-500 focus:ring-error-500'
      : hasSuccess
      ? 'border-success-300 dark:border-success-600 text-success-900 dark:text-success-100 focus:border-success-500 focus:ring-success-500'
      : 'border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-gray-500 focus:border-primary-500 focus:ring-primary-500'
    }
    ${disabled ? 'bg-gray-50 dark:bg-gray-700/50 cursor-not-allowed opacity-75' : 'bg-white dark:bg-gray-800'}
    sm:text-sm focus:outline-none focus:ring-1 transition-colors
  `;

  const renderInput = () => {
    if (children) {
      return children;
    }

    switch (type) {
      case 'textarea':
        return (
          <textarea
            id={name}
            {...register}
            rows={rows}
            className={inputClasses.trim()}
            placeholder={placeholder}
            disabled={disabled}
          />
        );

      case 'select':
        return (
          <select
            id={name}
            {...register}
            className={inputClasses.trim()}
            disabled={disabled}
          >
            <option value="">{placeholder || `Select ${label}`}</option>
            {options.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        );

      case 'checkbox':
        return (
          <div className="flex items-center">
            <input
              id={name}
              type="checkbox"
              {...register}
              className={`
                h-4 w-4 rounded border-gray-300 dark:border-gray-600 
                ${hasError
                  ? 'text-error-600 focus:ring-error-500'
                  : 'text-primary-600 focus:ring-primary-500'
                }
                dark:bg-gray-800 dark:checked:bg-primary-600
                ${disabled ? 'cursor-not-allowed opacity-75' : ''}
              `}
              disabled={disabled}
            />
            <label htmlFor={name} className="ml-3 text-sm text-gray-700 dark:text-gray-300">
              {label}
              {required && <span className="text-error-500 ml-1">*</span>}
            </label>
          </div>
        );

      default:
        return (
          <input
            id={name}
            type={type}
            {...register}
            min={min}
            max={max}
            step={step}
            className={inputClasses.trim()}
            placeholder={placeholder}
            disabled={disabled}
          />
        );
    }
  };

  return (
    <div className={className}>
      {type !== 'checkbox' && (
        <label htmlFor={name} className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          {label}
          {required && <span className="text-error-500 ml-1">*</span>}
        </label>
      )}

      <div className="relative">
        {renderInput()}
        
        {/* Status Icons */}
        {(hasError || hasSuccess) && (
          <div className="absolute inset-y-0 right-0 pr-3 flex items-center pointer-events-none">
            {hasError && (
              <ExclamationCircleIcon className="h-5 w-5 text-error-500" aria-hidden="true" />
            )}
            {hasSuccess && (
              <CheckCircleIcon className="h-5 w-5 text-success-500" aria-hidden="true" />
            )}
          </div>
        )}
      </div>

      {/* Error Message */}
      {hasError && (
        <div className="mt-2 flex items-center text-sm text-error-600 dark:text-error-400">
          <ExclamationCircleIcon className="h-4 w-4 mr-1 flex-shrink-0" />
          <span>{error.message}</span>
        </div>
      )}

      {/* Success Message */}
      {hasSuccess && successMessage && (
        <div className="mt-2 flex items-center text-sm text-success-600 dark:text-success-400">
          <CheckCircleIcon className="h-4 w-4 mr-1 flex-shrink-0" />
          <span>{successMessage}</span>
        </div>
      )}

      {/* Help Text */}
      {helpText && !hasError && (
        <div className="mt-2 flex items-center text-sm text-gray-500 dark:text-gray-400">
          <InformationCircleIcon className="h-4 w-4 mr-1 flex-shrink-0" />
          <span>{helpText}</span>
        </div>
      )}
    </div>
  );
}
