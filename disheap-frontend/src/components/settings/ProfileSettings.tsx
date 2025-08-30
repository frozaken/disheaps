import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import {
  UserCircleIcon,
  CheckIcon,
  ExclamationTriangleIcon,
} from '@heroicons/react/24/outline';
import { useAuth } from '../../store/auth';
import { useNotifications } from '../../store/app';
import { formatAPIError } from '../../lib/errors';
import { updateProfileSchema, type UpdateProfileFormData } from '../../lib/validation';

export function ProfileSettings() {
  const [isLoading, setIsLoading] = useState(false);
  const { user } = useAuth();
  const { showSuccess, showError } = useNotifications();

  const {
    register,
    handleSubmit,
    formState: { errors, isDirty },
    reset,
  } = useForm<UpdateProfileFormData>({
    resolver: zodResolver(updateProfileSchema),
    defaultValues: {
      name: user?.name || '',
      email: user?.email || '',
    },
  });

  const onSubmit = async (data: UpdateProfileFormData) => {
    if (!user) return;
    
    setIsLoading(true);
    
    try {
      // In a real app: await api.updateProfile(data);
      // For now, simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // In a real app, the user would be updated in the store after API success
      // For demo purposes, we'll just show a success message
      
      showSuccess(
        'Profile Updated',
        'Your profile has been updated successfully.'
      );
      
      // Reset form dirty state
      reset(data);
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCancel = () => {
    reset({
      name: user?.name || '',
      email: user?.email || '',
    });
  };

  if (!user) {
    return (
      <div className="text-center py-8">
        <ExclamationTriangleIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
        <p className="text-gray-600 dark:text-gray-400">Unable to load user profile</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="pb-6 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center space-x-4">
          <div className="w-16 h-16 bg-primary-100 dark:bg-primary-900/20 rounded-full flex items-center justify-center">
            <UserCircleIcon className="w-10 h-10 text-primary-600 dark:text-primary-400" />
          </div>
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Profile Information
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Update your account details and email address.
            </p>
          </div>
        </div>
      </div>

      {/* Profile Form */}
      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
          {/* Name Field */}
          <div>
            <label htmlFor="name" className="label">
              Full Name *
            </label>
            <input
              {...register('name')}
              type="text"
              className={`input ${errors.name ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              disabled={isLoading}
            />
            {errors.name && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.name.message}
              </p>
            )}
          </div>

          {/* Email Field */}
          <div>
            <label htmlFor="email" className="label">
              Email Address *
            </label>
            <input
              {...register('email')}
              type="email"
              className={`input ${errors.email ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
              disabled={isLoading}
            />
            {errors.email && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.email.message}
              </p>
            )}
            <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
              Changing your email will require verification.
            </p>
          </div>
        </div>

        {/* Account Information */}
        <div className="bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4">
          <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
            Account Information
          </h4>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 text-sm">
            <div>
              <span className="text-gray-500 dark:text-gray-400">Account ID:</span>
              <span className="ml-2 font-mono text-gray-900 dark:text-white">
                {user.id}
              </span>
            </div>
            <div>
              <span className="text-gray-500 dark:text-gray-400">Member since:</span>
              <span className="ml-2 text-gray-900 dark:text-white">
                {new Date(user.created_at).toLocaleDateString()}
              </span>
            </div>
            <div>
              <span className="text-gray-500 dark:text-gray-400">Account status:</span>
              <span className={`ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                user.is_active 
                  ? 'bg-success-100 text-success-800 dark:bg-success-900/20 dark:text-success-300'
                  : 'bg-error-100 text-error-800 dark:bg-error-900/20 dark:text-error-300'
              }`}>
                <CheckIcon className="w-3 h-3 mr-1" />
                {user.is_active ? 'Active' : 'Inactive'}
              </span>
            </div>
            {user.last_login_at && (
              <div>
                <span className="text-gray-500 dark:text-gray-400">Last login:</span>
                <span className="ml-2 text-gray-900 dark:text-white">
                  {new Date(user.last_login_at).toLocaleString()}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Form Actions */}
        {isDirty && (
          <div className="flex items-center justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700">
            <button
              type="button"
              onClick={handleCancel}
              className="btn btn-secondary"
              disabled={isLoading}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="btn btn-primary"
              disabled={isLoading}
            >
              {isLoading ? (
                <div className="flex items-center">
                  <div className="loading-spinner h-4 w-4 mr-2" />
                  Updating...
                </div>
              ) : (
                'Update Profile'
              )}
            </button>
          </div>
        )}
      </form>
    </div>
  );
}
