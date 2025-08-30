import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import {
  ShieldCheckIcon,
  EyeIcon,
  EyeSlashIcon,
  KeyIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/outline';
import { useNotifications } from '../../store/app';
import { formatAPIError } from '../../lib/errors';
import { changePasswordSchema, type ChangePasswordFormData } from '../../lib/validation';

export function SecuritySettings() {
  const [isLoading, setIsLoading] = useState(false);
  const [showPasswords, setShowPasswords] = useState({
    current: false,
    new: false,
    confirm: false,
  });
  const { showSuccess, showError } = useNotifications();

  const {
    register,
    handleSubmit,
    formState: { errors },
    reset,
    watch,
  } = useForm<ChangePasswordFormData>({
    resolver: zodResolver(changePasswordSchema),
    defaultValues: {
      currentPassword: '',
      newPassword: '',
      confirmPassword: '',
    },
  });

  const newPassword = watch('newPassword');

  const onSubmit = async () => {
    setIsLoading(true);
    
    try {
      // In a real app: await api.changePassword(data);
      // For now, simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      showSuccess(
        'Password Updated',
        'Your password has been changed successfully.'
      );
      
      // Reset form
      reset();
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
    } finally {
      setIsLoading(false);
    }
  };

  const togglePasswordVisibility = (field: keyof typeof showPasswords) => {
    setShowPasswords(prev => ({
      ...prev,
      [field]: !prev[field],
    }));
  };

  // Password strength requirements
  const requirements = [
    { test: (pwd: string) => pwd.length >= 8, label: 'At least 8 characters' },
    { test: (pwd: string) => /[A-Z]/.test(pwd), label: 'One uppercase letter' },
    { test: (pwd: string) => /[a-z]/.test(pwd), label: 'One lowercase letter' },
    { test: (pwd: string) => /[0-9]/.test(pwd), label: 'One number' },
    { test: (pwd: string) => /[^A-Za-z0-9]/.test(pwd), label: 'One special character' },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="pb-6 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center space-x-4">
          <div className="w-16 h-16 bg-blue-100 dark:bg-blue-900/20 rounded-full flex items-center justify-center">
            <ShieldCheckIcon className="w-10 h-10 text-blue-600 dark:text-blue-400" />
          </div>
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Security Settings
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Manage your account security and change your password.
            </p>
          </div>
        </div>
      </div>

      {/* Security Status */}
      <div className="bg-success-50 dark:bg-success-900/20 border border-success-200 dark:border-success-800 rounded-lg p-4">
        <div className="flex items-start space-x-3">
          <CheckCircleIcon className="w-5 h-5 text-success-600 dark:text-success-400 mt-0.5" />
          <div>
            <h4 className="text-sm font-medium text-success-800 dark:text-success-200">
              Account Security Status
            </h4>
            <p className="mt-1 text-sm text-success-700 dark:text-success-300">
              Your account is secure with strong authentication enabled.
            </p>
            <ul className="mt-2 text-xs text-success-600 dark:text-success-400 space-y-1">
              <li>• Strong password requirements enforced</li>
              <li>• JWT token-based authentication</li>
              <li>• API key access controls active</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Change Password Form */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
        <div className="flex items-center space-x-3 mb-6">
          <KeyIcon className="w-6 h-6 text-gray-600 dark:text-gray-400" />
          <h4 className="text-lg font-medium text-gray-900 dark:text-white">
            Change Password
          </h4>
        </div>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
          {/* Current Password */}
          <div>
            <label htmlFor="currentPassword" className="label">
              Current Password *
            </label>
            <div className="relative">
              <input
                {...register('currentPassword')}
                type={showPasswords.current ? 'text' : 'password'}
                className={`input pr-10 ${errors.currentPassword ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
                disabled={isLoading}
                placeholder="Enter your current password"
              />
              <button
                type="button"
                onClick={() => togglePasswordVisibility('current')}
                className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
              >
                {showPasswords.current ? (
                  <EyeSlashIcon className="w-5 h-5" />
                ) : (
                  <EyeIcon className="w-5 h-5" />
                )}
              </button>
            </div>
            {errors.currentPassword && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.currentPassword.message}
              </p>
            )}
          </div>

          {/* New Password */}
          <div>
            <label htmlFor="newPassword" className="label">
              New Password *
            </label>
            <div className="relative">
              <input
                {...register('newPassword')}
                type={showPasswords.new ? 'text' : 'password'}
                className={`input pr-10 ${errors.newPassword ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
                disabled={isLoading}
                placeholder="Enter your new password"
              />
              <button
                type="button"
                onClick={() => togglePasswordVisibility('new')}
                className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
              >
                {showPasswords.new ? (
                  <EyeSlashIcon className="w-5 h-5" />
                ) : (
                  <EyeIcon className="w-5 h-5" />
                )}
              </button>
            </div>
            {errors.newPassword && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.newPassword.message}
              </p>
            )}
            
            {/* Password Requirements */}
            {newPassword && (
              <div className="mt-3 p-3 bg-gray-50 dark:bg-gray-700/50 rounded-md">
                <h5 className="text-sm font-medium text-gray-900 dark:text-white mb-2">
                  Password Requirements
                </h5>
                <ul className="space-y-1">
                  {requirements.map((req, index) => {
                    const isMet = req.test(newPassword);
                    return (
                      <li key={index} className="flex items-center text-xs">
                        {isMet ? (
                          <CheckCircleIcon className="w-4 h-4 text-success-500 mr-2" />
                        ) : (
                          <ExclamationTriangleIcon className="w-4 h-4 text-warning-500 mr-2" />
                        )}
                        <span className={isMet ? 'text-success-700 dark:text-success-400' : 'text-gray-600 dark:text-gray-400'}>
                          {req.label}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}
          </div>

          {/* Confirm Password */}
          <div>
            <label htmlFor="confirmPassword" className="label">
              Confirm New Password *
            </label>
            <div className="relative">
              <input
                {...register('confirmPassword')}
                type={showPasswords.confirm ? 'text' : 'password'}
                className={`input pr-10 ${errors.confirmPassword ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
                disabled={isLoading}
                placeholder="Confirm your new password"
              />
              <button
                type="button"
                onClick={() => togglePasswordVisibility('confirm')}
                className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
              >
                {showPasswords.confirm ? (
                  <EyeSlashIcon className="w-5 h-5" />
                ) : (
                  <EyeIcon className="w-5 h-5" />
                )}
              </button>
            </div>
            {errors.confirmPassword && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.confirmPassword.message}
              </p>
            )}
          </div>

          {/* Security Notice */}
          <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md p-4">
            <div className="flex items-start space-x-3">
              <ExclamationTriangleIcon className="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5" />
              <div>
                <h5 className="text-sm font-medium text-blue-800 dark:text-blue-200">
                  Security Notice
                </h5>
                <p className="mt-1 text-sm text-blue-700 dark:text-blue-300">
                  Changing your password will sign you out of all other sessions. You'll need to sign in again on other devices.
                </p>
              </div>
            </div>
          </div>

          {/* Form Actions */}
          <div className="flex justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700">
            <button
              type="button"
              onClick={() => reset()}
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
                  Updating Password...
                </div>
              ) : (
                'Change Password'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
