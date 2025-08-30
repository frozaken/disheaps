import { useState } from 'react';
import { useForm, Controller } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import {
  SunIcon,
  MoonIcon,
  ComputerDesktopIcon,
  BellIcon,
  GlobeAltIcon,
  ClockIcon,
  CheckIcon,
} from '@heroicons/react/24/outline';
import { Switch } from '@headlessui/react';
import { useAppStore } from '../../store/app';
import { useNotifications } from '../../store/app';
import { formatAPIError } from '../../lib/errors';
import { userPreferencesSchema, type UserPreferencesFormData } from '../../lib/validation';
import type { Theme } from '../../lib/types';

// Mock timezone data - in a real app, you'd get this from a timezone API
const timezones = [
  { value: 'America/New_York', label: 'Eastern Time (ET)' },
  { value: 'America/Chicago', label: 'Central Time (CT)' },
  { value: 'America/Denver', label: 'Mountain Time (MT)' },
  { value: 'America/Los_Angeles', label: 'Pacific Time (PT)' },
  { value: 'Europe/London', label: 'London (GMT)' },
  { value: 'Europe/Paris', label: 'Paris (CET)' },
  { value: 'Europe/Berlin', label: 'Berlin (CET)' },
  { value: 'Asia/Tokyo', label: 'Tokyo (JST)' },
  { value: 'Asia/Shanghai', label: 'Shanghai (CST)' },
  { value: 'Asia/Kolkata', label: 'India (IST)' },
  { value: 'UTC', label: 'UTC' },
];

const refreshIntervals = [
  { value: 5, label: '5 seconds' },
  { value: 10, label: '10 seconds' },
  { value: 30, label: '30 seconds' },
  { value: 60, label: '1 minute' },
  { value: 120, label: '2 minutes' },
  { value: 300, label: '5 minutes' },
];

export function PreferencesSettings() {
  const [isLoading, setIsLoading] = useState(false);
  const { preferences, updatePreferences } = useAppStore();
  const { showSuccess, showError } = useNotifications();

  const {
    register,
    handleSubmit,
    control,
    formState: { errors, isDirty },
    reset,
    watch,
  } = useForm<UserPreferencesFormData>({
    resolver: zodResolver(userPreferencesSchema),
    defaultValues: {
      theme: preferences.theme,
      autoRefreshInterval: preferences.autoRefreshInterval,
      timezone: preferences.timezone,
      notifications: {
        email: preferences.notifications.email,
        browser: preferences.notifications.browser,
      },
    },
  });

  const selectedTheme = watch('theme');

  const onSubmit = async (data: UserPreferencesFormData) => {
    setIsLoading(true);
    
    try {
      // In a real app: await api.updatePreferences(data);
      // For now, simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Update preferences in app store
      updatePreferences({
        theme: data.theme,
        autoRefreshInterval: data.autoRefreshInterval,
        timezone: data.timezone,
        notifications: data.notifications,
      });
      
      showSuccess(
        'Preferences Updated',
        'Your preferences have been saved successfully.'
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
      theme: preferences.theme,
      autoRefreshInterval: preferences.autoRefreshInterval,
      timezone: preferences.timezone,
      notifications: {
        email: preferences.notifications.email,
        browser: preferences.notifications.browser,
      },
    });
  };

  const getThemeIcon = (theme: Theme) => {
    switch (theme) {
      case 'light':
        return <SunIcon className="w-5 h-5" />;
      case 'dark':
        return <MoonIcon className="w-5 h-5" />;
      case 'system':
        return <ComputerDesktopIcon className="w-5 h-5" />;
      default:
        return <ComputerDesktopIcon className="w-5 h-5" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="pb-6 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center space-x-4">
          <div className="w-16 h-16 bg-purple-100 dark:bg-purple-900/20 rounded-full flex items-center justify-center">
            <GlobeAltIcon className="w-10 h-10 text-purple-600 dark:text-purple-400" />
          </div>
          <div>
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Preferences
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Customize your experience and application settings.
            </p>
          </div>
        </div>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-8">
        {/* Theme Settings */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
          <h4 className="text-lg font-medium text-gray-900 dark:text-white mb-4">
            Appearance
          </h4>
          
          <div>
            <label className="label mb-3">
              Theme
            </label>
            <Controller
              name="theme"
              control={control}
              render={({ field }) => (
                <div className="space-y-2">
                  {(['light', 'dark', 'system'] as const).map((theme) => (
                    <label key={theme} className="flex items-center p-3 border border-gray-200 dark:border-gray-700 rounded-lg cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors">
                      <input
                        type="radio"
                        {...field}
                        value={theme}
                        checked={field.value === theme}
                        className="form-radio text-primary-600"
                        disabled={isLoading}
                      />
                      <div className="ml-3 flex items-center space-x-3">
                        <div className={`p-2 rounded-lg ${
                          theme === 'light' ? 'bg-yellow-100 text-yellow-600' :
                          theme === 'dark' ? 'bg-gray-900 text-gray-100' :
                          'bg-blue-100 text-blue-600'
                        }`}>
                          {getThemeIcon(theme)}
                        </div>
                        <div>
                          <div className="font-medium text-gray-900 dark:text-white capitalize">
                            {theme}
                          </div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">
                            {theme === 'light' && 'Light mode'}
                            {theme === 'dark' && 'Dark mode'}
                            {theme === 'system' && 'Follow system preference'}
                          </div>
                        </div>
                        {field.value === theme && (
                          <CheckIcon className="w-5 h-5 text-primary-600 dark:text-primary-400 ml-auto" />
                        )}
                      </div>
                    </label>
                  ))}
                </div>
              )}
            />
            {errors.theme && (
              <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                {errors.theme.message}
              </p>
            )}
          </div>
        </div>

        {/* Auto Refresh Settings */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
          <div className="flex items-center space-x-3 mb-4">
            <ClockIcon className="w-6 h-6 text-gray-600 dark:text-gray-400" />
            <h4 className="text-lg font-medium text-gray-900 dark:text-white">
              Auto Refresh
            </h4>
          </div>
          
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label htmlFor="autoRefreshInterval" className="label">
                Refresh Interval
              </label>
              <select
                {...register('autoRefreshInterval', { valueAsNumber: true })}
                className={`input ${errors.autoRefreshInterval ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
                disabled={isLoading}
              >
                {refreshIntervals.map((interval) => (
                  <option key={interval.value} value={interval.value}>
                    {interval.label}
                  </option>
                ))}
              </select>
              {errors.autoRefreshInterval && (
                <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                  {errors.autoRefreshInterval.message}
                </p>
              )}
              <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                How often dashboards and metrics should automatically refresh.
              </p>
            </div>

            <div>
              <label htmlFor="timezone" className="label">
                Timezone
              </label>
              <select
                {...register('timezone')}
                className={`input ${errors.timezone ? 'border-error-500 focus:border-error-500 focus:ring-error-500' : ''}`}
                disabled={isLoading}
              >
                {timezones.map((tz) => (
                  <option key={tz.value} value={tz.value}>
                    {tz.label}
                  </option>
                ))}
              </select>
              {errors.timezone && (
                <p className="mt-1 text-sm text-error-600 dark:text-error-400">
                  {errors.timezone.message}
                </p>
              )}
              <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                Used for displaying timestamps and scheduling.
              </p>
            </div>
          </div>
        </div>

        {/* Notification Settings */}
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
          <div className="flex items-center space-x-3 mb-6">
            <BellIcon className="w-6 h-6 text-gray-600 dark:text-gray-400" />
            <h4 className="text-lg font-medium text-gray-900 dark:text-white">
              Notifications
            </h4>
          </div>
          
          <div className="space-y-4">
            <Controller
              name="notifications.email"
              control={control}
              render={({ field }) => (
                <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                  <div>
                    <div className="font-medium text-gray-900 dark:text-white">
                      Email Notifications
                    </div>
                    <div className="text-sm text-gray-500 dark:text-gray-400">
                      Receive alerts and updates via email
                    </div>
                  </div>
                  <Switch
                    checked={field.value}
                    onChange={field.onChange}
                    disabled={isLoading}
                    className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                      field.value ? 'bg-primary-600' : 'bg-gray-200 dark:bg-gray-600'
                    } ${isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
                  >
                    <span
                      className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                        field.value ? 'translate-x-6' : 'translate-x-1'
                      }`}
                    />
                  </Switch>
                </div>
              )}
            />

            <Controller
              name="notifications.browser"
              control={control}
              render={({ field }) => (
                <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                  <div>
                    <div className="font-medium text-gray-900 dark:text-white">
                      Browser Notifications
                    </div>
                    <div className="text-sm text-gray-500 dark:text-gray-400">
                      Show desktop notifications for important events
                    </div>
                  </div>
                  <Switch
                    checked={field.value}
                    onChange={field.onChange}
                    disabled={isLoading}
                    className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                      field.value ? 'bg-primary-600' : 'bg-gray-200 dark:bg-gray-600'
                    } ${isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
                  >
                    <span
                      className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                        field.value ? 'translate-x-6' : 'translate-x-1'
                      }`}
                    />
                  </Switch>
                </div>
              )}
            />
          </div>
        </div>

        {/* Current Settings Preview */}
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h5 className="font-medium text-blue-800 dark:text-blue-200 mb-2">
            Current Settings Summary
          </h5>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 text-sm">
            <div className="flex items-center space-x-2">
              {getThemeIcon(selectedTheme)}
              <span className="text-blue-700 dark:text-blue-300">
                Theme: {selectedTheme}
              </span>
            </div>
            <div className="text-blue-700 dark:text-blue-300">
              Refresh: {refreshIntervals.find(i => i.value === watch('autoRefreshInterval'))?.label}
            </div>
            <div className="text-blue-700 dark:text-blue-300">
              Timezone: {timezones.find(tz => tz.value === watch('timezone'))?.label}
            </div>
            <div className="text-blue-700 dark:text-blue-300">
              Notifications: {watch('notifications.email') || watch('notifications.browser') ? 'Enabled' : 'Disabled'}
            </div>
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
                  Saving...
                </div>
              ) : (
                'Save Preferences'
              )}
            </button>
          </div>
        )}
      </form>
    </div>
  );
}
