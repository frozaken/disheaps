import { useDocumentTitle } from '../../components/layout/Breadcrumbs';
import { Tabs } from '../../components/ui/Tabs';
import { ProfileSettings } from '../../components/settings/ProfileSettings';
import { SecuritySettings } from '../../components/settings/SecuritySettings';
import { PreferencesSettings } from '../../components/settings/PreferencesSettings';

export function Settings() {
  useDocumentTitle('Settings');

  const tabs = [
    {
      id: 'profile',
      label: 'Profile',
      content: <ProfileSettings />,
    },
    {
      id: 'security',
      label: 'Security',
      content: <SecuritySettings />,
    },
    {
      id: 'preferences',
      label: 'Preferences',
      content: <PreferencesSettings />,
    },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">Settings</h1>
          <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
            Manage your account, security settings, and application preferences.
          </p>
        </div>
      </div>

      {/* Settings Tabs */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
        <Tabs 
          tabs={tabs} 
          defaultTab="profile" 
          orientation="vertical" 
          className="min-h-[600px]"
        />
      </div>
    </div>
  );
}