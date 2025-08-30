import { Fragment } from 'react';
import { Tab } from '@headlessui/react';

interface TabItem {
  id: string;
  label: string;
  content: React.ReactNode;
  badge?: string | number;
  disabled?: boolean;
}

interface TabsProps {
  tabs: TabItem[];
  defaultTab?: string;
  onChange?: (tabId: string) => void;
  orientation?: 'horizontal' | 'vertical';
  className?: string;
}

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(' ');
}

export function Tabs({ 
  tabs, 
  defaultTab, 
  onChange, 
  orientation = 'horizontal',
  className = '' 
}: TabsProps) {
  const defaultIndex = defaultTab ? tabs.findIndex(tab => tab.id === defaultTab) : 0;
  const validDefaultIndex = defaultIndex >= 0 ? defaultIndex : 0;

  const handleTabChange = (index: number) => {
    if (onChange) {
      onChange(tabs[index].id);
    }
  };

  if (orientation === 'vertical') {
    return (
      <Tab.Group defaultIndex={validDefaultIndex} onChange={handleTabChange}>
        <div className={classNames('flex', className)}>
          {/* Vertical Tab List */}
          <Tab.List className="flex flex-col space-y-1 rounded-lg bg-gray-100 dark:bg-gray-800 p-1 min-w-48">
            {tabs.map((tab) => (
              <Tab key={tab.id} disabled={tab.disabled} as={Fragment}>
                {({ selected }) => (
                  <button
                    className={classNames(
                      'w-full rounded-md px-3 py-2 text-sm font-medium leading-5 text-left focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2',
                      selected
                        ? 'bg-white dark:bg-gray-700 text-primary-700 dark:text-primary-300 shadow'
                        : 'text-gray-700 dark:text-gray-300 hover:bg-white/[0.12] hover:text-gray-900 dark:hover:text-white',
                      tab.disabled ? 'opacity-50 cursor-not-allowed' : ''
                    )}
                    disabled={tab.disabled}
                  >
                    <div className="flex items-center justify-between">
                      <span>{tab.label}</span>
                      {tab.badge && (
                        <span className={classNames(
                          'ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
                          selected
                            ? 'bg-primary-100 text-primary-800 dark:bg-primary-800 dark:text-primary-100'
                            : 'bg-gray-200 text-gray-800 dark:bg-gray-600 dark:text-gray-200'
                        )}>
                          {tab.badge}
                        </span>
                      )}
                    </div>
                  </button>
                )}
              </Tab>
            ))}
          </Tab.List>

          {/* Tab Panels */}
          <Tab.Panels className="flex-1 ml-6">
            {tabs.map((tab) => (
              <Tab.Panel
                key={tab.id}
                className="focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 rounded-lg"
              >
                {tab.content}
              </Tab.Panel>
            ))}
          </Tab.Panels>
        </div>
      </Tab.Group>
    );
  }

  // Horizontal tabs (default)
  return (
    <Tab.Group defaultIndex={validDefaultIndex} onChange={handleTabChange}>
      <div className={className}>
        {/* Horizontal Tab List */}
        <Tab.List className="flex space-x-1 rounded-lg bg-gray-100 dark:bg-gray-800 p-1 mb-6">
          {tabs.map((tab) => (
            <Tab key={tab.id} disabled={tab.disabled} as={Fragment}>
              {({ selected }) => (
                <button
                  className={classNames(
                    'w-full rounded-md px-3 py-2 text-sm font-medium leading-5 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2',
                    selected
                      ? 'bg-white dark:bg-gray-700 text-primary-700 dark:text-primary-300 shadow'
                      : 'text-gray-700 dark:text-gray-300 hover:bg-white/[0.12] hover:text-gray-900 dark:hover:text-white',
                    tab.disabled ? 'opacity-50 cursor-not-allowed' : ''
                  )}
                  disabled={tab.disabled}
                >
                  <div className="flex items-center justify-center space-x-2">
                    <span>{tab.label}</span>
                    {tab.badge && (
                      <span className={classNames(
                        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
                        selected
                          ? 'bg-primary-100 text-primary-800 dark:bg-primary-800 dark:text-primary-100'
                          : 'bg-gray-200 text-gray-800 dark:bg-gray-600 dark:text-gray-200'
                      )}>
                        {tab.badge}
                      </span>
                    )}
                  </div>
                </button>
              )}
            </Tab>
          ))}
        </Tab.List>

        {/* Tab Panels */}
        <Tab.Panels>
          {tabs.map((tab) => (
            <Tab.Panel
              key={tab.id}
              className="focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 rounded-lg"
            >
              {tab.content}
            </Tab.Panel>
          ))}
        </Tab.Panels>
      </div>
    </Tab.Group>
  );
}
