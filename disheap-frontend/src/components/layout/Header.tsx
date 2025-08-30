import React, { Fragment } from 'react';
import { Menu, Transition } from '@headlessui/react';
import {
  Bars3Icon,
  BellIcon,
  MoonIcon,
  SunIcon,
  ComputerDesktopIcon,
  UserCircleIcon,
  CogIcon,
  ArrowRightOnRectangleIcon,
} from '@heroicons/react/24/outline';
import { useAuth } from '../../store/auth';
import { useTheme, useSidebar, useNotifications } from '../../store/app';
import type { Theme } from '../../lib/types';

function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  const themeOptions: Array<{ value: Theme; icon: React.ComponentType<any>; label: string }> = [
    { value: 'light', icon: SunIcon, label: 'Light' },
    { value: 'dark', icon: MoonIcon, label: 'Dark' },
    { value: 'system', icon: ComputerDesktopIcon, label: 'System' },
  ];

  return (
    <Menu as="div" className="relative inline-block text-left">
      <Menu.Button className="inline-flex items-center justify-center rounded-md p-2 text-gray-400 hover:text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-700 dark:hover:text-gray-300 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-primary-500">
        <span className="sr-only">Open theme menu</span>
        {theme === 'light' && <SunIcon className="h-5 w-5" />}
        {theme === 'dark' && <MoonIcon className="h-5 w-5" />}
        {theme === 'system' && <ComputerDesktopIcon className="h-5 w-5" />}
      </Menu.Button>

      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <Menu.Items className="absolute right-0 z-10 mt-2 w-36 origin-top-right rounded-md bg-white dark:bg-gray-800 shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
          <div className="py-1">
            {themeOptions.map((option) => (
              <Menu.Item key={option.value}>
                {({ active }) => (
                  <button
                    onClick={() => setTheme(option.value)}
                    className={`${
                      active ? 'bg-gray-100 dark:bg-gray-700' : ''
                    } ${
                      theme === option.value ? 'text-primary-600 dark:text-primary-400' : 'text-gray-700 dark:text-gray-300'
                    } group flex w-full items-center px-4 py-2 text-sm`}
                  >
                    <option.icon className="mr-3 h-4 w-4" />
                    {option.label}
                  </button>
                )}
              </Menu.Item>
            ))}
          </div>
        </Menu.Items>
      </Transition>
    </Menu>
  );
}

function NotificationButton() {
  const { notifications } = useNotifications();
  const unreadCount = notifications.length;

  return (
    <button
      type="button"
      className="relative inline-flex items-center justify-center rounded-md p-2 text-gray-400 hover:text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-700 dark:hover:text-gray-300 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-primary-500"
    >
      <span className="sr-only">View notifications</span>
      <BellIcon className="h-5 w-5" />
      {unreadCount > 0 && (
        <span className="absolute -top-1 -right-1 h-4 w-4 bg-error-500 text-white text-xs rounded-full flex items-center justify-center">
          {unreadCount > 9 ? '9+' : unreadCount}
        </span>
      )}
    </button>
  );
}

function UserMenu() {
  const { user, logout } = useAuth();

  if (!user) return null;

  const handleLogout = async () => {
    try {
      await logout();
    } catch (error) {
      // Error is handled by the store
    }
  };

  return (
    <Menu as="div" className="relative inline-block text-left">
      <Menu.Button className="flex items-center text-sm rounded-full focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
        <span className="sr-only">Open user menu</span>
        <div className="h-8 w-8 rounded-full bg-gray-300 dark:bg-gray-600 flex items-center justify-center">
          <UserCircleIcon className="h-6 w-6 text-gray-600 dark:text-gray-400" />
        </div>
      </Menu.Button>

      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <Menu.Items className="absolute right-0 z-10 mt-2 w-48 origin-top-right rounded-md bg-white dark:bg-gray-800 shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
          <div className="py-1">
            {/* User info */}
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
              <p className="text-sm font-medium text-gray-900 dark:text-white">
                {user.name}
              </p>
              <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                {user.email}
              </p>
            </div>

            {/* Menu items */}
            <Menu.Item>
              {({ active }) => (
                <a
                  href="/settings"
                  className={`${
                    active ? 'bg-gray-100 dark:bg-gray-700' : ''
                  } group flex items-center px-4 py-2 text-sm text-gray-700 dark:text-gray-300`}
                >
                  <CogIcon className="mr-3 h-4 w-4" />
                  Settings
                </a>
              )}
            </Menu.Item>

            <Menu.Item>
              {({ active }) => (
                <button
                  onClick={handleLogout}
                  className={`${
                    active ? 'bg-gray-100 dark:bg-gray-700' : ''
                  } group flex w-full items-center px-4 py-2 text-sm text-gray-700 dark:text-gray-300`}
                >
                  <ArrowRightOnRectangleIcon className="mr-3 h-4 w-4" />
                  Sign out
                </button>
              )}
            </Menu.Item>
          </div>
        </Menu.Items>
      </Transition>
    </Menu>
  );
}

export function Header() {
  const { toggleSidebar } = useSidebar();

  return (
    <div className="relative z-10 flex-shrink-0 flex h-16 bg-white dark:bg-gray-800 shadow">
      {/* Mobile menu button */}
      <button
        type="button"
        className="px-4 border-r border-gray-200 dark:border-gray-700 text-gray-500 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-primary-500 lg:hidden"
        onClick={toggleSidebar}
      >
        <span className="sr-only">Open sidebar</span>
        <Bars3Icon className="h-6 w-6" />
      </button>

      {/* Header content */}
      <div className="flex-1 px-4 flex justify-between items-center">
        {/* Left side - could add search or breadcrumbs here */}
        <div className="flex-1">
          {/* Empty for now, breadcrumbs are shown below */}
        </div>

        {/* Right side - actions and user menu */}
        <div className="ml-4 flex items-center md:ml-6 space-x-2">
          {/* Theme toggle */}
          <ThemeToggle />

          {/* Notifications */}
          <NotificationButton />

          {/* User menu */}
          <UserMenu />
        </div>
      </div>
    </div>
  );
}
