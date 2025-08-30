import { NavLink, useLocation } from 'react-router-dom';
import {
  HomeIcon,
  QueueListIcon,
  KeyIcon,
  ExclamationTriangleIcon,
  CogIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { useSidebar } from '../../store/app';
import type { NavigationItem } from '../../lib/types';

// Navigation configuration
const navigation: NavigationItem[] = [
  {
    name: 'Dashboard',
    href: '/dashboard',
    icon: HomeIcon,
  },
  {
    name: 'Heaps',
    href: '/heaps',
    icon: QueueListIcon,
  },
  {
    name: 'API Keys',
    href: '/keys',
    icon: KeyIcon,
  },
  {
    name: 'Dead Letter Queue',
    href: '/dlq',
    icon: ExclamationTriangleIcon,
  },
  {
    name: 'Settings',
    href: '/settings',
    icon: CogIcon,
  },
];

interface NavItemProps {
  item: NavigationItem;
  isCurrent: boolean;
}

function NavItem({ item, isCurrent }: NavItemProps) {
  const Icon = item.icon!;

  return (
    <NavLink
      to={item.href}
      className={({ isActive }) =>
        `sidebar-nav-item ${
          isActive || isCurrent
            ? 'sidebar-nav-item-active'
            : 'sidebar-nav-item-inactive'
        }`
      }
    >
      <Icon className="mr-3 h-5 w-5 flex-shrink-0" />
      <span className="truncate">{item.name}</span>
    </NavLink>
  );
}

interface SidebarContentProps {
  onClose?: () => void;
}

function SidebarContent({ onClose }: SidebarContentProps) {
  const location = useLocation();

  return (
    <div className="flex flex-col h-full">
      {/* Logo and close button */}
      <div className="flex items-center justify-between h-16 px-4 bg-primary-600 dark:bg-primary-700">
        <div className="flex items-center">
          <div className="flex-shrink-0 flex items-center">
            <div className="h-8 w-8 bg-white rounded-lg flex items-center justify-center">
              <span className="text-primary-600 font-bold text-lg">D</span>
            </div>
            <span className="ml-2 text-white font-semibold text-lg">
              Disheap
            </span>
          </div>
        </div>
        
        {onClose && (
          <button
            type="button"
            onClick={onClose}
            className="lg:hidden rounded-md p-2 text-primary-200 hover:text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-white"
          >
            <XMarkIcon className="h-6 w-6" />
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="mt-5 flex-1 px-3 space-y-1 overflow-y-auto">
        {navigation.map((item) => (
          <NavItem
            key={item.name}
            item={item}
            isCurrent={location.pathname === item.href}
          />
        ))}
      </nav>

      {/* Bottom section */}
      <div className="flex-shrink-0 px-3 py-4 border-t border-gray-200 dark:border-gray-700">
        <div className="text-xs text-gray-500 dark:text-gray-400 text-center">
          <p>© 2024 Disheap</p>
          <p className="mt-1">
            v{import.meta.env.VITE_APP_VERSION || '1.0.0'}
          </p>
        </div>
      </div>
    </div>
  );
}

export function Sidebar() {
  const { sidebarOpen, setSidebarOpen } = useSidebar();

  return (
    <>
      {/* Desktop sidebar */}
      <div className="hidden lg:flex lg:flex-shrink-0">
        <div className="flex flex-col w-64">
          <div className="flex flex-col h-0 flex-1 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700">
            <SidebarContent />
          </div>
        </div>
      </div>

      {/* Mobile sidebar */}
      <div className={`lg:hidden ${sidebarOpen ? 'fixed inset-0 z-40' : 'hidden'}`}>
        {/* Overlay */}
        <div
          className="fixed inset-0 bg-gray-600 bg-opacity-75"
          onClick={() => setSidebarOpen(false)}
        />

        {/* Sidebar */}
        <div className="relative flex-1 flex flex-col max-w-xs w-full bg-white dark:bg-gray-800">
          <SidebarContent onClose={() => setSidebarOpen(false)} />
        </div>
      </div>
    </>
  );
}

// Hook for getting current navigation item
export function useCurrentNavigation(): NavigationItem | undefined {
  const location = useLocation();
  
  return navigation.find(item => {
    if (item.href === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(item.href);
  });
}
