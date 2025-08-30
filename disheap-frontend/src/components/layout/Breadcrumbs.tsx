import React from 'react';
import { Link, useLocation, useParams } from 'react-router-dom';
import { ChevronRightIcon, HomeIcon } from '@heroicons/react/24/solid';

interface BreadcrumbItem {
  name: string;
  href?: string;
  current: boolean;
}

// Helper function to generate breadcrumbs based on current path
function generateBreadcrumbs(pathname: string, params: Record<string, string | undefined>): BreadcrumbItem[] {
  const segments = pathname.split('/').filter(Boolean);
  const breadcrumbs: BreadcrumbItem[] = [];

  // Always start with Dashboard (except if we're on dashboard)
  if (pathname !== '/dashboard') {
    breadcrumbs.push({
      name: 'Dashboard',
      href: '/dashboard',
      current: false,
    });
  }

  // Generate breadcrumbs based on route segments
  let currentPath = '';
  segments.forEach((segment, index) => {
    currentPath += `/${segment}`;
    const isLast = index === segments.length - 1;

    let name = segment;
    let href: string | undefined = currentPath;

    // Customize names and hrefs for specific routes
    switch (segment) {
      case 'dashboard':
        name = 'Dashboard';
        break;
      case 'heaps':
        name = 'Heaps';
        break;
      case 'keys':
        name = 'API Keys';
        break;
      case 'dlq':
        name = 'Dead Letter Queue';
        break;
      case 'settings':
        name = 'Settings';
        break;
      default:
        // Handle dynamic segments (like topic names)
        if (params.topic && segment === params.topic) {
          name = `Topic: ${segment}`;
        } else if (params.keyId && segment === params.keyId) {
          name = `Key: ${segment}`;
        } else if (params.section && segment === params.section) {
          name = segment.charAt(0).toUpperCase() + segment.slice(1);
        } else {
          // Capitalize first letter for generic segments
          name = segment.charAt(0).toUpperCase() + segment.slice(1);
        }
    }

    // Don't make the last item a link
    if (isLast) {
      href = undefined;
    }

    breadcrumbs.push({
      name,
      href,
      current: isLast,
    });
  });

  return breadcrumbs;
}

export function Breadcrumbs() {
  const location = useLocation();
  const params = useParams();
  
  const breadcrumbs = generateBreadcrumbs(location.pathname, params);

  // Don't show breadcrumbs if we're on the dashboard
  if (location.pathname === '/dashboard') {
    return null;
  }

  return (
    <nav className="flex" aria-label="Breadcrumb">
      <ol className="flex items-center space-x-4">
        {/* Home icon for first item if not dashboard */}
        <li>
          <div>
            <Link
              to="/dashboard"
              className="text-gray-400 hover:text-gray-500 dark:text-gray-500 dark:hover:text-gray-400"
            >
              <HomeIcon className="h-5 w-5 flex-shrink-0" />
              <span className="sr-only">Dashboard</span>
            </Link>
          </div>
        </li>

        {breadcrumbs.map((item) => (
          <li key={item.name}>
            <div className="flex items-center">
              <ChevronRightIcon className="h-5 w-5 flex-shrink-0 text-gray-300 dark:text-gray-600" />
              <div className="ml-4">
                {item.href ? (
                  <Link
                    to={item.href}
                    className="text-sm font-medium text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
                    aria-current={item.current ? 'page' : undefined}
                  >
                    {item.name}
                  </Link>
                ) : (
                  <span
                    className="text-sm font-medium text-gray-900 dark:text-gray-100"
                    aria-current="page"
                  >
                    {item.name}
                  </span>
                )}
              </div>
            </div>
          </li>
        ))}
      </ol>
    </nav>
  );
}

// Custom hook to get current page title from breadcrumbs
export function usePageTitle(): string {
  const location = useLocation();
  const params = useParams();
  
  const breadcrumbs = generateBreadcrumbs(location.pathname, params);
  const currentItem = breadcrumbs.find(item => item.current);
  
  return currentItem?.name || 'Disheap';
}

// Hook to set document title
export function useDocumentTitle(title?: string) {
  const defaultTitle = usePageTitle();
  const finalTitle = title || defaultTitle;
  
  React.useEffect(() => {
    const appName = import.meta.env.VITE_APP_NAME || 'Disheap';
    document.title = finalTitle === appName ? appName : `${finalTitle} - ${appName}`;
    
    return () => {
      document.title = appName;
    };
  }, [finalTitle]);
}
