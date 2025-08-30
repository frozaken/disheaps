# Disheap Frontend

A modern React dashboard for managing Disheap priority messaging queues. This application provides a web interface for managing heaps, API keys, monitoring system health, and browsing dead letter queues.

## 🎯 Overview

The Disheap Frontend is a React-based web application that serves as the management interface for the Disheap priority messaging system. It provides administrators and developers with an intuitive dashboard to:

- **Manage Priority Heaps**: Create, configure, and monitor message queues
- **API Key Management**: Generate and manage API keys for SDK access
- **Real-time Monitoring**: View system metrics and health status
- **Dead Letter Queue**: Browse and manage failed messages
- **User Authentication**: Secure access with JWT-based sessions

## ✨ Features

### 🔐 Authentication System
- **JWT-based authentication** with automatic token refresh
- **Secure login/logout** with session persistence
- **Protected routes** with automatic redirects
- **Error handling** for authentication failures

### 🎨 Modern UI/UX
- **Responsive design** that works on desktop, tablet, and mobile
- **Dark/Light theme** with system preference detection
- **Intuitive navigation** with breadcrumbs and sidebar
- **Accessibility support** (WCAG 2.1 compliant)
- **Loading states** and error boundaries

### 📊 Dashboard Features
- **System overview** with key metrics
- **Quick actions** for common tasks  
- **Health monitoring** with status indicators
- **Real-time notifications** system

### 🔧 Developer Experience
- **TypeScript** for type safety
- **Hot reload** development server
- **Code splitting** for optimal performance
- **Comprehensive error handling**
- **State management** with Zustand

## 🚀 Quick Start

### Prerequisites

- **Node.js** (v18 or higher)
- **npm** or **yarn**

### Installation

```bash
# Clone the repository (if not already done)
git clone <repository-url>
cd disheap-frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

The application will be available at `http://localhost:5173`

### Building for Production

```bash
# Build for production
npm run build

# Preview production build
npm run preview
```

## 📁 Project Structure

```
disheap-frontend/
├── src/
│   ├── auth/                 # Authentication components
│   │   ├── LoginForm.tsx     # Login form component
│   │   └── PrivateRoute.tsx  # Route protection
│   │
│   ├── components/           # Reusable UI components
│   │   ├── layout/           # Layout components
│   │   │   ├── AppLayout.tsx # Main app layout
│   │   │   ├── Header.tsx    # Top navigation
│   │   │   ├── Sidebar.tsx   # Side navigation
│   │   │   └── Breadcrumbs.tsx
│   │   └── ui/               # Common UI components
│   │       └── NotificationContainer.tsx
│   │
│   ├── lib/                  # Core utilities
│   │   ├── api.ts           # HTTP API client
│   │   ├── types.ts         # TypeScript definitions
│   │   └── errors.ts        # Error handling utilities
│   │
│   ├── pages/               # Page components
│   │   ├── dashboard/       # Dashboard page
│   │   ├── heaps/          # Heap management pages
│   │   ├── keys/           # API key management
│   │   ├── dlq/            # Dead letter queue
│   │   └── settings/       # User settings
│   │
│   ├── store/              # State management
│   │   ├── auth.ts         # Authentication state
│   │   └── app.ts          # Application state
│   │
│   ├── App.tsx             # Main app component
│   ├── main.tsx            # Entry point
│   └── index.css           # Global styles
│
├── public/                 # Static assets
├── package.json           # Dependencies and scripts
├── vite.config.ts        # Vite configuration
├── tailwind.config.js    # Tailwind CSS config
└── tsconfig.json         # TypeScript config
```

## 🛠 Available Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start development server with hot reload |
| `npm run build` | Build for production |
| `npm run preview` | Preview production build locally |
| `npm run lint` | Run ESLint for code quality |
| `npm run type-check` | Run TypeScript type checking |

## 🔌 API Integration

The frontend communicates with the Disheap API service through HTTP/JSON endpoints:

### Authentication Endpoints
- `POST /v1/auth/login` - User login
- `POST /v1/auth/logout` - User logout  
- `POST /v1/auth/refresh` - Token refresh
- `GET /v1/auth/me` - Get current user

### Management Endpoints
- `POST /v1/heaps` - Create heap
- `GET /v1/heaps` - List heaps
- `GET /v1/heaps/{topic}` - Get heap details
- `PATCH /v1/heaps/{topic}` - Update heap
- `DELETE /v1/heaps/{topic}` - Delete heap

### API Key Endpoints
- `POST /v1/keys` - Create API key
- `GET /v1/keys` - List API keys
- `DELETE /v1/keys/{keyId}` - Revoke API key

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the root directory:

```bash
# API Configuration
VITE_API_URL=http://localhost:8080

# Application Info
VITE_APP_NAME=Disheap
VITE_APP_VERSION=1.0.0

# Feature Flags
VITE_ENABLE_DARK_MODE=true
VITE_ENABLE_METRICS_DASHBOARD=true
```

### API Client Configuration

The API client automatically handles:
- **Base URL** from environment variables
- **Request/response interceptors**
- **Authentication headers** (JWT tokens)
- **Error handling** and retries
- **Rate limiting** headers

## 🎨 Theming

The application supports three theme modes:

- **Light Mode**: Clean, bright interface
- **Dark Mode**: Easy on the eyes for extended use  
- **System**: Automatically matches OS preference

Themes can be switched using the theme toggle in the header.

## 🧭 Navigation

### Main Navigation
- **Dashboard** (`/`) - System overview and quick actions
- **Heaps** (`/heaps`) - Manage priority message queues
- **API Keys** (`/keys`) - Generate and manage API keys
- **DLQ** (`/dlq`) - Browse dead letter queue messages
- **Settings** (`/settings`) - User preferences and account

### Route Protection
- **Public routes**: Login page only
- **Protected routes**: All other pages require authentication
- **Auto-redirect**: Unauthenticated users redirect to login

## 🔧 Development

### Adding New Pages

1. Create page component in `src/pages/`
2. Add route to `src/App.tsx`
3. Update navigation in `src/components/layout/Sidebar.tsx`
4. Add breadcrumb logic in `src/components/layout/Breadcrumbs.tsx`

### State Management

The application uses **Zustand** for state management:

- `useAuth()` - Authentication state and actions
- `useAppStore()` - Application state (theme, notifications, etc.)
- `useNotifications()` - Toast notifications
- `useTheme()` - Theme management

### Error Handling

Comprehensive error handling includes:
- **API error mapping** (gRPC codes → HTTP status)
- **User-friendly messages** for all error types
- **Retry logic** for transient failures
- **Global error boundaries** for unhandled errors

## 📱 Browser Support

- **Modern browsers** (Chrome, Firefox, Safari, Edge)
- **Mobile responsive** design
- **Progressive Web App** capabilities (planned)

## 🔒 Security

- **JWT token management** with secure storage
- **CSRF protection** via SameSite cookies
- **XSS prevention** through React's built-in protections
- **Content Security Policy** headers (recommended)

## 🧪 Testing

```bash
# Run unit tests
npm run test

# Run tests with coverage
npm run test:coverage

# Run E2E tests
npm run test:e2e
```

## 📈 Performance

The application is optimized for performance:

- **Code splitting** by route
- **Lazy loading** of non-critical components
- **Image optimization** and compression
- **Bundle analysis** available via `npm run analyze`

## 🚀 Deployment

### Docker Deployment

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build
EXPOSE 5173
CMD ["npm", "run", "preview"]
```

### Static Hosting

The build output (`dist/` folder) can be deployed to:
- **Netlify**
- **Vercel** 
- **AWS S3 + CloudFront**
- **GitHub Pages**
- Any static file server

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

- **Documentation**: [Full API Documentation](../docs/)
- **Issues**: [GitHub Issues](../../issues)
- **Discussions**: [GitHub Discussions](../../discussions)

---

**Built with ❤️ using React, TypeScript, and Tailwind CSS**