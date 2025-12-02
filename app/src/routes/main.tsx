import { createRoot } from 'react-dom/client'
import '../stylesheet/index.css'
import App from '../App.tsx'
import { BrowserRouter, Route, Routes } from 'react-router';
import { ThemeProvider } from '@/context/theme.context.tsx';
import { AuthProvider } from '@/features/auth/hook/auth.context.tsx';
import { Suspense, lazy } from 'react';
import { Loading } from '@/pages/Loading.tsx';
import PrivateRoute from './private.route.tsx';
import { NotFound } from '@/pages/NotFound.tsx';

const Dashboard = lazy(() => import('@/pages/Dashboard.tsx').then(m => ({ default: m.Dashboard })));
const Home = lazy(() => import('@/pages/Home.tsx').then(m => ({ default: m.Home })));
const Signup = lazy(() => import('@/pages/Signup.tsx').then(m => ({ default: m.Signup })));

const root = document.getElementById('root');
createRoot(root!).render(
  <BrowserRouter>
    <AuthProvider>
      <ThemeProvider>
        <Routes>
          <Route path="*" element={<NotFound />} />
          <Route path="/" element={<App />}>
            <Route 
              index 
              element={
                <Suspense fallback={<Loading />}>
                  <Home />
                </Suspense>
              } 
            />
            <Route 
              path="signup" 
              element={
                <Suspense fallback={<Loading />}>
                  <Signup />
                </Suspense>
              } 
            />
            
            <Route
              path="dashboard"
              element={
                <PrivateRoute>
                  <Suspense fallback={<Loading />}>
                    <Dashboard />
                  </Suspense>
                </PrivateRoute>
              }
            />
          </Route>
        </Routes>
      </ThemeProvider>
    </AuthProvider>
  </BrowserRouter>
)