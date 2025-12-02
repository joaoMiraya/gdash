/* eslint-disable @typescript-eslint/no-explicit-any */
import api from '@/services/api.service';
import type { AuthRequestType } from '@/types/responses.type';
import type { SafeUserType, UserType } from '@/types/user.type';
import { createContext, useState, useEffect } from 'react';


interface AuthContextType {
  user: SafeUserType | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<{ success: boolean; error?: string }>;
  register: (email: string, name: string, password: string) => Promise<{ success: boolean; error?: string }>;
  logout: () => void;
  isAuthenticated: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<SafeUserType | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const checkAuth = async () => {
      try {
        const storedUser = localStorage.getItem('user');
        const storedToken = localStorage.getItem('token');
        
        if (storedUser && storedToken) {
          const parsedUser: SafeUserType = JSON.parse(storedUser);
          const parsedToken = JSON.parse(storedToken);
          
          api.defaults.headers.common['Authorization'] = `Bearer ${parsedToken}`;
          setUser(parsedUser);
        }
      } catch (error) {
        console.error('Error checking auth:', error);
        localStorage.removeItem('user');
        localStorage.removeItem('token');
        localStorage.removeItem('refresh_token');
      } finally {
        setLoading(false);
      }
    };

    checkAuth();
  }, []);

  const login = async (email: string, password: string) => {
    try {
      const response: AuthRequestType = await api.post('/auth/login', { email, password });
      const responseData: UserType = response.data.data;
      const { accessToken, user: userData } = responseData;

      const safeUser: SafeUserType = {
        user: userData
      };

      api.defaults.headers.common['Authorization'] = `Bearer ${accessToken}`;
      
      setUser(safeUser);

      localStorage.setItem('user', JSON.stringify(safeUser));
      localStorage.setItem('token', JSON.stringify(accessToken));

      return { success: true };
    } catch (error) {
      console.error('Login error:', error);
      return { success: false, error: 'Invalid credentials' };
    }
  };

  const register = async (email: string, name: string, password: string) => {
    try {
      const response: AuthRequestType = await api.post('/auth/register', { email, password, name });
      const responseData: UserType = response.data.data;
      const { accessToken, user: userData } = responseData;

      const safeUser: SafeUserType = {
        user: userData
      };

      api.defaults.headers.common['Authorization'] = `Bearer ${accessToken}`;
      
      setUser(safeUser);

      localStorage.setItem('user', JSON.stringify(safeUser));
      localStorage.setItem('token', JSON.stringify(accessToken));
 
      return { success: true };
    } catch (error: any) {
      const message = error.response?.data?.errors?.[0]?.message || 'Erro desconhecido';

      console.error('Login error:', error);
      return { success: false, error: message };
    }
  };

  const logout = () => {
    setUser(null);
    delete api.defaults.headers.common['Authorization'];
    localStorage.removeItem('user');
    localStorage.removeItem('token');
    localStorage.removeItem('refresh_token');
  };

  const isAuthenticated = !!user;

  const value: AuthContextType = {
    user,
    loading,
    login,
    register,
    logout,
    isAuthenticated
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}

export { AuthContext };