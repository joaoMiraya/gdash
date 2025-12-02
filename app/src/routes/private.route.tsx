import { useAuth } from "@/features/auth/hook/useAuth";
import { Navigate } from "react-router";

const PrivateRoute = ({children}: {children: React.ReactNode}) => {
    const { isAuthenticated, loading } = useAuth();

  if (!isAuthenticated && !loading) {
    return <Navigate to="/" replace />;
  }

  return children;
};

export default PrivateRoute;