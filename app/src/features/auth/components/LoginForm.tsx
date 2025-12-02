import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Link, useNavigate } from "react-router"
import { useEffect, useState, type FormEvent } from "react"
import { isValidEmail, isValidPassword } from "../services/validator.service"
import { useAuth } from "../hook/useAuth"

export function LoginForm({
  className,
  ...props
}: React.ComponentProps<"div">) {

  const [formData, setFormData] = useState({
    email: "",
    password: "", 
  });

  const [errors, setErrors] = useState({
    email: '',
    password: '',
    system: ''
  });

  const { login, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  
  useEffect(() => {
    if (isAuthenticated) {
      navigate('/dashboard');
    }
  }, [isAuthenticated, navigate]);

  const validateEmail = () => {
    const validationResult = isValidEmail(formData.email);
    setErrors(prev => ({
      ...prev,
      email: validationResult ? '' : 'Email inválido. Use um formato válido (ex: usuario@email.com).'
    }));
    return validationResult;
  };

  const validatePassword = () => {
    const validationResult = isValidPassword(formData.password);
    setErrors(prev => ({
      ...prev,
      password: validationResult ? '' : `Senha deve ter 6-32 caracteres.`
    }));
    return validationResult;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    
    const isEmailValid = validateEmail();
    const isPasswordValid = validatePassword();

    if (isEmailValid && isPasswordValid) {
      try {
        const result = await login(formData.email, formData.password);

        if (result.success) {
          navigate('/dashboard');
        } else {
          console.error("Login failed:", result.error);
          setErrors(prev => ({ ...prev, system: result.error || 'Email ou senha inválidos' }));
        }
      } catch (error) {
        console.error("Erro ao registrar:", error);
      }
    }
  };

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <Card>
        <CardHeader>
          <CardTitle>Entre na sua conta</CardTitle>
          <CardDescription>
            Insira seu email abaixo para entrar na sua conta
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={(e: FormEvent) => handleSubmit(e)}>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="email">Email</FieldLabel>
                <Input
                  id="email"
                  type="email"
                  placeholder="m@example.com"
                  required
                  onBlur={validateEmail}
                  value={formData.email}
                  onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
                />
                {errors.email && <p className="text-red-500 text-sm">{errors.email}</p>}
              </Field>
              <Field>
                <div className="flex items-center">
                  <FieldLabel htmlFor="password">Senha</FieldLabel>
                </div>
                <Input
                  id="password"
                  type="password"
                  required
                  onBlur={validatePassword}
                  value={formData.password}
                  onChange={(e) => setFormData(prev => ({ ...prev, password: e.target.value }))}
                />
                {errors.password && <p className="text-red-500 text-sm">{errors.password}</p>}
              </Field>
              <Field>
                <Button type="submit">Entrar</Button>
                <FieldDescription className="text-center">
                  Não tem uma conta? <Link to="/signup">Cadastre-se</Link>
                </FieldDescription>
              </Field>
              {errors.system && (
                <span className="text-sm text-red-500 mt-1">{errors.system}</span>
              )}
            </FieldGroup>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
