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
import { useAuth } from "../hook/useAuth"
import { useEffect, useState, type FormEvent } from "react"
import { isValidEmail, isValidName, isValidPassword } from "../services/validator.service"

export function SignupForm({ ...props }: React.ComponentProps<typeof Card>) {
    const [formData, setFormData] = useState({
        name: "",
        email: "",
        password: "", 
        confirmPassword: "" 
    });
    
    const [errors, setErrors] = useState({
      name: '',
      email: '',
      password: '',
      confirmPassword: '',
      system: ''
    });
    
    const { register, isAuthenticated } = useAuth();
    const navigate = useNavigate();

    useEffect(() => {
      if (isAuthenticated) {
        navigate('/dashboard');
      }
    }, [isAuthenticated, navigate]);

    const validateName = () => {
      const validationResult = isValidName(formData.name);
      setErrors(prev => ({
        ...prev,
        name: validationResult ? '' : 'Nome inválido. Insira seu nome completo.'
      }));
      return validationResult;
    };

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
        password: validationResult ? '' : 'Senha deve ter 6-32 caracteres.'
      }));
      return validationResult;
    };

    const validateConfirmPassword = () => {
      const isValid = formData.password === formData.confirmPassword && formData.confirmPassword !== '';
      setErrors(prev => ({
        ...prev,
        confirmPassword: isValid ? '' : 'As senhas não coincidem.'
      }));
      return isValid;
    };

    const handleSubmit = async (e: FormEvent) => {
      e.preventDefault();
      
      const isNameValid = validateName();
      const isEmailValid = validateEmail();
      const isPasswordValid = validatePassword();
      const isConfirmPasswordValid = validateConfirmPassword();

      if (isNameValid && isEmailValid && isPasswordValid && isConfirmPasswordValid) {
        try {
          const response = await register(formData.email, formData.name, formData.password);
          if (response.success) {
            navigate('/dashboard');
          } else {
            console.error("Registro falhou:", response.error);
            setErrors(prev => ({ ...prev, system: response.error || 'Erro ao registrar' }));
          }
        } catch (error) {
          console.error("Erro ao registrar:", error);
        }
      }
    };

  return (
    <Card {...props}>
      <CardHeader>
        <CardTitle>Crie uma conta</CardTitle>
        <CardDescription>
          Insira suas informações abaixo para criar sua conta
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit}>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="name">Nome Completo</FieldLabel>
              <Input
                onBlur={validateName}
                onChange={(e: FormEvent) => {
                  setFormData({ ...formData, name: (e.target as HTMLInputElement).value });
                  if (errors.name) validateName();
                }}
                id="name"
                type="text"
                placeholder="John Doe"
                required 
                className={errors.name ? "border-red-500" : ""}
              />
              {errors.name && (
                <span className="text-sm text-red-500 mt-1">{errors.name}</span>
              )}
            </Field>

            <Field>
              <FieldLabel htmlFor="email">Email</FieldLabel>
              <Input
                onBlur={validateEmail}
                onChange={(e: FormEvent) => {
                  setFormData({ ...formData, email: (e.target as HTMLInputElement).value });
                  if (errors.email) validateEmail();
                }}
                id="email"
                type="email"
                placeholder="m@example.com"
                required
                className={errors.email ? "border-red-500" : ""}
              />
              {errors.email && (
                <span className="text-sm text-red-500 mt-1">{errors.email}</span>
              )}
            </Field>

            <Field>
              <FieldLabel htmlFor="password">Senha</FieldLabel>
              <Input
                onBlur={validatePassword}
                onChange={(e: FormEvent) => {
                  setFormData({ ...formData, password: (e.target as HTMLInputElement).value });
                  if (errors.password) validatePassword();
                }}
                id="password"
                type="password"
                required
                className={errors.password ? "border-red-500" : ""}
              />
              <FieldDescription>
                Deve ter pelo menos 8 caracteres.
              </FieldDescription>
              {errors.password && (
                <span className="text-sm text-red-500 mt-1">{errors.password}</span>
              )}
            </Field>

            <Field>
              <FieldLabel htmlFor="confirm-password">
                Confirme a Senha
              </FieldLabel>
              <Input
                onBlur={validateConfirmPassword}
                onChange={(e: FormEvent) => {
                  setFormData({ ...formData, confirmPassword: (e.target as HTMLInputElement).value });
                  if (errors.confirmPassword) validateConfirmPassword();
                }}
                id="confirm-password"
                type="password"
                required
                className={errors.confirmPassword ? "border-red-500" : ""}
              />
              <FieldDescription>Por favor, confirme sua senha.</FieldDescription>
              {errors.confirmPassword && (
                <span className="text-sm text-red-500 mt-1">{errors.confirmPassword}</span>
              )}
            </Field>

            <FieldGroup>
              <Field>
                <Button type="submit" className="cursor-pointer">Criar Conta</Button>
                <FieldDescription className="px-6 text-center">
                  Já tem uma conta? <Link to="/">Entrar</Link>
                </FieldDescription>
              </Field>
            </FieldGroup>
              {errors.system && (
                <span className="text-sm text-red-500 mt-1">{errors.system}</span>
              )}
          </FieldGroup>
        </form>
      </CardContent>
    </Card>
  )
}