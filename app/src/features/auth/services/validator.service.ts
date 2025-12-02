
export const isValidEmail = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

export const isValidPassword = (password: string, min = 6, max = 32): boolean => {
    return password.length >= min && password.length <= max;
}

export const doPasswordsMatch = (password: string, confirmPassword: string): boolean => {
    return password === confirmPassword;
}

export const isValidName = (name: string): boolean => {  
    return name.trim().length > 2;
}