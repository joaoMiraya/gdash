export interface UserType {
    user: {
        name: string;
        email: string;
        role: string;
    }
    accessToken: string;
}

export type SafeUserType = Omit<UserType, "accessToken">;