export interface UserPayload {
  email: string;
  username: string;
  role: string;
}

export interface RequestWithUser extends Request {
  user: UserPayload;
  token: string;
}
