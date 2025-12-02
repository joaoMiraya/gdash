import type { UserType } from "./user.type";

export interface AuthRequestType {
  data: {
    data: UserType
  };
  success: boolean;
}