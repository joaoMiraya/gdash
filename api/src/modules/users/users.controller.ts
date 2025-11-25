import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  UseGuards,
} from '@nestjs/common';
import { UsersService } from './users.service';
import { CreateUserDto, UpdateUserDto, UserResponseDto } from './dto/user.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { UserDocument } from './schemas/user.schema';

@Controller('users')
@UseGuards(JwtAuthGuard)
export class UsersController {
  constructor(private readonly usersService: UsersService) {}

  @Post()
  async create(@Body() createUserDto: CreateUserDto): Promise<{
    success: boolean;
    data: UserResponseDto;
  }> {
    const user = await this.usersService.create(createUserDto);
    return {
      success: true,
      data: this.sanitizeUser(user),
    };
  }

  @Get()
  async findAll(): Promise<{
    success: boolean;
    data: UserResponseDto[];
  }> {
    const users = await this.usersService.findAll();
    return {
      success: true,
      data: users.map((user) => this.sanitizeUser(user)),
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string): Promise<{
    success: boolean;
    data: UserResponseDto;
  }> {
    const user = await this.usersService.findOne(id);
    return {
      success: true,
      data: this.sanitizeUser(user),
    };
  }

  @Patch(':id')
  async update(
    @Param('id') id: string,
    @Body() updateUserDto: UpdateUserDto,
  ): Promise<{
    success: boolean;
    data: UserResponseDto;
  }> {
    const user = await this.usersService.update(id, updateUserDto);
    return {
      success: true,
      data: this.sanitizeUser(user),
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string): Promise<{
    success: boolean;
    message: string;
  }> {
    await this.usersService.remove(id);
    return {
      success: true,
      message: 'User deleted successfully',
    };
  }

  private sanitizeUser(user: UserDocument): UserResponseDto {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { password, __v, ...result } = user.toObject();
    return { id: result._id.toString(), ...result };
  }
}
