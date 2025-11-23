import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as bcrypt from 'bcrypt';

import { User, UserDocument } from '../../modules/users/schemas/user.schema';

@Injectable()
export class AdminSeedService {
  constructor(
    @InjectModel(User.name) private userModel: Model<UserDocument>,
    private configService: ConfigService,
  ) {}

  async seed(): Promise<void> {
    const adminEmail = this.configService.get<string>('ADMIN_EMAIL', 'admin@example.com');
    const adminPassword = this.configService.get<string>('ADMIN_PASSWORD', '123456');

    const existingAdmin = await this.userModel.findOne({ email: adminEmail });

    if (existingAdmin) {
      console.log(`✅ Admin user already exists: ${adminEmail}`);
      return;
    }

    const hashedPassword = await bcrypt.hash(adminPassword, 10);

    const admin = new this.userModel({
      name: 'Administrator',
      email: adminEmail,
      password: hashedPassword,
      role: 'admin',
      isActive: true,
    });

    await admin.save();
    console.log(`🌱 Admin user created: ${adminEmail}`);
  }
}
