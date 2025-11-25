import { Module, OnModuleInit } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';

import { AuthModule } from './modules/auth/auth.module';
import { UsersModule } from './modules/users/users.module';
import { WeatherModule } from './modules/weather/weather.module';
import { AdminSeedService } from './database/seeds/admin-seed.service';
import { User, UserSchema } from './modules/users/schemas/user.schema';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),

    MongooseModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        uri: configService.get<string>('MONGODB_URI'),
      }),
      inject: [ConfigService],
    }),

    AuthModule,
    UsersModule,
    WeatherModule,

    // Schema para o seed
    MongooseModule.forFeature([{ name: User.name, schema: UserSchema }]),
  ],
  providers: [AdminSeedService],
})
export class AppModule implements OnModuleInit {
  constructor(private readonly adminSeedService: AdminSeedService) {}

  async onModuleInit() {
    await this.adminSeedService.seed();
  }
}
