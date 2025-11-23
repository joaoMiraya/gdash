import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';

import { WeatherService } from './weather.service';
import { WeatherController } from './weather.controller';
import { ExportService } from './services/export.service';
import { InsightsService } from './services/insights.service';
import { WeatherLog, WeatherLogSchema } from './schemas/weather-log.schema';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: WeatherLog.name, schema: WeatherLogSchema }]),
  ],
  controllers: [WeatherController],
  providers: [WeatherService, ExportService, InsightsService],
  exports: [WeatherService],
})
export class WeatherModule {}