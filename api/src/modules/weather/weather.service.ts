import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';

import { WeatherLog, WeatherLogDocument } from './schemas/weather-log.schema';
import { CreateWeatherLogDto, WeatherQueryDto } from './dto/weather-log.dto';

@Injectable()
export class WeatherService {
  constructor(
    @InjectModel(WeatherLog.name)
    private weatherModel: Model<WeatherLogDocument>,
  ) {}

  async create(dto: CreateWeatherLogDto): Promise<WeatherLogDocument> {
    const weatherLog = new this.weatherModel({
      city: dto.city,
      country: dto.country,
      temperature: dto.temperature,
      feelsLike: dto.feels_like,
      humidity: dto.humidity,
      pressure: dto.pressure,
      windSpeed: dto.wind_speed,
      windDirection: dto.wind_direction,
      clouds: dto.clouds,
      visibility: dto.visibility,
      condition: dto.condition,
      conditionDescription: dto.condition_description,
      icon: dto.icon,
      rain1h: dto.rain_1h,
      snow1h: dto.snow_1h,
      sunrise: new Date(dto.sunrise),
      sunset: new Date(dto.sunset),
      collectedAt: new Date(dto.collected_at),
    });

    return weatherLog.save();
  }

  async findAll(
    query: WeatherQueryDto,
  ): Promise<{ data: WeatherLogDocument[]; total: number }> {
    const filter: any = {};

    if (query.city) {
      filter.city = { $regex: query.city, $options: 'i' };
    }

    if (query.startDate || query.endDate) {
      filter.collectedAt = {};
      if (query.startDate) filter.collectedAt.$gte = new Date(query.startDate);
      if (query.endDate) filter.collectedAt.$lte = new Date(query.endDate);
    }

    const [data, total] = await Promise.all([
      this.weatherModel
        .find(filter)
        .sort({ collectedAt: -1 })
        .skip(query.offset || 0)
        .limit(query.limit || 100)
        .exec(),
      this.weatherModel.countDocuments(filter).exec(),
    ]);

    return { data, total };
  }

  async findOne(id: string): Promise<WeatherLogDocument | null> {
    return this.weatherModel.findById(id).exec();
  }

  async getStats(city?: string) {
    const match: any = {};
    if (city) match.city = city;

    const stats = await this.weatherModel.aggregate([
      { $match: match },
      {
        $group: {
          _id: '$city',
          avgTemperature: { $avg: '$temperature' },
          minTemperature: { $min: '$temperature' },
          maxTemperature: { $max: '$temperature' },
          avgHumidity: { $avg: '$humidity' },
          avgWindSpeed: { $avg: '$windSpeed' },
          totalRecords: { $sum: 1 },
          lastCollected: { $max: '$collectedAt' },
        },
      },
    ]);

    return stats;
  }

  async getAllForExport(query: WeatherQueryDto): Promise<WeatherLogDocument[]> {
    const filter: any = {};

    if (query.city) {
      filter.city = { $regex: query.city, $options: 'i' };
    }

    if (query.startDate || query.endDate) {
      filter.collectedAt = {};
      if (query.startDate) filter.collectedAt.$gte = new Date(query.startDate);
      if (query.endDate) filter.collectedAt.$lte = new Date(query.endDate);
    }

    return this.weatherModel.find(filter).sort({ collectedAt: -1 }).exec();
  }
}
