import {
  IsString,
  IsNumber,
  IsDateString,
  IsOptional,
  Min,
  Max,
} from 'class-validator';
import { Transform } from 'class-transformer';

export class CreateWeatherLogDto {
  @IsString()
  city: string;

  @IsString()
  country: string;

  @IsNumber()
  temperature: number;

  @IsNumber()
  feels_like: number;

  @IsNumber()
  @Min(0)
  @Max(100)
  humidity: number;

  @IsNumber()
  pressure: number;

  @IsNumber()
  wind_speed: number;

  @IsNumber()
  wind_direction: number;

  @IsNumber()
  @Min(0)
  @Max(100)
  clouds: number;

  @IsNumber()
  visibility: number;

  @IsString()
  condition: string;

  @IsString()
  condition_description: string;

  @IsString()
  icon: string;

  @IsOptional()
  @IsNumber()
  rain_1h?: number;

  @IsOptional()
  @IsNumber()
  snow_1h?: number;

  @IsDateString()
  sunrise: string;

  @IsDateString()
  sunset: string;

  @IsDateString()
  collected_at: string;
}

export class WeatherQueryDto {
  @IsOptional()
  @IsString()
  city?: string;

  @IsOptional()
  @IsDateString()
  startDate?: string;

  @IsOptional()
  @IsDateString()
  endDate?: string;

  @IsOptional()
  @Transform(({ value }) => parseInt(value, 10))
  @IsNumber()
  @Min(1)
  limit?: number = 100;

  @IsOptional()
  @Transform(({ value }) => parseInt(value, 10))
  @IsNumber()
  @Min(0)
  offset?: number = 0;
}
