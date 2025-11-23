import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type WeatherLogDocument = WeatherLog & Document;

@Schema({ timestamps: true, collection: 'weather_logs' })
export class WeatherLog {
  @Prop({ required: true })
  city: string;

  @Prop({ required: true })
  country: string;

  @Prop({ required: true })
  temperature: number;

  @Prop({ required: true })
  feelsLike: number;

  @Prop({ required: true })
  humidity: number;

  @Prop({ required: true })
  pressure: number;

  @Prop({ required: true })
  windSpeed: number;

  @Prop({ required: true })
  windDirection: number;

  @Prop({ required: true })
  clouds: number;

  @Prop({ required: true })
  visibility: number;

  @Prop({ required: true })
  condition: string;

  @Prop({ required: true })
  conditionDescription: string;

  @Prop({ required: true })
  icon: string;

  @Prop()
  rain1h?: number;

  @Prop()
  snow1h?: number;

  @Prop({ required: true })
  sunrise: Date;

  @Prop({ required: true })
  sunset: Date;

  @Prop({ required: true, index: true })
  collectedAt: Date;
}

export const WeatherLogSchema = SchemaFactory.createForClass(WeatherLog);

// Índices para consultas comuns
WeatherLogSchema.index({ city: 1, collectedAt: -1 });
