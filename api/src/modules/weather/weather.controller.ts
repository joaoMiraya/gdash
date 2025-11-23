import { Controller, Get, Post, Body, Param, Query, Res, UseGuards } from '@nestjs/common';
import { Response } from 'express';

import { WeatherService } from './weather.service';
import { ExportService } from './services/export.service';
import { InsightsService } from './services/insights.service';

import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { CreateWeatherLogDto, WeatherQueryDto } from './dto/weather-log.dto';

@Controller('weather')
export class WeatherController {
  constructor(
    private readonly weatherService: WeatherService,
    private readonly exportService: ExportService,
    private readonly insightsService: InsightsService,
  ) {}

  // Endpoint para o Worker Go (sem autenticação)
  @Post('logs')
  async create(@Body() createDto: CreateWeatherLogDto) {
    const log = await this.weatherService.create(createDto);
    return {
      success: true,
      id: log._id,
      message: 'Weather log created successfully',
    };
  }

  @Get('logs')
  @UseGuards(JwtAuthGuard)
  async findAll(@Query() query: WeatherQueryDto) {
    const { data, total } = await this.weatherService.findAll(query);
    return {
      success: true,
      data,
      meta: {
        total,
        limit: query.limit,
        offset: query.offset,
      },
    };
  }

  @Get('logs/:id')
  @UseGuards(JwtAuthGuard)
  async findOne(@Param('id') id: string) {
    const log = await this.weatherService.findOne(id);
    return {
      success: true,
      data: log,
    };
  }

  @Get('stats')
  @UseGuards(JwtAuthGuard)
  async getStats(@Query('city') city?: string) {
    const stats = await this.weatherService.getStats(city);
    return {
      success: true,
      data: stats,
    };
  }

  @Get('insights')
  @UseGuards(JwtAuthGuard)
  async getInsights(@Query('city') city?: string) {
    const insights = await this.insightsService.generateInsights(city);
    return {
      success: true,
      data: insights,
    };
  }

  @Get('export.csv')
  @UseGuards(JwtAuthGuard)
  async exportCSV(@Query() query: WeatherQueryDto, @Res() res: Response) {
    const data = await this.weatherService.getAllForExport(query);
    const csv = this.exportService.generateCSV(data);

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=weather_logs.csv');
    res.send(csv);
  }

  @Get('export.xlsx')
  @UseGuards(JwtAuthGuard)
  async exportXLSX(@Query() query: WeatherQueryDto, @Res() res: Response) {
    const data = await this.weatherService.getAllForExport(query);
    const buffer = await this.exportService.generateXLSX(data);

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=weather_logs.xlsx');
    res.send(buffer);
  }
}
