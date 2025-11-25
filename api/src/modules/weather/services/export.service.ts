import { Injectable } from '@nestjs/common';
import * as ExcelJS from 'exceljs';
import { WeatherLogDocument } from '../schemas/weather-log.schema';

@Injectable()
export class ExportService {
  generateCSV(data: WeatherLogDocument[]): string {
    const headers = [
      'ID',
      'City',
      'Country',
      'Temperature',
      'Feels Like',
      'Humidity',
      'Pressure',
      'Wind Speed',
      'Wind Direction',
      'Clouds',
      'Visibility',
      'Condition',
      'Description',
      'Rain 1h',
      'Snow 1h',
      'Collected At',
    ];

    const rows = data.map((log) => [
      log._id.toString(),
      log.city,
      log.country,
      log.temperature,
      log.feelsLike,
      log.humidity,
      log.pressure,
      log.windSpeed,
      log.windDirection,
      log.clouds,
      log.visibility,
      log.condition,
      log.conditionDescription,
      log.rain1h ?? '',
      log.snow1h ?? '',
      log.collectedAt.toISOString(),
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map((row) => row.map((cell) => `"${cell}"`).join(',')),
    ].join('\n');

    return csvContent;
  }

  async generateXLSX(data: WeatherLogDocument[]): Promise<Buffer> {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Weather Logs');

    // Headers
    worksheet.columns = [
      { header: 'ID', key: 'id', width: 26 },
      { header: 'City', key: 'city', width: 15 },
      { header: 'Country', key: 'country', width: 10 },
      { header: 'Temperature (°C)', key: 'temperature', width: 15 },
      { header: 'Feels Like (°C)', key: 'feelsLike', width: 15 },
      { header: 'Humidity (%)', key: 'humidity', width: 12 },
      { header: 'Pressure (hPa)', key: 'pressure', width: 14 },
      { header: 'Wind Speed (m/s)', key: 'windSpeed', width: 16 },
      { header: 'Wind Dir (°)', key: 'windDirection', width: 12 },
      { header: 'Clouds (%)', key: 'clouds', width: 10 },
      { header: 'Visibility (m)', key: 'visibility', width: 14 },
      { header: 'Condition', key: 'condition', width: 12 },
      { header: 'Description', key: 'description', width: 20 },
      { header: 'Rain 1h (mm)', key: 'rain1h', width: 12 },
      { header: 'Snow 1h (mm)', key: 'snow1h', width: 12 },
      { header: 'Collected At', key: 'collectedAt', width: 22 },
    ];

    // Style header row
    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FF4472C4' },
    };
    worksheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };

    // Add data
    data.forEach((log) => {
      worksheet.addRow({
        id: log._id.toString(),
        city: log.city,
        country: log.country,
        temperature: log.temperature,
        feelsLike: log.feelsLike,
        humidity: log.humidity,
        pressure: log.pressure,
        windSpeed: log.windSpeed,
        windDirection: log.windDirection,
        clouds: log.clouds,
        visibility: log.visibility,
        condition: log.condition,
        description: log.conditionDescription,
        rain1h: log.rain1h,
        snow1h: log.snow1h,
        collectedAt: log.collectedAt.toISOString(),
      });
    });

    const arrayBuffer = await workbook.xlsx.writeBuffer();
    return Buffer.from(arrayBuffer);
  }
}
