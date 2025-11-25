import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { ConfigService } from '@nestjs/config';
import OpenAI from 'openai';

import { WeatherLog, WeatherLogDocument } from '../schemas/weather-log.schema';
type FilterType = {
  city?: string;
  collectedAt?: { $gte: Date; $lte?: Date };
};

interface InsightsResponse {
  period: {
    start: Date;
    end: Date;
    totalRecords: number;
  };
  statistics: {
    temperature: {
      avg: number;
      min: number;
      max: number;
    };
    humidity: {
      avg: number;
      min: number;
      max: number;
    };
    windSpeed: {
      avg: number;
      max: number;
    };
  };
  mostCommonCondition: string | null;
  conditionDistribution: Record<string, number>;
  insights: string[];
  aiSummary?: string;
}

@Injectable()
export class InsightsService {
  private client: OpenAI;

  constructor(
    @InjectModel(WeatherLog.name)
    private weatherModel: Model<WeatherLogDocument>,
    private configService: ConfigService,
  ) {
    const endpoint = this.configService.get<string>('AZURE_OPENAI_ENDPOINT');
    const apiKey = this.configService.get<string>('AZURE_OPENAI_KEY');

    this.client = new OpenAI({
      apiKey,
      baseURL: `${endpoint}/openai/deployments`,
      defaultHeaders: { 'api-key': apiKey },
    });
  }

  async generateInsights(
    city?: string,
  ): Promise<InsightsResponse | { message: string; insights: any[] }> {
    const match: FilterType = {};
    if (city) match.city = city;

    // Busca dados dos últimos 7 dias
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    match.collectedAt = { $gte: sevenDaysAgo };

    const data = await this.weatherModel
      .find(match)
      .sort({ collectedAt: -1 })
      .exec();

    if (data.length === 0) {
      return { message: 'No data available for insights', insights: [] };
    }

    const insights = this.analyzeData(data);

    // IA via Azure OpenAI
    const azureKey = this.configService.get<string>('AZURE_OPENAI_KEY');
    if (azureKey) {
      const aiInsights = await this.generateAIInsights(data);
      insights.aiSummary = aiInsights;
    }

    return insights;
  }

  private analyzeData(data: WeatherLogDocument[]): InsightsResponse {
    const temps = data.map((d) => d.temperature);
    const humidities = data.map((d) => d.humidity);
    const windSpeeds = data.map((d) => d.windSpeed);

    const avgTemp = temps.reduce((a, b) => a + b, 0) / temps.length;
    const avgHumidity =
      humidities.reduce((a, b) => a + b, 0) / humidities.length;
    const avgWindSpeed =
      windSpeeds.reduce((a, b) => a + b, 0) / windSpeeds.length;

    const conditions = data.map((d) => d.condition);
    const conditionCounts = conditions.reduce(
      (acc, cond) => {
        acc[cond] = (acc[cond] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>,
    );

    const mostCommonCondition = Object.entries(conditionCounts).sort(
      (a, b) => b[1] - a[1],
    )[0];

    const insights: string[] = [];

    // Temperatura
    if (avgTemp > 30) {
      insights.push(
        'Temperaturas elevadas detectadas. Recomenda-se hidratação frequente.',
      );
    } else if (avgTemp < 10) {
      insights.push(
        'Temperaturas baixas no período. Agasalhe-se adequadamente.',
      );
    }

    // Umidade
    if (avgHumidity < 30) {
      insights.push(
        '💨 Umidade do ar muito baixa. Considere usar umidificadores.',
      );
    } else if (avgHumidity > 80) {
      insights.push('💧 Alta umidade detectada. Possibilidade de chuvas.');
    }

    // Vento
    if (avgWindSpeed > 10) {
      insights.push('🌬️ Ventos fortes registrados. Atenção a objetos soltos.');
    }

    // Tendência
    if (data.length >= 2) {
      const recent = data.slice(0, Math.ceil(data.length / 2));
      const older = data.slice(Math.ceil(data.length / 2));
      const recentAvg =
        recent.reduce((a, b) => a + b.temperature, 0) / recent.length;
      const olderAvg =
        older.reduce((a, b) => a + b.temperature, 0) / older.length;

      if (recentAvg > olderAvg + 2) {
        insights.push('📈 Tendência de aquecimento nos últimos dias.');
      } else if (recentAvg < olderAvg - 2) {
        insights.push('📉 Tendência de resfriamento nos últimos dias.');
      }
    }

    return {
      period: {
        start: data[data.length - 1].collectedAt,
        end: data[0].collectedAt,
        totalRecords: data.length,
      },
      statistics: {
        temperature: {
          avg: Math.round(avgTemp * 10) / 10,
          min: Math.min(...temps),
          max: Math.max(...temps),
        },
        humidity: {
          avg: Math.round(avgHumidity),
          min: Math.min(...humidities),
          max: Math.max(...humidities),
        },
        windSpeed: {
          avg: Math.round(avgWindSpeed * 10) / 10,
          max: Math.max(...windSpeeds),
        },
      },
      mostCommonCondition: mostCommonCondition ? mostCommonCondition[0] : null,
      conditionDistribution: conditionCounts,
      insights,
    };
  }

  private async generateAIInsights(
    data: WeatherLogDocument[],
  ): Promise<string> {
    try {
      const summary = {
        city: data[0].city,
        records: data.length,
        avgTemp: data.reduce((a, b) => a + b.temperature, 0) / data.length,
        conditions: [...new Set(data.map((d) => d.condition))],
      };

      const deployment = this.configService.get<string>(
        'AZURE_OPENAI_DEPLOYMENT',
        'gpt-35-turbo',
      );

      const response = await this.client.chat.completions.create({
        model: deployment,
        messages: [
          {
            role: 'system',
            content:
              'Você é um meteorologista. Resuma insights climáticos em 2-3 frases.',
          },
          {
            role: 'user',
            content: `Analise estes dados climáticos: ${JSON.stringify(
              summary,
            )}`,
          },
        ],
        max_tokens: 150,
      });

      return (
        response.choices[0]?.message?.content ??
        'Não foi possível gerar insights.'
      );
    } catch (e) {
      console.error(e);
      return 'Erro ao gerar insights via Azure OpenAI.';
    }
  }
}
