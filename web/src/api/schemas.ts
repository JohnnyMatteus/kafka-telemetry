import { z } from 'zod'

export const SENSOR_TYPES = ['TEMPERATURE', 'HUMIDITY', 'CO2', 'LUMINOSITY'] as const
export const sensorTypeSchema = z.enum(SENSOR_TYPES)
export type SensorType = z.infer<typeof sensorTypeSchema>

const date = z.coerce.date()

export const sensorSchema = z.object({
  window_start: date,
  window_end: date,
  sample_count: z.number().int(),
  mean: z.number(),
  min: z.number(),
  max: z.number(),
  unit: z.string(),
  alert: z.object({ rule: z.string(), observed: z.number(), limit: z.number() }).nullish(),
})
export type Sensor = z.infer<typeof sensorSchema>

export const greenhouseSchema = z.object({
  greenhouse_id: z.string(),
  sensors: z.partialRecord(sensorTypeSchema, sensorSchema),
})
export type Greenhouse = z.infer<typeof greenhouseSchema>
export const greenhousesResponse = z.object({ greenhouses: z.array(greenhouseSchema) })

export const seriesPointSchema = z.object({
  window_start: date,
  window_end: date,
  sample_count: z.number().int(),
  mean: z.number(),
  min: z.number(),
  max: z.number(),
})
export type SeriesPoint = z.infer<typeof seriesPointSchema>

export const seriesResponse = z.object({
  greenhouse_id: z.string(),
  series: z.partialRecord(
    sensorTypeSchema,
    z.object({ unit: z.string(), points: z.array(seriesPointSchema) }),
  ),
})
export type SeriesResponse = z.infer<typeof seriesResponse>

export const alertSchema = z.object({
  alert_id: z.string(),
  greenhouse_id: z.string(),
  sensor_type: sensorTypeSchema,
  window_start: date,
  window_end: date,
  rule: z.enum(['ABOVE_MAX', 'BELOW_MIN']),
  observed_value: z.number(),
  limit_value: z.number(),
  acknowledged_at: date.nullable(),
  note: z.string().nullable(),
})
export type Alert = z.infer<typeof alertSchema>
export const alertsResponse = z.object({ alerts: z.array(alertSchema) })

export const dlqMessageSchema = z.object({
  partition: z.number().int(),
  offset: z.number().int(),
  timestamp: date,
  key: z.string().nullable().optional(),
  reason: z.string(),
  error: z.string().nullable().optional(),
  source_topic: z.string(),
  source_partition: z.number().int(),
  source_offset: z.number().int(),
})
export type DlqMessage = z.infer<typeof dlqMessageSchema>
export const dlqResponse = z.object({ messages: z.array(dlqMessageSchema) })

export const thresholdsResponse = z.object({
  thresholds: z.partialRecord(
    sensorTypeSchema,
    z.object({ min: z.number(), max: z.number(), unit: z.string() }),
  ),
})
export type Thresholds = z.infer<typeof thresholdsResponse>['thresholds']

export const pipelineHealthSchema = z.object({
  group: z.string(),
  group_state: z.string(),
  topic: z.string(),
  total_lag: z.number(),
  partitions: z.array(
    z.object({
      partition: z.number().int(),
      committed_offset: z.number(),
      end_offset: z.number(),
      lag: z.number(),
    }),
  ),
  dlq_messages: z.number(),
  throughput_records_per_second: z.number(),
  throughput_window_seconds: z.number(),
})
export type PipelineHealth = z.infer<typeof pipelineHealthSchema>
