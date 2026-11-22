/** 训练窗口接口：迁移、清单、下发、训练与回放，与后端 /api/train-windows 对齐。 */

import { request } from '@/api/client'

export type PendingItem = {
  type: string
  field: string
  raw_expression: string
  reason: string
  candidates?: string[]
}

export type PartitionRange = {
  field: string
  start: string | null
  end: string | null
  count: number
  partitions: string[]
}

export type TrainingWindow = {
  id: number
  窗口编号: string
  关联流水线: string
  关联调度: string
  周期版本: string
  周期来源: string
  调度类型: string
  来源表达式: string
  原表达式: string
  时区: string
  时区状态: string
  开始日期: string
  结束日期: string
  起止日期: string
  分区字段: string
  分区范围: PartitionRange
  待核项: PendingItem[]
  窗口状态: string
  冲突处理?: string
}

export type ManifestSlot = {
  日期: string
  分区: string
  周期版本: string
  时区: string
  状态: string
  跑次ID?: number
  占用状态?: string
  占用周期版本?: string
}

export type Manifest = {
  ok: boolean
  reason?: string
  message?: string
  清单编号?: string
  window_version?: string
  cycle_version?: string
  cycle_source?: string
  raw_expression?: string
  timezone?: string
  timezone_status?: string
  pending_items?: PendingItem[]
  start_date?: string
  end_date?: string
  partition_range?: PartitionRange
  输入时段?: {
    周期版本: string
    周期来源: string
    时区: string
    开始日期: string
    结束日期: string
    分区范围: PartitionRange
  }[]
  日期分区?: ManifestSlot[]
  可下发分区?: string[]
  可下发数量?: number
  冲突分区?: { partition: string; resolution: string; owner_cycle_version: string }[]
  冲突数量?: number
  状态?: string
}

export type Conflict = {
  partition: string
  date: string
  owner_run_id: number
  owner_run_code: string
  owner_cycle_version: string
  claim_state: string
  resolution: string
  assigned_cycle_version: string
}

export type TrainingRun = {
  id: number
  运行编号: string
  周期版本: string
  窗口编号: string
  status: string
  窗口开始: string
  窗口结束: string
  下发分区: string[]
  已消费分区: string[]
  预留分区: string[]
  时区: string
  时区状态: string
}

export type MigrateResult = {
  ok: boolean
  message: string
  window_id?: number
  window_version?: string
  cycle_source?: string
  timezone?: string
  timezone_status?: string
  start_date?: string
  end_date?: string
  raw_expression?: string
  pending_items?: PendingItem[]
  policy?: string
  conflicts?: Conflict[]
  manifest?: Manifest
}

export type DispatchResult = {
  ok: boolean
  message: string
  run_id?: number | null
  cycle_version?: string
  cycle_source?: string
  timezone?: string
  start_date?: string
  end_date?: string
  dispatched?: { partition: string; date: string; cycle_version: string }[]
  dispatched_partitions?: string[]
  skipped?: { partition: string; reason: string; cycle_version: string }[]
  skipped_count?: number
  blocked?: boolean
}

export type ReplayResult = {
  ok: boolean
  message?: string
  mode?: string
  model_id?: number
  model_code?: string
  run_id?: number
  run_code?: string
  run_status?: string
  cycle_version?: string
  cycle_source?: string
  raw_expression?: string
  timezone?: string
  start_date?: string
  end_date?: string
  partitions?: string[]
  partition_range?: PartitionRange
  schedule_link?: {
    schedule_id: number
    cycle_version: string
    current_cycle_version: string
    cycle_changed: boolean
  }
}

export type Timeline = {
  ok: boolean
  schedule_id: number
  current_cycle_version?: string
  current_window_id?: string
  windows: TrainingWindow[]
  runs: TrainingRun[]
  history: {
    run_id: number
    run_code: string
    cycle_version: string
    window_id: number
    status: string
    start_date: string
    end_date: string
    partitions: string[]
  }[]
}

export type WindowOverview = {
  ok: boolean
  window_count: number
  run_count: number
  pending_timezone: {
    window_id: number
    window_number: string
    schedule_id: string
    raw_expression: string
    timezone: string
    pending_items: PendingItem[]
  }[]
  unconsumed_runs: {
    run_id: number
    run_code: string
    cycle_version: string
    status: string
    reserved: string[]
    pending_partitions: string[]
    consumed: string[]
  }[]
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，训练窗口数据未更新`)
  return (await response.json()) as T
}

function post<T>(path: string, body: unknown = {}): Promise<T> {
  return call<T>(path, { method: 'POST', body: JSON.stringify(body) })
}

export function loadOverview(): Promise<WindowOverview> {
  return call<WindowOverview>('/api/train-windows/overview')
}

export function loadTimeline(scheduleId: number): Promise<Timeline> {
  return call<Timeline>(`/api/train-windows/schedules/${scheduleId}/timeline`)
}

export function loadManifest(scheduleId: number): Promise<Manifest> {
  return call<Manifest>(`/api/train-windows/schedules/${scheduleId}/manifest`)
}

export function acceptLegacyRun(
  scheduleId: number,
  acceptedAt: string,
): Promise<{ ok: boolean; message: string; run_id: number; cycle_version: string }> {
  return post(`/api/train-windows/schedules/${scheduleId}/accept`, { accepted_at: acceptedAt })
}

export function migrateSchedule(
  scheduleId: number,
  payload: {
    new_cycle?: string
    timezone?: string
    effective_date?: string
    policy: string
  },
): Promise<MigrateResult> {
  return post<MigrateResult>(`/api/train-windows/schedules/${scheduleId}/migrate`, payload)
}

export function dispatchManifest(
  scheduleId: number,
  version?: string,
): Promise<DispatchResult> {
  return post<DispatchResult>(`/api/train-windows/schedules/${scheduleId}/dispatch`, { version })
}

export function trainFromWindow(
  scheduleId: number,
  payload: { version?: string; run_id?: number },
): Promise<DispatchResult> {
  return post<DispatchResult>(`/api/train-windows/schedules/${scheduleId}/train`, payload)
}

export function replayModel(modelId: number): Promise<ReplayResult> {
  return call<ReplayResult>(`/api/train-windows/models/${modelId}/replay`)
}

export type ModelRow = {
  id: number
  模型编号: string
  模型名称: string
  周期版本?: string
  周期来源?: string
  起止日期?: string
  训练分区?: string[]
}

export function listModels(): Promise<{ items: ModelRow[]; total: number }> {
  return call<{ items: ModelRow[]; total: number }>('/api/model_registry?size=50')
}
