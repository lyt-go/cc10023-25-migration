<template>
  <section class="page" data-module="training_window">
    <header class="page-head">
      <div>
        <h2>训练窗口</h2>
        <p class="page-desc">
          把 weekly / Cron 两类旧训练周期迁移成带版本、时区、起止日期和分区范围的窗口，
          调度按训练输入时段清单逐日期分区下发，训练产物保留实际读取的分区与周期来源。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="reload" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">窗口版本数</span>
        <strong class="stat-value">{{ overview?.window_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">时区待核窗口</span>
        <strong class="stat-value">{{ overview?.pending_timezone.length ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">仍占用分区的跑次</span>
        <strong class="stat-value">{{ overview?.unconsumed_runs.length ?? 0 }}</strong>
      </article>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</p>

    <form class="filter-bar" @submit.prevent="runScenario">
      <label class="filter-item">
        <span>调度 ID</span>
        <input v-model.number="scheduleId" data-testid="schedule-id" type="number" min="1" />
      </label>
      <label class="filter-item">
        <span>旧跑次受理时间</span>
        <input v-model="acceptedAt" data-testid="accepted-at" placeholder="2026-11-16T02:00:00+08:00" />
      </label>
      <label class="filter-item">
        <span>新周期表达式</span>
        <input v-model="newCycle" data-testid="new-cycle" placeholder="0 3 * * 2 或 weekly" />
      </label>
      <label class="filter-item">
        <span>时区（留空走旧配置）</span>
        <input v-model="timezone" data-testid="timezone" placeholder="Asia/Shanghai" />
      </label>
      <label class="filter-item">
        <span>生效日期</span>
        <input v-model="effectiveDate" data-testid="effective-date" placeholder="2026-11-16" />
      </label>
      <label class="filter-item">
        <span>重合日期归属</span>
        <select v-model="policy" data-testid="policy">
          <option value="carry_over_old">沿用旧计划</option>
          <option value="handover_unconsumed">未消费分区交给新窗口</option>
        </select>
      </label>
      <button class="btn primary" type="submit" data-testid="run-scenario" :disabled="busy">
        {{ busy ? '执行中…' : '受理旧跑次 → 迁移 → 训练' }}
      </button>
    </form>

    <section v-if="migrateResult" class="panel" data-testid="migrate-result">
      <h3>迁移结果</h3>
      <dl class="kv">
        <dt>周期版本</dt>
        <dd data-testid="migrate-version">{{ migrateResult.window_version }}</dd>
        <dt>周期来源</dt>
        <dd>{{ migrateResult.cycle_source }}</dd>
        <dt>时区</dt>
        <dd>
          {{ migrateResult.timezone }}
          <span v-if="migrateResult.timezone_status === '待确认'" class="tag warn">待确认</span>
          <span v-else class="tag ok">已确认</span>
        </dd>
        <dt>起止日期</dt>
        <dd>{{ migrateResult.start_date }} ~ {{ migrateResult.end_date }}</dd>
        <dt>重合归属</dt>
        <dd>{{ policyLabel(migrateResult.policy) }} · {{ migrateResult.conflicts?.length ?? 0 }} 个重合日期</dd>
      </dl>
      <div v-if="migrateResult.pending_items?.length" class="pending-box" data-testid="pending-items">
        <h4>待核项（旧配置无法确认）</h4>
        <ul>
          <li v-for="(item, index) in migrateResult.pending_items" :key="index">
            <code>{{ item.raw_expression }}</code> — {{ item.reason }}
            <span v-if="item.candidates?.length">（候选：{{ item.candidates.join('、') }}）</span>
          </li>
        </ul>
      </div>
    </section>

    <section v-if="conflicts.length" class="panel" data-testid="conflicts">
      <h3>重合日期归属</h3>
      <table class="data-table">
        <thead>
          <tr><th>日期</th><th>分区</th><th>归属跑次</th><th>旧周期版本</th><th>占用状态</th><th>处理结果</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in conflicts" :key="item.partition" :data-testid="`conflict-${item.date}`">
            <td>{{ item.date }}</td>
            <td>{{ item.partition }}</td>
            <td>{{ item.owner_run_code }}（#{{ item.owner_run_id }}）</td>
            <td>{{ item.owner_cycle_version }}</td>
            <td>{{ item.claim_state }}</td>
            <td>{{ item.resolution }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="manifest" class="panel" data-testid="manifest">
      <h3>训练输入时段清单 {{ manifest.清单编号 }}</h3>
      <p class="page-desc">
        周期版本 {{ manifest.window_version }} · 时区 {{ manifest.timezone }}
        <span v-if="manifest.timezone_status === '待确认'" class="tag warn">时区待核</span>
        · {{ manifest.start_date }} ~ {{ manifest.end_date }}
        · 可下发 {{ manifest.可下发数量 }} / {{ manifest.partition_range?.count ?? 0 }} 个日期分区
      </p>
      <div class="partition-list">
        <span
          v-for="slot in manifest.日期分区"
          :key="slot.分区"
          class="partition-chip"
          :class="slot.状态 === '可下发' ? 'is-free' : 'is-taken'"
          :title="slot.状态 === '可下发' ? '可下发' : `占用：${slot.占用周期版本 ?? slot.周期版本} / 跑次 ${slot.跑次ID ?? '—'}`"
          :data-testid="`slot-${slot.日期}`"
        >{{ slot.日期 }}</span>
      </div>
    </section>

    <section v-if="dispatchResult" class="panel" data-testid="dispatch-result">
      <h3>下发结果</h3>
      <p>{{ dispatchResult.message }}</p>
      <dl class="kv">
        <dt>跑次</dt>
        <dd>{{ dispatchResult.run_id ?? '—' }}</dd>
        <dt>周期版本</dt>
        <dd>{{ dispatchResult.cycle_version }}</dd>
        <dt>实际下发</dt>
        <dd data-testid="dispatched">{{ dispatchResult.dispatched_partitions?.join(', ') || '无' }}</dd>
        <dt>跳过</dt>
        <dd>{{ dispatchResult.skipped_count ?? 0 }} 个</dd>
      </dl>
      <ul v-if="dispatchResult.skipped?.length" class="reason-list">
        <li v-for="item in dispatchResult.skipped.slice(0, 8)" :key="item.partition">
          {{ item.partition }}：{{ item.reason }}
        </li>
      </ul>
    </section>

    <section class="panel" data-testid="timeline">
      <h3>周期版本与跑次</h3>
      <table class="data-table">
        <thead>
          <tr><th>跑次</th><th>周期版本</th><th>状态</th><th>起止日期</th><th>分区数</th><th>实际读取</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in timeline?.history ?? []" :key="row.run_id" :data-testid="`run-${row.run_id}`">
            <td>{{ row.run_code }}（#{{ row.run_id }}）</td>
            <td>{{ row.cycle_version }}</td>
            <td>{{ row.status }}</td>
            <td>{{ row.start_date }} ~ {{ row.end_date }}</td>
            <td>{{ row.partitions.length }}</td>
            <td class="mono">{{ row.partitions[0] }} … {{ row.partitions[row.partitions.length - 1] }}</td>
          </tr>
          <tr v-if="!(timeline?.history ?? []).length">
            <td colspan="6" class="empty-state">暂无跑次，先受理一次旧跑次或发起训练</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel" data-testid="replay">
      <h3>从模型详情回放</h3>
      <div class="filter-bar">
        <label class="filter-item">
          <span>模型</span>
          <select v-model.number="modelId" data-testid="model-select">
            <option v-for="model in models" :key="model.id" :value="model.id">
              {{ model.模型编号 }} · {{ model.模型名称 }} · {{ model.周期版本 ?? '—' }}
            </option>
          </select>
        </label>
        <button class="btn" type="button" data-testid="replay-btn" @click="replay" :disabled="!modelId">
          回放原分区
        </button>
      </div>
      <dl v-if="replayResult?.ok" class="kv" data-testid="replay-result">
        <dt>周期版本</dt>
        <dd>{{ replayResult.cycle_version }}</dd>
        <dt>周期来源</dt>
        <dd>{{ replayResult.cycle_source }} · <code>{{ replayResult.raw_expression }}</code></dd>
        <dt>原时间范围</dt>
        <dd>{{ replayResult.start_date }} ~ {{ replayResult.end_date }}</dd>
        <dt>原分区</dt>
        <dd class="mono">{{ replayResult.partitions?.join(', ') }}</dd>
        <dt>调度关联</dt>
        <dd>
          调度 #{{ replayResult.schedule_link?.schedule_id }} 当时周期
          {{ replayResult.schedule_link?.cycle_version }}
          <span v-if="replayResult.schedule_link?.cycle_changed" class="tag warn">
            当前已换到 {{ replayResult.schedule_link?.current_cycle_version }}
          </span>
        </dd>
      </dl>
      <p v-else-if="replayResult" class="error-text">{{ replayResult.message }}</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  acceptLegacyRun,
  dispatchManifest,
  listModels,
  loadManifest,
  loadOverview,
  loadTimeline,
  migrateSchedule,
  replayModel,
  trainFromWindow,
} from '@/api/training-window'
import type {
  Conflict,
  DispatchResult,
  Manifest,
  MigrateResult,
  ModelRow,
  ReplayResult,
  Timeline,
  WindowOverview,
} from '@/api/training-window'

const scheduleId = ref(4)
const acceptedAt = ref('2026-11-16T02:00:00+08:00')
const newCycle = ref('0 3 * * 2')
const timezone = ref('Asia/Shanghai')
const effectiveDate = ref('2026-11-16')
const policy = ref('carry_over_old')
const busy = ref(false)
const errorMessage = ref('')
const notice = ref('')
const overview = ref<WindowOverview | null>(null)
const timeline = ref<Timeline | null>(null)
const manifest = ref<Manifest | null>(null)
const migrateResult = ref<MigrateResult | null>(null)
const dispatchResult = ref<DispatchResult | null>(null)
const conflicts = ref<Conflict[]>([])
const models = ref<ModelRow[]>([])
const modelId = ref(0)
const replayResult = ref<ReplayResult | null>(null)

function policyLabel(value?: string) {
  if (value === 'carry_over_old') return '沿用旧计划'
  if (value === 'handover_unconsumed') return '未消费分区交给新窗口'
  return value ?? '沿用旧计划'
}

async function reload() {
  errorMessage.value = ''
  try {
    overview.value = await loadOverview()
    timeline.value = await loadTimeline(scheduleId.value)
    manifest.value = await loadManifest(scheduleId.value)
    if (!models.value.length) {
      const payload = await listModels()
      models.value = payload.items
      if (!modelId.value && payload.items.length) modelId.value = payload.items[0].id
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '训练窗口读取失败'
  }
}

async function runScenario() {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  dispatchResult.value = null
  conflicts.value = []
  try {
    const accepted = await acceptLegacyRun(scheduleId.value, acceptedAt.value)
    if (!accepted.ok) throw new Error(accepted.message)
    notice.value = `旧跑次 ${accepted.run_id} 已受理，冻结周期版本 ${accepted.cycle_version}`
    const migrated = await migrateSchedule(scheduleId.value, {
      new_cycle: newCycle.value || undefined,
      timezone: timezone.value || undefined,
      effective_date: effectiveDate.value || undefined,
      policy: policy.value,
    })
    if (!migrated.ok) throw new Error(migrated.message)
    migrateResult.value = migrated
    conflicts.value = migrated.conflicts ?? []
    manifest.value = migrated.manifest ?? null
    const dispatched = await dispatchManifest(scheduleId.value, migrated.window_version)
    if (!dispatched.ok) throw new Error(dispatched.message)
    dispatchResult.value = dispatched
    if (dispatched.run_id) {
      // 用刚下发的跑次继续训练，避免再建一次跑次并重复占分区。
      const trained = await trainFromWindow(scheduleId.value, { run_id: dispatched.run_id })
      if (trained.ok) {
        dispatchResult.value = { ...trained, skipped: dispatched.skipped }
        notice.value += `；新窗口训练完成，实际读取 ${trained.dispatched_partitions?.length ?? 0} 个日期分区`
      } else {
        notice.value += `；${trained.message}`
      }
    } else {
      notice.value += '；首批训练没有可下发的日期分区，重合日期仍归旧跑次'
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '场景执行失败'
  } finally {
    busy.value = false
  }
}

async function replay() {
  errorMessage.value = ''
  try {
    replayResult.value = await replayModel(modelId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回放失败'
  }
}

onMounted(reload)
</script>
