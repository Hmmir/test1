<script setup lang="ts">
import { onMounted, reactive } from 'vue'
import { Delete, Plus } from '@element-plus/icons-vue'
import { useAccountsStore } from './stores/accounts'
import type { Account, AccountType, LabelItem } from './stores/accounts'

type Draft = {
  label: string
  type: AccountType
  login: string
  password: string
}

type FieldErrors = {
  label: boolean
  login: boolean
  password: boolean
}

const store = useAccountsStore()
const drafts = reactive<Record<string, Draft>>({})
const errors = reactive<Record<string, FieldErrors>>({})

const createErrors = (): FieldErrors => ({
  label: false,
  login: false,
  password: false
})

const toLabelText = (labels: LabelItem[]) => labels.map((item) => item.text).join('; ')

const toLabelItems = (value: string): LabelItem[] =>
  value
    .split(';')
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
    .map((text) => ({ text }))

const ensureDraft = (account: Account) => {
  if (!drafts[account.id]) {
    drafts[account.id] = {
      label: toLabelText(account.label),
      type: account.type,
      login: account.login,
      password: account.password ?? ''
    }
  }
  if (!errors[account.id]) {
    errors[account.id] = createErrors()
  }
}

const syncDrafts = () => {
  const ids = new Set(store.accounts.map((account) => account.id))
  store.accounts.forEach((account) => ensureDraft(account))
  Object.keys(drafts).forEach((id) => {
    if (!ids.has(id)) {
      delete drafts[id]
      delete errors[id]
    }
  })
}

const getDraftById = (id: string): Draft | null => {
  const draft = drafts[id]
  if (draft) {
    return draft
  }
  const account = store.accounts.find((item) => item.id === id)
  if (!account) {
    return null
  }
  ensureDraft(account)
  const next = drafts[id]
  return next ?? null
}

const getErrorsById = (id: string): FieldErrors => {
  const existing = errors[id]
  if (existing) {
    return existing
  }
  const next = createErrors()
  errors[id] = next
  return next
}

const draftFor = (account: Account): Draft => {
  ensureDraft(account)
  const draft = drafts[account.id]
  if (draft) {
    return draft
  }
  const next: Draft = {
    label: toLabelText(account.label),
    type: account.type,
    login: account.login,
    password: account.password ?? ''
  }
  drafts[account.id] = next
  return next
}

const errorsFor = (account: Account): FieldErrors => {
  ensureDraft(account)
  const existing = errors[account.id]
  if (existing) {
    return existing
  }
  const next = createErrors()
  errors[account.id] = next
  return next
}

const handleAdd = () => {
  store.addAccount()
  const account = store.accounts[store.accounts.length - 1]
  if (account) {
    ensureDraft(account)
  }
}

const handleRemove = (id: string) => {
  store.removeAccount(id)
  delete drafts[id]
  delete errors[id]
}

const commitLabel = (id: string) => {
  const draft = getDraftById(id)
  if (!draft) {
    return
  }
  const isValid = draft.label.length <= 50
  const fieldErrors = getErrorsById(id)
  fieldErrors.label = !isValid
  if (!isValid) {
    return
  }
  store.updateAccount(id, { label: toLabelItems(draft.label) })
}

const commitLogin = (id: string) => {
  const draft = getDraftById(id)
  if (!draft) {
    return
  }
  const trimmed = draft.login.trim()
  const isValid = trimmed.length > 0 && trimmed.length <= 100
  const fieldErrors = getErrorsById(id)
  fieldErrors.login = !isValid
  if (!isValid) {
    return
  }
  draft.login = trimmed
  store.updateAccount(id, { login: trimmed })
}

const commitPassword = (id: string) => {
  const draft = getDraftById(id)
  if (!draft) {
    return
  }
  if (draft.type === 'ldap') {
    const fieldErrors = getErrorsById(id)
    fieldErrors.password = false
    store.updateAccount(id, { password: null })
    return
  }
  const trimmed = draft.password.trim()
  const isValid = trimmed.length > 0 && trimmed.length <= 100
  const fieldErrors = getErrorsById(id)
  fieldErrors.password = !isValid
  if (!isValid) {
    return
  }
  draft.password = trimmed
  store.updateAccount(id, { password: trimmed })
}

const handleTypeChange = (id: string) => {
  const draft = getDraftById(id)
  if (!draft) {
    return
  }
  store.setType(id, draft.type)
  if (draft.type === 'ldap') {
    draft.password = ''
    const fieldErrors = getErrorsById(id)
    fieldErrors.password = false
  } else {
    commitPassword(id)
  }
}

onMounted(() => {
  store.load()
  syncDrafts()
})
</script>

<template>
  <main class="page">
    <section class="card">
      <header class="card-header">
        <div>
          <p class="eyebrow">Управление доступом</p>
          <h1 class="title">Учетные записи</h1>
        </div>
        <el-button
          class="add-button"
          type="primary"
          :icon="Plus"
          aria-label="Добавить учетную запись"
          @click="handleAdd"
        >
          Добавить
        </el-button>
      </header>

      <el-alert
        class="hint"
        type="info"
        show-icon
        :closable="false"
        title="Для указания нескольких меток для одной пары логин/пароль используйте разделитель ;"
      />

      <div class="table">
        <div class="table-head">
          <span>Метка</span>
          <span>Тип записи</span>
          <span>Логин</span>
          <span>Пароль</span>
          <span></span>
        </div>

        <div
          v-for="(account, index) in store.accounts"
          :key="account.id"
          class="table-row"
          :style="{ animationDelay: `${index * 60}ms` }"
        >
          <div class="field">
            <span class="field-label">Метка</span>
            <el-input
              v-model="draftFor(account).label"
              placeholder="Метка"
              maxlength="50"
              :class="{ 'field-error': errorsFor(account).label }"
              @blur="commitLabel(account.id)"
            />
          </div>

          <div class="field">
            <span class="field-label">Тип записи</span>
            <el-select
              v-model="draftFor(account).type"
              placeholder="Тип записи"
              @change="handleTypeChange(account.id)"
            >
              <el-option label="Локальная" value="local" />
              <el-option label="LDAP" value="ldap" />
            </el-select>
          </div>

          <div class="field">
            <span class="field-label">Логин</span>
            <el-input
              v-model="draftFor(account).login"
              placeholder="Логин"
              maxlength="100"
              :class="{ 'field-error': errorsFor(account).login }"
              @blur="commitLogin(account.id)"
            />
          </div>

          <div class="field">
            <span class="field-label">Пароль</span>
            <el-input
              v-if="draftFor(account).type === 'local'"
              v-model="draftFor(account).password"
              type="password"
              show-password
              placeholder="Пароль"
              maxlength="100"
              :class="{ 'field-error': errorsFor(account).password }"
              @blur="commitPassword(account.id)"
            />
            <div v-else class="password-hidden" aria-hidden="true"></div>
          </div>

          <div class="actions">
            <el-button
              type="danger"
              text
              circle
              :icon="Delete"
              aria-label="Удалить учетную запись"
              @click="handleRemove(account.id)"
            />
          </div>
        </div>
      </div>
    </section>
  </main>
</template>
