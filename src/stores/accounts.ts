import { defineStore } from 'pinia'
import { ref } from 'vue'

export type AccountType = 'ldap' | 'local'

export interface LabelItem {
  text: string
}

export interface Account {
  id: string
  label: LabelItem[]
  type: AccountType
  login: string
  password: string | null
}

const STORAGE_KEY = 'accounts-form'

const createId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

const isAccountType = (value: unknown): value is AccountType =>
  value === 'ldap' || value === 'local'

const parseLabels = (value: unknown): LabelItem[] => {
  if (!Array.isArray(value)) {
    return []
  }

  return value
    .map((item) => {
      if (typeof item !== 'object' || item === null) {
        return null
      }
      const maybeText = 'text' in item ? (item as { text?: unknown }).text : undefined
      return typeof maybeText === 'string' ? { text: maybeText } : null
    })
    .filter((item): item is LabelItem => item !== null)
}

const parseAccount = (value: unknown): Account | null => {
  if (typeof value !== 'object' || value === null) {
    return null
  }

  const raw = value as {
    id?: unknown
    label?: unknown
    type?: unknown
    login?: unknown
    password?: unknown
  }

  const type = isAccountType(raw.type) ? raw.type : 'local'

  return {
    id: typeof raw.id === 'string' ? raw.id : createId(),
    label: parseLabels(raw.label),
    type,
    login: typeof raw.login === 'string' ? raw.login : '',
    password:
      type === 'ldap'
        ? null
        : typeof raw.password === 'string'
          ? raw.password
          : ''
  }
}

const createEmptyAccount = (): Account => ({
  id: createId(),
  label: [],
  type: 'local',
  login: '',
  password: ''
})

export const useAccountsStore = defineStore('accounts', () => {
  const accounts = ref<Account[]>([])

  const persist = () => {
    if (typeof window === 'undefined') {
      return
    }
    localStorage.setItem(STORAGE_KEY, JSON.stringify(accounts.value))
  }

  const load = () => {
    if (typeof window === 'undefined') {
      return
    }
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) {
      return
    }
    try {
      const data = JSON.parse(raw)
      if (Array.isArray(data)) {
        accounts.value = data
          .map((item) => parseAccount(item))
          .filter((item): item is Account => item !== null)
      }
    } catch {
      accounts.value = []
    }
  }

  const addAccount = () => {
    accounts.value = [...accounts.value, createEmptyAccount()]
    persist()
  }

  const removeAccount = (id: string) => {
    accounts.value = accounts.value.filter((account) => account.id !== id)
    persist()
  }

  const updateAccount = (id: string, updates: Partial<Omit<Account, 'id'>>) => {
    const index = accounts.value.findIndex((account) => account.id === id)
    if (index === -1) {
      return
    }

    const current = accounts.value[index]
    if (!current) {
      return
    }
    const next: Account = {
      id: current.id,
      label: updates.label ?? current.label,
      type: updates.type ?? current.type,
      login: updates.login ?? current.login,
      password: updates.password ?? current.password
    }

    if (next.type === 'ldap') {
      next.password = null
    } else if (next.password === null) {
      next.password = ''
    }

    accounts.value.splice(index, 1, next)
    persist()
  }

  const setType = (id: string, type: AccountType) => {
    const current = accounts.value.find((account) => account.id === id)
    if (!current) {
      return
    }

    const nextPassword = type === 'ldap' ? null : current.password ?? ''
    updateAccount(id, { type, password: nextPassword })
  }

  return {
    accounts,
    load,
    addAccount,
    removeAccount,
    updateAccount,
    setType
  }
})
