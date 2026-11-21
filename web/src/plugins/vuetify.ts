import { createVuetify, type ThemeDefinition } from 'vuetify'
import { aliases, mdi } from 'vuetify/iconsets/mdi-svg'

// Tokens da direção de identidade Canteiro (design/direcao-de-identidade.md, seção 7).
const canteiroLight: ThemeDefinition = {
  dark: false,
  colors: {
    background: '#F1F3EC',
    surface: '#FBFCF8',
    'surface-light': '#EEF1E8',
    'surface-variant': '#E2E7DA',
    'on-surface-variant': '#1A2117',
    'on-background': '#1A2117',
    'on-surface': '#1A2117',
    primary: '#2F6A31',
    'on-primary': '#FFFFFF',
    secondary: '#6B5137',
    'on-secondary': '#FFFFFF',
    accent: '#1F6E8C',
    'on-accent': '#FFFFFF',
    success: '#2F6A31',
    warning: '#8F5A00',
    error: '#A61B1B',
    info: '#1F6E8C',
    'dev-cold': '#2B6CB0',
    'dev-hot': '#C2410C',
    'band-fill': '#CFE3C4',
    'band-edge': '#5E8F55',
  },
  variables: {
    'border-color': '#1A2117',
    'border-opacity': 0.14,
    'medium-emphasis-opacity': 0.7,
    'disabled-opacity': 0.4,
    'hover-opacity': 0.05,
    'focus-opacity': 0.12,
  },
}

const canteiroDark: ThemeDefinition = {
  dark: true,
  colors: {
    background: '#0F140E',
    surface: '#161D15',
    'surface-light': '#1C241A',
    'surface-variant': '#253022',
    'on-surface-variant': '#E4EBDD',
    'on-background': '#E4EBDD',
    'on-surface': '#E4EBDD',
    primary: '#9BD18F',
    'on-primary': '#10290F',
    secondary: '#D2B48F',
    'on-secondary': '#2B1D0E',
    accent: '#86C9E3',
    'on-accent': '#0B2A36',
    success: '#9BD18F',
    warning: '#F3C26B',
    error: '#FF9C8F',
    info: '#86C9E3',
    'dev-cold': '#7FB2F0',
    'dev-hot': '#FF9A5C',
    'band-fill': '#24361F',
    'band-edge': '#6FA864',
  },
  variables: {
    'border-color': '#E4EBDD',
    'border-opacity': 0.14,
    'medium-emphasis-opacity': 0.7,
    'disabled-opacity': 0.4,
    'hover-opacity': 0.07,
    'focus-opacity': 0.14,
  },
}

export default createVuetify({
  icons: { defaultSet: 'mdi', aliases, sets: { mdi } },
  theme: { defaultTheme: 'canteiroLight', themes: { canteiroLight, canteiroDark } },
  defaults: {
    VBtn: { rounded: 'md', variant: 'flat', class: 'text-none', height: 40 },
    VCard: { rounded: 'md', variant: 'flat', elevation: 0, border: true },
    VChip: { rounded: 'sm', variant: 'tonal', size: 'small', label: true },
    VTextField: { variant: 'outlined', density: 'comfortable', rounded: 'md' },
    VNavigationDrawer: { location: 'right', temporary: false, width: 420, elevation: 0, border: 's' },
    VAppBar: { flat: true, height: 56, color: 'background' },
    VDataTable: { density: 'comfortable', hover: true },
    VAlert: { variant: 'tonal', rounded: 'md', border: 'start' },
    VTooltip: { location: 'top' },
  },
})
