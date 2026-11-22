import { VueQueryPlugin } from '@tanstack/vue-query'
import { createApp } from 'vue'
import '@fontsource-variable/archivo/wdth.css'
import '@fontsource/azeret-mono/500.css'
import 'vuetify/styles'
import './styles/main.css'
import App from './App.vue'
import vuetify from './plugins/vuetify'
import router from './router'

createApp(App).use(vuetify).use(router).use(VueQueryPlugin).mount('#app')
