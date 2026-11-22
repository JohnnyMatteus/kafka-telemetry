<script setup lang="ts">
defineProps<{ title: string; text?: string; tone?: 'info' | 'error' | 'success' }>()
</script>

<template>
  <v-alert :type="tone ?? 'info'" :title="title" :text="text" class="ma-4" role="status">
    <slot />
  </v-alert>
</template>
