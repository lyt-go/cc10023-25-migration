import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import TrainingWindowView from '@/views/TrainingWindowView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/training_window', name: 'training_window', component: TrainingWindowView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
