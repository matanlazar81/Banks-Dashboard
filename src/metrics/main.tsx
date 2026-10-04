import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './metrics.css'
import Metrics from './Metrics.tsx'
import PageErrorBoundary from '../new-dashboard/PageErrorBoundary.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PageErrorBoundary>
      <Metrics />
    </PageErrorBoundary>
  </StrictMode>,
)
