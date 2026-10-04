import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './pnl-projection.css'
import PnlProjection from './PnlProjection.tsx'
import PageErrorBoundary from '../new-dashboard/PageErrorBoundary.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PageErrorBoundary>
      <PnlProjection />
    </PageErrorBoundary>
  </StrictMode>,
)
