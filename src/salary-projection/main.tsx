import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './salary-projection.css'
import SalaryProjection from './SalaryProjection.tsx'
import PageErrorBoundary from '../new-dashboard/PageErrorBoundary.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PageErrorBoundary>
      <SalaryProjection />
    </PageErrorBoundary>
  </StrictMode>,
)
