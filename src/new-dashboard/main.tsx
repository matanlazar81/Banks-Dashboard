import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './new-dashboard.css'
import NewBankDashboard from './NewBankDashboard.tsx'
import PageErrorBoundary from './PageErrorBoundary.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PageErrorBoundary>
      <NewBankDashboard />
    </PageErrorBoundary>
  </StrictMode>,
)
