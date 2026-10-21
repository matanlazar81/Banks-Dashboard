import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './fx-challenge.css'
import FxChallenge from './FxChallenge.tsx'
import PageErrorBoundary from '../new-dashboard/PageErrorBoundary.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PageErrorBoundary>
      <FxChallenge />
    </PageErrorBoundary>
  </StrictMode>,
)
