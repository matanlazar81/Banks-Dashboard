import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './new-dashboard.css'
import NewBankDashboard from './NewBankDashboard.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <NewBankDashboard />
  </StrictMode>,
)
