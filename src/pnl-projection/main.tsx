import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './pnl-projection.css'
import PnlProjection from './PnlProjection.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PnlProjection />
  </StrictMode>,
)
