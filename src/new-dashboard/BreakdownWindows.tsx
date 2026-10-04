// The breakdown window of a table cell plus, on demand, a second window with one of its accounts by
// department. Shared by the New Bank Dashboard and the P&L Projection. Both windows move on their own
// and keep their place; the one clicked last is in front; Esc closes the department window first.
import { useCallback, useEffect, useState } from 'react';
import BreakdownPanel from './BreakdownPanel.tsx';
import { besidePosition, CASH_BREAKDOWN_ENDPOINT, clampPosition, type BreakdownRow, type PanelPosition } from './breakdown.ts';
import type { Ccy, VariantKey } from './types.ts';

interface Props {
  /** The open cell, or null when no window is open. */
  cell: { line: string; period: string } | null;
  variant: VariantKey;
  ccy: Ccy;
  endpoint?: string;
  onClose: () => void;
}

export default function BreakdownWindows({ cell, variant, ccy, endpoint = CASH_BREAKDOWN_ENDPOINT, onClose }: Props) {
  const [mainPos, setMainPos] = useState<PanelPosition>(() => clampPosition({ x: window.innerWidth - 580, y: 120 }));
  const [drillPos, setDrillPos] = useState<PanelPosition | null>(null);
  const [drill, setDrill] = useState<{ cellKey: string; row: string } | null>(null);
  const [front, setFront] = useState<'main' | 'drill'>('main');
  const cellKey = cell ? `${cell.line}|${cell.period}` : '';

  // The department window belongs to the cell it was opened from.
  const openRow = drill && drill.cellKey === cellKey ? drill.row : null;
  const closeDrill = useCallback(() => setDrill(null), []);
  const closeAll = useCallback(() => { setDrill(null); onClose(); }, [onClose]);
  const onDrill = useCallback((row: BreakdownRow) => {
    setDrill({ cellKey, row: row.key });
    setDrillPos((p) => p ?? besidePosition(mainPos));
    setFront('drill');
  }, [cellKey, mainPos]);

  useEffect(() => {
    if (!cell) return undefined;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      if (openRow) closeDrill(); else closeAll();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [cell, openRow, closeDrill, closeAll]);

  if (!cell) return null;
  return (
    <>
      <BreakdownPanel
        request={{ line: cell.line, period: cell.period, variant, ccy }}
        position={mainPos}
        onMove={setMainPos}
        onClose={closeAll}
        endpoint={endpoint}
        onDrill={onDrill}
        activeRow={openRow}
        escToClose={false}
        zIndex={front === 'main' ? 51 : 50}
        onFocus={() => setFront('main')}
      />
      {openRow && drillPos && (
        <BreakdownPanel
          request={{ line: cell.line, period: cell.period, variant, ccy, row: openRow }}
          position={drillPos}
          onMove={setDrillPos}
          onClose={closeDrill}
          endpoint={endpoint}
          escToClose={false}
          zIndex={front === 'drill' ? 51 : 50}
          onFocus={() => setFront('drill')}
        />
      )}
    </>
  );
}
