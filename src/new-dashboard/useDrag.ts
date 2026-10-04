// Moves a window by its title bar: spread the result on the bar. A press on the bar's buttons, links or
// fields is a click, not a drag; the window is kept on screen (clampPosition).
import { useRef, type PointerEvent as ReactPointerEvent } from 'react';
import { clampPosition, PANEL_W, type PanelPosition } from './breakdown.ts';

type BarEvent = ReactPointerEvent<HTMLDivElement>;

export function useDrag(position: PanelPosition, onMove: (p: PanelPosition) => void, width = PANEL_W) {
  const drag = useRef<{ dx: number; dy: number } | null>(null);
  const end = (e: BarEvent) => {
    drag.current = null;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId);
  };
  return {
    onPointerDown: (e: BarEvent) => {
      if ((e.target as HTMLElement).closest('button, a, input, select, textarea')) return;
      drag.current = { dx: e.clientX - position.x, dy: e.clientY - position.y };
      e.currentTarget.setPointerCapture(e.pointerId);
    },
    onPointerMove: (e: BarEvent) => {
      if (drag.current) onMove(clampPosition({ x: e.clientX - drag.current.dx, y: e.clientY - drag.current.dy }, undefined, width));
    },
    onPointerUp: end,
    onPointerCancel: end,
  };
}
