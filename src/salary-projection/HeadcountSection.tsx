// The headcount under the payroll table: HiBob's employees at month-end with hires and leavers, month by month,
// from their start and leave dates (future ones too); HiBob's open positions are shown, not counted. People, not
// money: the same columns and year view as the payroll table.
import ProjectionTable, { type TableMarkers } from '../new-dashboard/ProjectionTable.tsx';
import { monthLongLabel, type Column, type ProjectionTable as Table } from '../new-dashboard/model.ts';
import { isHeadcountBreakdownLine } from './breakdown.ts';
import type { Ccy, SalaryPayload } from './types.ts';

const MARKERS: TableMarkers = { anchorLine: null, rollForwardLine: '', closingLine: '', negativeLines: new Set() };

interface Props {
  table: Table<string>;
  headcount: NonNullable<SalaryPayload['headcount']>;
  computedMonth: string;
  ccy: Ccy;
  /** Opens a cell's breakdown (the page's window); activeCell is the one open. */
  onOpenCell: (line: string, col: Column) => void;
  activeCell: string | null;
}

export default function HeadcountSection({ table, headcount, computedMonth, ccy, onOpenCell, activeCell }: Props) {
  const breakdownLines = new Set(table.lines.map((l) => l.key).filter(isHeadcountBreakdownLine));
  const asOf = new Date(`${headcount.asOf}T12:00:00`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  const current = monthLongLabel(computedMonth);
  return (
    <section className="rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-baseline gap-2 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">Headcount by month</h2>
        <span className="text-xs text-slate-500">
          HiBob ({headcount.company}){headcount.today !== null ? `: ${headcount.today.toLocaleString('en-US')} employees on ${asOf}` : ''}.
          {' '}HiBob's employees by their start and leave dates, signed hires and people leaving included. From {current}, HiBob's open positions are shown below the count, not in it. Hover a line for how it is counted; click a figure for it by department.
        </span>
      </div>
      <ProjectionTable
        table={table}
        ccy={ccy}
        anchorDate={null}
        markers={MARKERS}
        unitLabel="People"
        onOpenCell={onOpenCell}
        activeCell={activeCell}
        breakdownLines={breakdownLines}
      />
    </section>
  );
}
