// The NRR breakdown as a workbook for review (the Excel button of the NRR window): every customer, and the
// calculation as live formulas over them, so a reader can check each step. Pure, no spreadsheet library:
// exportNrr.ts writes it, scripts/test-metrics.cjs checks it.
import { monthName } from './model.ts';
import type { Explain, NrrCustomer } from './types.ts';

/** A cell with a format; a formula (without '=') carries its value as computed here, shown until Excel recalculates. */
export interface WbCell {
  v: string | number;
  f?: string;
  z?: string;
  bold?: boolean;
  size?: number;
  /** Right-aligned: a header over figures. */
  right?: boolean;
}
export type WbValue = WbCell | string | number | null;
export interface WbSheet {
  name: string;
  rows: WbValue[][];
  /** Column widths, in characters. */
  cols: number[];
  /** The filter range of a table whose header is the first row ('A1:H120'). */
  autofilter?: string;
  freezeRows?: number;
}

export const EUR_FMT = '€#,##0';
export const PCT_FMT = '0.00%';

export const GROUP_LABEL: Record<NrrCustomer['group'], string> = {
  churned: 'Churned', contracted: 'Contracted', expanded: 'Expanded', unchanged: 'Unchanged', new: 'New this year',
};

const round2 = (n: number) => Math.round(n * 100) / 100;
const sum = (xs: number[]) => round2(xs.reduce((s, x) => s + x, 0));
const eur = (v: number, f?: string, bold = false): WbCell => ({ v, ...(f ? { f } : {}), z: EUR_FMT, ...(bold ? { bold } : {}) });
const pct = (v: number, f?: string, bold = false): WbCell => ({ v, ...(f ? { f } : {}), z: PCT_FMT, ...(bold ? { bold } : {}) });
const bold = (v: string, size?: number): WbCell => ({ v, bold: true, ...(size ? { size } : {}) });
const head = (v: string): WbCell => ({ v, bold: true, right: true });

/** The workbook of an NRR breakdown read with all=true (Summary, Customers, Month by month) and its file name. */
export function nrrWorkbook(d: Explain, generatedAt: Date): { sheets: WbSheet[]; fileName: string } {
  if (!d.period || !d.customers) throw new Error('The breakdown has no customer list: Pull & Build and restart the server.');
  const P = monthName(d.period.from);
  const N = monthName(d.period.to);
  const span = d.period.to.slice(5) === '01' ? N : `Jan–${N}`;
  const list = d.customers;
  const counted = list.filter((c) => c.group !== 'new');
  const of = (g: NrrCustomer['group']) => list.filter((c) => c.group === g);

  // Customers: one row each (header on row 1), the change and the GRR amount as formulas, totals below.
  const last = list.length + 1;
  const customers: WbValue[][] = [
    [bold('Customer'), bold('Group'), head(`${P} MRR`), head(`${N} MRR`), head('Change'), bold('Counted in NRR'), head('Kept for GRR'),
      bold('Currency'), bold('CRM churn date'), bold('Customer ID')],
    ...list.map((c, i) => {
      const r = i + 2;
      const inNrr = c.group !== 'new';
      return [
        c.customer, GROUP_LABEL[c.group], eur(c.then), eur(c.now), eur(round2(c.now - c.then), `D${r}-C${r}`),
        inNrr ? 'Yes' : 'No', eur(inNrr ? Math.min(c.then, c.now) : 0, `IF(F${r}="Yes",MIN(C${r},D${r}),0)`),
        c.currency, c.churnDate, c.id,
      ];
    }),
    [bold('Total'), null,
      eur(sum(list.map((c) => c.then)), `SUM(C2:C${last})`, true), eur(sum(list.map((c) => c.now)), `SUM(D2:D${last})`, true),
      eur(sum(list.map((c) => c.now - c.then)), `SUM(E2:E${last})`, true), null,
      eur(sum(counted.map((c) => Math.min(c.then, c.now))), `SUM(G2:G${last})`, true)],
  ];

  // Summary: the definitions, then the bridge as formulas over the Customers sheet.
  const range = (c: string) => `Customers!$${c}$2:$${c}$${last}`;
  const sumIf = (sumCol: string, critCol: string, crit: string) => `SUMIFS(${range(sumCol)},${range(critCol)},"${crit}")`;
  const countIf = (critCol: string, crit: string) => `COUNTIFS(${range(critCol)},"${crit}")`;
  const base = sum(counted.map((c) => c.then));
  const now = sum(counted.map((c) => c.now));
  const keptForGrr = sum(counted.map((c) => Math.min(c.then, c.now)));
  const foreign = list.filter((c) => c.currency !== 'EUR');
  const generated = generatedAt.toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });

  const summary: WbValue[][] = [
    [bold(`LSports NRR, ${span}: counted from 1 January on contracted MRR`, 14)],
    [`From the Metrics page (Business Tools → Metrics), generated ${generated}. MRR as in Snowflake, in each contract's currency.`],
    [],
    [bold('How it is calculated')],
    [`NRR = the ${N} MRR of the customers who had MRR in ${P} (going into 1 January) ÷ their ${P} MRR.`],
    [`GRR = the same with each customer capped at its ${P} MRR, so growth doesn't count: only what was kept.`],
    [`Customers with no MRR in ${P} are new this year: listed at the end of the Customers sheet, not counted.`],
    [`No forecast: only the contracts as they stood. Unchanged: within €0.50 of its ${P} MRR; MRR below zero counts as zero.`],
    ['Source: Snowflake FCT_OPPORTUNITY__MONTHLY__FINANCE, contracted MRR by opportunity and month (the source of ARR), by customer; test customers excluded; churn dates from DIM_CUSTOMER__FINANCE.'],
    ...(foreign.length ? [[`Customers with contracts in ${[...new Set(foreign.map((c) => c.currency))].join(', ')}: ${foreign.length}. Their MRR is added as it is, not converted to €.`]] : []),
    [],
    [bold('The bridge'), head('€'), head('Customers')],
  ];
  const baseRow = summary.length + 1;
  summary.push([`Their MRR, ${P}`, eur(base, sumIf('C', 'F', 'Yes')), { v: counted.length, f: countIf('F', 'Yes') }]);
  const step = (label: string, g: NrrCustomer['group']) => [
    label, eur(sum(of(g).map((c) => c.now - c.then)), sumIf('E', 'B', GROUP_LABEL[g])), { v: of(g).length, f: countIf('B', GROUP_LABEL[g]) },
  ];
  summary.push(step('+ Expansion', 'expanded'));
  summary.push(step('− Contraction', 'contracted'));
  summary.push(step(`− Churned (no MRR in ${N})`, 'churned'));
  summary.push(step('± Unchanged', 'unchanged'));
  const nowRow = summary.length + 1;
  // No label starts with '=': a spreadsheet could take it for a formula.
  summary.push([bold(`Their MRR, ${N}`), eur(now, sumIf('D', 'F', 'Yes'), true)]);
  summary.push([]);
  summary.push([bold(`NRR = ${N} ÷ ${P}`), pct(base > 0 ? now / base : 0, `B${nowRow}/B${baseRow}`, true)]);
  summary.push([`GRR (each capped at its ${P} MRR)`, pct(base > 0 ? keptForGrr / base : 0, `${sumIf('G', 'F', 'Yes')}/B${baseRow}`)]);
  summary.push(['NRR shown on the Metrics page', pct(d.value.value / 100)]);
  summary.push([]);
  summary.push([`Not counted: new this year, MRR ${N}`, eur(sum(of('new').map((c) => c.now)), sumIf('D', 'B', GROUP_LABEL.new)), { v: of('new').length, f: countIf('B', GROUP_LABEL.new) }]);
  summary.push(['Churned customers with a churn date in the CRM', null,
    { v: of('churned').filter((c) => c.churnDate).length, f: `COUNTIFS(${range('B')},"${GROUP_LABEL.churned}",${range('I')},"<>")` }]);
  summary.push([]);
  summary.push([bold('Worth checking')]);
  summary.push(['A churned customer that still buys under another customer ID (a renamed or merged account) shows as churned, and again as new this year.']);
  summary.push(['A churned customer with no churn date in the CRM: its contracts ended without being marked churned, or its MRR moved to another opportunity or customer.']);

  const months: WbValue[][] = [
    [bold(`NRR month by month, ${d.period.to.slice(0, 4)}`, 14)],
    [`Each month: the MRR that month of the customers who had MRR in ${P}, ÷ their ${P} MRR.`],
    [],
    [bold('Month'), head('NRR'), head('GRR')],
    ...(d.series || []).map((s) => [monthName(s.month), pct(s.nrr / 100), pct(s.grr / 100)]),
  ];

  return {
    sheets: [
      { name: 'Summary', rows: summary, cols: [62, 16, 12] },
      { name: 'Customers', rows: customers, cols: [40, 14, 14, 14, 14, 15, 14, 10, 15, 20], autofilter: `A1:J${last}`, freezeRows: 1 },
      { name: 'Month by month', rows: months, cols: [16, 12, 12] },
    ],
    fileName: `lsports-nrr-${d.period.to}.xlsx`,
  };
}
