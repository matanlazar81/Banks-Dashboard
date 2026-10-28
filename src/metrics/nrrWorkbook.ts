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
  if (!d.period || !d.period.months || !d.customers) throw new Error('The breakdown has no customer list: Pull & Build and restart the server.');
  const P = monthName(d.period.from);
  const N = monthName(d.period.to);
  const span = d.period.to.slice(5) === '01' ? N : `Jan–${N}`;
  const months = d.period.months;
  const k = months.length;
  const atDec = k > 1 ? `${k} × ${P}` : P;
  const list = d.customers;
  const counted = list.filter((c) => c.group !== 'new');
  const of = (g: NrrCustomer['group']) => list.filter((c) => c.group === g);

  // Customers: one row each (header on row 1), a column per month, the sums, the change and the GRR amount as
  // formulas, totals below. Columns: A customer, B group, C December, then the months, then the rest.
  const col = (i: number) => String.fromCharCode(65 + i);
  const M0 = 3;
  const [YTD, AT, CHG, CNT, KEPT, UNPAID, CHURN, ID] = Array.from({ length: 8 }, (_, i) => col(M0 + k + i));
  const MONTH = (i: number) => col(M0 + i);
  const LAST = MONTH(k - 1);
  const ytdOf = (c: NrrCustomer) => sum(c.months);
  const keptOf = (c: NrrCustomer) => (c.group === 'new' ? 0 : sum(c.months.map((v) => Math.min(v, c.dec))));
  const last = list.length + 1;
  const customers: WbValue[][] = [
    [bold('Customer'), bold('Group'), head(P), ...months.map((m) => head(monthName(m))), head(span), head(atDec), head('Change'),
      bold('Counted in NRR'), head('Kept for GRR'), head(`${span} still to collect`), bold('CRM churn date'), bold('Customer ID')],
    ...list.map((c, i) => {
      const r = i + 2;
      const inNrr = c.group !== 'new';
      return [
        c.customer, GROUP_LABEL[c.group], eur(c.dec), ...c.months.map((v) => eur(v)),
        eur(ytdOf(c), `SUM(${MONTH(0)}${r}:${LAST}${r})`), eur(round2(c.dec * k), `C${r}*${k}`), eur(round2(ytdOf(c) - c.dec * k), `${YTD}${r}-${AT}${r}`),
        inNrr ? 'Yes' : 'No',
        eur(keptOf(c), `IF(${CNT}${r}="Yes",${months.map((_, j) => `MIN(${MONTH(j)}${r},C${r})`).join('+')},0)`),
        eur(c.unpaid), c.churnDate, c.id,
      ];
    }),
    [bold('Total'), null, eur(sum(list.map((c) => c.dec)), `SUM(C2:C${last})`, true),
      ...months.map((_, j) => eur(sum(list.map((c) => c.months[j])), `SUM(${MONTH(j)}2:${MONTH(j)}${last})`, true)),
      eur(sum(list.map(ytdOf)), `SUM(${YTD}2:${YTD}${last})`, true), eur(sum(list.map((c) => round2(c.dec * k))), `SUM(${AT}2:${AT}${last})`, true),
      eur(sum(list.map((c) => round2(ytdOf(c) - c.dec * k))), `SUM(${CHG}2:${CHG}${last})`, true), null,
      eur(sum(list.map(keptOf)), `SUM(${KEPT}2:${KEPT}${last})`, true), eur(sum(list.map((c) => c.unpaid)), `SUM(${UNPAID}2:${UNPAID}${last})`, true)],
  ];

  // Summary: the definitions, then the bridge as formulas over the Customers sheet.
  const range = (c: string) => `Customers!$${c}$2:$${c}$${last}`;
  const sumIf = (sumCol: string, critCol: string, crit: string) => `SUMIFS(${range(sumCol)},${range(critCol)},"${crit}")`;
  const countIf = (critCol: string, crit: string) => `COUNTIFS(${range(critCol)},"${crit}")`;
  const base = sum(counted.map((c) => round2(c.dec * k)));
  const ytd = sum(counted.map(ytdOf));
  const generated = generatedAt.toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });

  const summary: WbValue[][] = [
    [bold(`LSports NRR, ${span}: revenue from 1 January`, 14)],
    [`From the Metrics page (Business Tools → Metrics), generated ${generated}. Amounts in € as in Snowflake.`],
    [],
    [bold('How it is calculated')],
    [`NRR = the ${span} contracted revenue of the customers who had revenue in ${P} (going into 1 January) ÷ the same ${k === 1 ? 'month' : `${k} months`} at their ${P} level (${atDec}).`],
    [`Customers who left count for what they brought before leaving. GRR = the same with each customer's month capped at its ${P} revenue, so growth doesn't count.`],
    ['Contracted revenue is what each contract brings in the month in Salesforce\'s revenue schedule, paid or still to be collected. Only months that have happened count; nothing is forecast.'],
    [`Customers with no revenue in ${P} are new this year: listed at the end of the Customers sheet, not counted. At the year's end this is the year's revenue ÷ ARR on 1 January.`],
    [`${N} alone ÷ ${P} is the run-rate: where the ${P} customers stand in the last month. Unchanged: within €0.50 of ${atDec}; revenue below zero counts as zero.`],
    ['Source: Snowflake FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__FINANCE (the revenue schedule by opportunity, paid and unpaid), by customer; test customers excluded; churn dates from DIM_CUSTOMER__FINANCE.'],
    [],
    [bold('The bridge'), head('€'), head('Customers')],
  ];
  const baseRow = summary.length + 1;
  summary.push([`${k === 1 ? '1 month' : `${k} months`} at their ${P} level (${atDec})`, eur(base, sumIf(AT, CNT, 'Yes')), { v: counted.length, f: countIf(CNT, 'Yes') }]);
  const step = (label: string, g: NrrCustomer['group']) => [
    label, eur(sum(of(g).map((c) => round2(ytdOf(c) - c.dec * k))), sumIf(CHG, 'B', GROUP_LABEL[g])), { v: of(g).length, f: countIf('B', GROUP_LABEL[g]) },
  ];
  summary.push(step('+ Expansion', 'expanded'));
  summary.push(step('− Contraction', 'contracted'));
  summary.push(step(`− Churned (nothing in ${N}; what they brought before counts)`, 'churned'));
  summary.push(step('± Unchanged', 'unchanged'));
  const ytdRow = summary.length + 1;
  // No label starts with '=': a spreadsheet could take it for a formula.
  summary.push([bold(`Their revenue, ${span}`), eur(ytd, sumIf(YTD, CNT, 'Yes'), true)]);
  summary.push([`Of which still to be collected, ${span}`, eur(sum(counted.map((c) => c.unpaid)), sumIf(UNPAID, CNT, 'Yes'))]);
  summary.push([]);
  summary.push([bold(`NRR = ${span} ÷ ${atDec}`), pct(base > 0 ? ytd / base : 0, `B${ytdRow}/B${baseRow}`, true)]);
  summary.push([`GRR (each month capped at its ${P} revenue)`, pct(base > 0 ? sum(counted.map(keptOf)) / base : 0, `${sumIf(KEPT, CNT, 'Yes')}/B${baseRow}`)]);
  const decSum = sum(counted.map((c) => c.dec));
  summary.push([`${N} alone ÷ ${P} (the run-rate)`, pct(decSum > 0 ? sum(counted.map((c) => c.months[k - 1])) / decSum : 0, `${sumIf(LAST, CNT, 'Yes')}/${sumIf('C', CNT, 'Yes')}`)]);
  summary.push(['NRR shown on the Metrics page', pct(d.value.value / 100)]);
  summary.push([]);
  summary.push([`Not counted: new this year, revenue ${span}`, eur(sum(of('new').map(ytdOf)), sumIf(YTD, 'B', GROUP_LABEL.new)), { v: of('new').length, f: countIf('B', GROUP_LABEL.new) }]);
  summary.push(['Churned customers with a churn date in the CRM', null,
    { v: of('churned').filter((c) => c.churnDate).length, f: `COUNTIFS(${range('B')},"${GROUP_LABEL.churned}",${range(CHURN)},"<>")` }]);
  summary.push([]);
  summary.push([bold('Worth checking')]);
  summary.push(['A churned customer that still buys under another customer ID (a renamed or merged account) shows as churned, and again as new this year.']);
  summary.push(['A churned customer with no churn date in the CRM: its contracts ended without being marked churned, or its revenue moved to another opportunity or customer.']);

  const monthsSheet: WbValue[][] = [
    [bold(`NRR month by month, ${d.period.to.slice(0, 4)}`, 14)],
    [`To date: the ${P} customers' revenue from January through the month ÷ that many months at their ${P} level. The month alone: that month ÷ ${P} (the run-rate).`],
    [],
    [bold('Month'), head('NRR to date'), head('GRR to date'), head('NRR, the month alone'), head('GRR, the month alone')],
    ...(d.series || []).map((s) => [monthName(s.month), pct(s.nrr / 100), pct(s.grr / 100), pct(s.monthNrr / 100), pct(s.monthGrr / 100)]),
  ];

  return {
    sheets: [
      { name: 'Summary', rows: summary, cols: [70, 16, 12] },
      { name: 'Customers', rows: customers, cols: [40, 14, 12, ...months.map(() => 11), 14, 14, 13, 15, 14, 18, 15, 20], autofilter: `A1:${ID}${last}`, freezeRows: 1 },
      { name: 'Month by month', rows: monthsSheet, cols: [16, 14, 14, 20, 20] },
    ],
    fileName: `lsports-nrr-${d.period.to}.xlsx`,
  };
}
