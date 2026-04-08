declare const __GIT_HASH__: string;
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  Cell, Legend, ComposedChart, Line, Area, LabelList
} from 'recharts';
import {
  Landmark, RefreshCw, Loader2, AlertTriangle, TrendingUp, Building2, DollarSign,
  ChevronDown, ChevronRight, MessageCircle, Send, X, Sparkles, Paperclip
} from 'lucide-react';
import * as XLSX from 'xlsx-js-style';
import html2canvas from 'html2canvas';

// ── Simple Markdown → HTML renderer ──
function mdToHtml(md: string): string {
  let html = md
    // Escape HTML
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    // Tables: detect lines with | separators
    .replace(/((?:^|\n)\|.+\|(?:\n\|[-:| ]+\|)?(?:\n\|.+\|)*)/g, (_match) => {
      const lines = _match.trim().split('\n').filter(l => l.trim().startsWith('|'));
      if (lines.length < 2) return _match;
      const parseRow = (line: string) => line.split('|').slice(1, -1).map(c => c.trim());
      const headers = parseRow(lines[0]);
      const isSep = (l: string) => /^\|[\s\-:|]+\|$/.test(l.trim());
      const dataStart = isSep(lines[1]) ? 2 : 1;
      const rows = lines.slice(dataStart).map(parseRow);
      let t = '<table class="w-full text-xs my-2 border-collapse">';
      t += '<thead><tr>' + headers.map(h => `<th class="border border-gray-300 px-2 py-1 bg-gray-100 text-left font-semibold">${h}</th>`).join('') + '</tr></thead>';
      t += '<tbody>' + rows.map(r => '<tr>' + r.map(c => `<td class="border border-gray-200 px-2 py-1">${c}</td>`).join('') + '</tr>').join('') + '</tbody>';
      t += '</table>';
      return '\n' + t + '\n';
    });
  // Process line by line for headings, lists, etc.
  html = html.split('\n').map(line => {
    if (line.startsWith('<t')) return line; // skip table HTML
    // Headings
    if (line.startsWith('### ')) return `<h4 class="font-bold text-sm mt-3 mb-1">${line.slice(4)}</h4>`;
    if (line.startsWith('## ')) return `<h3 class="font-bold text-sm mt-3 mb-1 text-emerald-700">${line.slice(3)}</h3>`;
    if (line.startsWith('# ')) return `<h2 class="font-bold text-base mt-3 mb-1">${line.slice(2)}</h2>`;
    // Bullet lists
    if (/^[-*] /.test(line)) return `<li class="ml-3 list-disc">${line.slice(2)}</li>`;
    if (/^\d+\. /.test(line)) return `<li class="ml-3 list-decimal">${line.replace(/^\d+\. /, '')}</li>`;
    // Horizontal rule
    if (/^---+$/.test(line.trim())) return '<hr class="my-2 border-gray-300"/>';
    if (line.trim() === '') return '<br/>';
    return `<p class="my-0.5">${line}</p>`;
  }).join('\n');
  // Inline formatting
  html = html
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`(.+?)`/g, '<code class="bg-gray-200 px-1 rounded text-[10px]">$1</code>')
    // Warning emoji emphasis
    .replace(/⚠️/g, '<span class="text-amber-600">⚠️</span>');
  return html;
}

// ── Scenario Management Types ──
type ScenarioData = {
  salaryAdjPctByMonth: Record<number, number>;
  collPctByMonth: Record<number, number>;
  salaryDeptAdj: Record<string, Record<string, number>>;
  vendorCatAdj: Record<string, Record<string, number>>; // { "2026-05": { "Software": -20, "Consulting": -30 } }
  vendorDetailAdj: Record<string, Record<string, { pct: number; base: number }>>; // { "2026-05": { "Outsourcing||Playmakers||660004": { pct: -10, base: 227385 } } }
  leverOverrides: Record<string, Record<number, number>>;
  pipelineMinProb: number;
};
type Scenario = {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  data: ScenarioData;
  ownerEmail?: string;
  ownerName?: string;
  isShared?: boolean;
  company?: string;
};

type BankData = {
  openingBalance: number;
  dailyBalances: { date: string; balance: number; movement: number; adjustedBalance?: number }[];
  currentBalance: number;
  primary?: { openingBalance: number; dailyBalances: any[]; currentBalance: number; adjustedCurrentBalance?: number; currency: string; label: string };
  local?: { openingBalance: number; dailyBalances: any[]; currentBalance: number; adjustedCurrentBalance?: number; currency: string; label: string };
  revaluation?: { lastRevalDate: string; lastRevalImpact: number; unrevalSince: string; estimatedMissing: number };
  revaluationLocal?: { lastRevalDate: string; lastRevalImpact: number; unrevalSince: string; estimatedMissing: number };
};
type BankAccount = { id: number; name: string; number: string; primaryBalance: number; localBalance: number };
type VendorBill = { amountEUR: number; dueDate: string; tranDate: string; vendor: string };
type ARForecastItem = { customer: string; amountEUR: number; dueDate: string };
type SalaryMonth = { month: string; amountEUR: number; amountILS: number };
type VendorHistoryRecord = { vendor: string; paidDate: string; amountEUR: number; daysToPay: number };

const fmt = (n: number) => new Intl.NumberFormat('en-IE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(n);
const fmtFull = (n: number) => new Intl.NumberFormat('en-IE', { style: 'currency', currency: 'EUR', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(n);
const fmtILS = (n: number) => new Intl.NumberFormat('he-IL', { style: 'currency', currency: 'ILS', maximumFractionDigits: 0 }).format(n);

function ReconTable({ sfRevenueTotal, totalInflows, totalCollectionAdj, totalCarry, totalPipeline, winRate, reconMonths, fmt: fmtFn, nsAccountId, hasSF = true }: {
  sfRevenueTotal: number; totalInflows: number; totalCollectionAdj: number; totalCarry: number; totalPipeline: number; winRate: number; hasSF?: boolean;
  reconMonths: { month: string; mKey: string; isPast: boolean; isCurrent: boolean; revenue: number; collected: number; inflows: number; collAdj: number; carry: number; pipeline: number; paid: number; unpaid: number; actualColl: number; collPct: number; collPctAdj: number; remaining: number }[];
  fmt: (n: number) => string;
  nsAccountId: string;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const [clientDrill, setClientDrill] = useState<{ mKey: string; clients: any[] | 'loading' } | null>(null);
  const toggle = (key: string) => { setExpanded(prev => prev === key ? null : key); setClientDrill(null); };
  const loadClients = (mKey: string) => {
    if (clientDrill?.mKey === mKey && clientDrill.clients !== 'loading') { setClientDrill(null); return; }
    setClientDrill({ mKey, clients: 'loading' });
    fetch(`/api/sf-revenue-breakdown?month=${mKey}`).then(r => r.json()).then(j => {
      setClientDrill({ mKey, clients: (j.data || []).sort((a: any, b: any) => (b.revenue || 0) - (a.revenue || 0)) });
    }).catch(() => setClientDrill({ mKey, clients: [] }));
  };
  const renderSimpleBreakdown = (key: string, getValue: (m: typeof reconMonths[0]) => number, color: string) => (
    expanded === key && (
      <tr><td colSpan={2} className="p-0">
        <div className="bg-gray-50 px-4 py-2 mb-1">
          <table className="w-full text-[11px]">
            <thead><tr className="text-left text-[10px] text-gray-400 uppercase border-b border-gray-200">
              <th className="pb-1 pr-3">Month</th><th className="pb-1 text-right">Amount</th>
            </tr></thead>
            <tbody>
              {reconMonths.map((m, mi) => {
                const val = getValue(m);
                if (val === 0 && key !== 'revenue') return null;
                // For carry rows, find the prior month whose unpaid generated this carry
                const priorMonth = key === 'carry' && mi > 0 ? reconMonths[mi - 1] : null;
                const priorMKey = priorMonth?.mKey;
                const isCarryDrill = key === 'carry' && clientDrill?.mKey === `carry-${m.mKey}`;
                return (
                  <Fragment key={m.mKey}>
                  <tr className={`border-b border-gray-100 ${key === 'carry' && val !== 0 ? 'cursor-pointer hover:bg-blue-50/50' : ''}`}
                      onClick={() => {
                        if (key !== 'carry' || val === 0) return;
                        const drillKey = `carry-${m.mKey}`;
                        if (clientDrill?.mKey === drillKey && clientDrill.clients !== 'loading') { setClientDrill(null); return; }
                        // Load unpaid clients from the prior month (fetch only unpaid)
                        const fetchMonth = priorMKey || m.mKey;
                        setClientDrill({ mKey: drillKey, clients: 'loading' });
                        fetch(`/api/sf-revenue-breakdown?month=${fetchMonth}&unpaidOnly=1`).then(r => r.json()).then(j => {
                          const clients = (j.data || []).filter((c: any) => (c.unpaid || 0) > 0).sort((a: any, b: any) => (b.unpaid || 0) - (a.unpaid || 0));
                          setClientDrill({ mKey: drillKey, clients });
                        }).catch(() => setClientDrill({ mKey: drillKey, clients: [] }));
                      }}>
                    <td className="py-1 pr-3 text-gray-600">
                      {key === 'carry' && val !== 0 && <span className="text-gray-400 mr-1 text-[9px]">{isCarryDrill ? '▼' : '▶'}</span>}
                      {m.month}
                      {m.isPast ? <span className="ml-1.5 text-[9px] bg-green-100 text-green-700 px-1 py-0.5 rounded-full">ACTUAL</span>
                        : m.isCurrent ? <span className="ml-1.5 text-[9px] bg-amber-100 text-amber-700 px-1 py-0.5 rounded-full">CURRENT</span>
                        : <span className="ml-1.5 text-[9px] bg-violet-100 text-violet-700 px-1 py-0.5 rounded-full">PROJECTED</span>}
                      {key === 'carry' && priorMonth && val !== 0 && <span className="text-[9px] text-gray-400 ml-1">(from {priorMonth.month} unpaid)</span>}
                    </td>
                    <td className={`py-1 text-right font-medium ${color}`}>{val >= 0 && key !== 'revenue' ? '+' : ''}{fmtFn(val)}</td>
                  </tr>
                  {/* Client drilldown for carry */}
                  {isCarryDrill && clientDrill && (() => {
                    if (clientDrill.clients === 'loading') return (
                      <tr><td colSpan={2} className="p-0">
                        <div className="bg-white border-l-2 border-amber-300 ml-4 px-3 py-2 mb-1">
                          <p className="text-[10px] text-gray-400 italic">Loading unpaid clients...</p>
                        </div>
                      </td></tr>
                    );
                    const clients = clientDrill.clients as any[];
                    const srcMonth = priorMonth || m;
                    if (clients.length === 0) return (
                      <tr><td colSpan={2} className="p-0">
                        <div className="bg-white border-l-2 border-amber-300 ml-4 px-3 py-2 mb-1">
                          <p className="text-[10px] text-gray-400 italic">No per-client unpaid data available for {srcMonth.month}</p>
                          <p className="text-[9px] text-gray-400 mt-0.5">Aggregate: {fmtFn(srcMonth.revenue)} revenue, {fmtFn(srcMonth.paid)} paid, {fmtFn(srcMonth.unpaid)} unpaid ({srcMonth.revenue > 0 ? (srcMonth.paid / srcMonth.revenue * 100).toFixed(1) : 0}% collection rate)</p>
                        </div>
                      </td></tr>
                    );
                    return (
                      <tr><td colSpan={2} className="p-0">
                        <div className="bg-white border-l-2 border-amber-300 ml-4 px-3 py-2 mb-1">
                          <p className="text-[9px] text-gray-400 uppercase font-medium mb-1">Unpaid clients from {srcMonth.month}</p>
                          <table className="w-full text-[10px]">
                            <thead><tr className="text-left text-[9px] text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Client</th>
                              <th className="pb-1 pr-2 text-right">Revenue</th>
                              <th className="pb-1 pr-2 text-right">Paid</th>
                              <th className="pb-1 pr-2 text-right">Unpaid</th>
                              <th className="pb-1 text-center">NS</th>
                            </tr></thead>
                            <tbody>
                              {clients.map((c: any, ci: number) => {
                                const cName = c.customer || c.name || '';
                                // Extract clean search term: take first meaningful word(s) before " -", "(", "I Rev"
                                const searchTerm = cName.split(/\s*[-–(]/)[0].trim().split(/\s+/).slice(0, 2).join(' ');
                                const nsSearchUrl = nsAccountId && searchTerm ? `https://${nsAccountId}.app.netsuite.com/app/common/search/ubersearchresults.nl?quicksearch=T&searchtype=Uber&frame=be&Uber_NAMEtype=KEYWORDSTARTSWITH&Uber_NAME=${encodeURIComponent(searchTerm)}` : '';
                                return (
                                <tr key={ci} className="border-b border-gray-50">
                                  <td className="py-0.5 pr-2 text-gray-700 truncate max-w-[200px]">{cName || '-'}</td>
                                  <td className="py-0.5 pr-2 text-right text-blue-600">{fmtFn(c.revenue || 0)}</td>
                                  <td className="py-0.5 pr-2 text-right text-green-600">{fmtFn(c.paid || 0)}</td>
                                  <td className="py-0.5 pr-2 text-right text-orange-500 font-medium">{fmtFn(c.unpaid || 0)}</td>
                                  <td className="py-0.5 text-center">{nsSearchUrl ? <a href={nsSearchUrl} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 font-bold" onClick={(e) => e.stopPropagation()}>NS</a> : null}</td>
                                </tr>
                                );
                              })}
                                                          </tbody>
                            <tfoot><tr className="border-t font-bold text-[10px]">
                              <td className="py-1">Total ({clients.length} clients)</td>
                              <td className="py-1 pr-2 text-right text-blue-700">{fmtFn(clients.reduce((s: number, c: any) => s + (c.revenue || 0), 0))}</td>
                              <td className="py-1 pr-2 text-right text-green-700">{fmtFn(clients.reduce((s: number, c: any) => s + (c.paid || 0), 0))}</td>
                              <td className="py-1 pr-2 text-right text-orange-600">{fmtFn(clients.reduce((s: number, c: any) => s + (c.unpaid || 0), 0))}</td>
                              <td></td>
                            </tr></tfoot>
                          </table>
                        </div>
                      </td></tr>
                    );
                  })()}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </td></tr>
    )
  );
  // Client drilldown helper for revenue breakdown
  const renderClientDrill = (colSpanVal: number) => {
    if (!clientDrill || clientDrill.clients === 'loading') return (
      <tr><td colSpan={colSpanVal} className="p-0">
        <div className="bg-white border-l-2 border-blue-300 ml-4 px-3 py-2 mb-1">
          <p className="text-[10px] text-gray-400 italic">Loading clients...</p>
        </div>
      </td></tr>
    );
    const clients = clientDrill.clients as any[];
    if (clients.length === 0) return (
      <tr><td colSpan={colSpanVal} className="p-0">
        <div className="bg-white border-l-2 border-blue-300 ml-4 px-3 py-2 mb-1">
          <p className="text-[10px] text-gray-400 italic">No client data for this month</p>
        </div>
      </td></tr>
    );
    return (
      <tr><td colSpan={colSpanVal} className="p-0">
        <div className="bg-white border-l-2 border-blue-300 ml-4 px-3 py-2 mb-1">
          <table className="w-full text-[10px]">
            <thead><tr className="text-left text-[9px] text-gray-400 uppercase border-b">
              <th className="pb-1 pr-2">Client</th>
              <th className="pb-1 pr-2 text-right">Revenue</th>
              <th className="pb-1 pr-2 text-right">Paid</th>
              <th className="pb-1 text-right">Unpaid</th>
            </tr></thead>
            <tbody>
              {clients.slice(0, 25).map((c: any, ci: number) => (
                <tr key={ci} className="border-b border-gray-50">
                  <td className="py-0.5 pr-2 text-gray-700 truncate max-w-[200px]">{c.customer || c.name || '-'}</td>
                  <td className="py-0.5 pr-2 text-right text-blue-600">{fmtFn(c.revenue || 0)}</td>
                  <td className="py-0.5 pr-2 text-right text-green-600">{fmtFn(c.paid || 0)}</td>
                  <td className={`py-0.5 text-right ${(c.unpaid || 0) > 0 ? 'text-red-500 font-medium' : 'text-gray-400'}`}>{fmtFn(c.unpaid || 0)}</td>
                </tr>
              ))}
              {clients.length > 25 && <tr><td colSpan={4} className="py-0.5 text-gray-400 italic">+ {clients.length - 25} more</td></tr>}
            </tbody>
            <tfoot><tr className="border-t font-bold text-[10px]">
              <td className="py-1">Total ({clients.length})</td>
              <td className="py-1 pr-2 text-right text-blue-700">{fmtFn(clients.reduce((s: number, c: any) => s + (c.revenue || 0), 0))}</td>
              <td className="py-1 pr-2 text-right text-green-700">{fmtFn(clients.reduce((s: number, c: any) => s + (c.paid || 0), 0))}</td>
              <td className="py-1 text-right text-red-600">{fmtFn(clients.reduce((s: number, c: any) => s + (c.unpaid || 0), 0))}</td>
            </tr></tfoot>
          </table>
        </div>
      </td></tr>
    );
  };
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 className="text-sm font-medium text-gray-700 mb-3">Inflows Reconciliation</h3>
      <table className="max-w-lg text-[12px]">
        <tbody>
          <tr className="border-b border-gray-100 cursor-pointer hover:bg-blue-50/50" onClick={() => toggle('revenue')}>
            <td className="py-2 pr-8 text-blue-700 font-semibold">{expanded === 'revenue' ? '▼' : '▶'} {hasSF ? 'Snowflake' : 'NS Budget'} Revenue</td>
            <td className="py-2 text-right text-blue-700 font-bold">{fmtFn(sfRevenueTotal)}</td>
          </tr>
          {renderSimpleBreakdown('revenue', m => m.revenue, 'text-blue-700')}
          <tr className="border-b border-gray-100 cursor-pointer hover:bg-gray-50" onClick={() => toggle('collAdj')}>
            <td className="py-2 pr-8 text-gray-600">{expanded === 'collAdj' ? '▼' : '▶'} Cash collected vs Revenue <span className="text-gray-400 text-[10px]">(collected − {hasSF ? 'Snowflake' : 'NS Budget'} revenue)</span></td>
            <td className={`py-2 text-right font-medium ${totalCollectionAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{totalCollectionAdj >= 0 ? '+' : ''}{fmtFn(totalCollectionAdj)}</td>
          </tr>
          {expanded === 'collAdj' && (
            <tr><td colSpan={2} className="p-0">
              <div className="bg-gray-50 px-4 py-2 mb-1">
                <table className="w-full text-[10.5px]">
                  <thead><tr className="text-left text-[10px] text-gray-400 uppercase border-b border-gray-200">
                    <th className="pb-1 pr-3">Month</th>
                    <th className="pb-1 pr-2 text-right">Revenue</th>
                    <th className="pb-1 pr-2 text-right">Collected</th>
                    <th className="pb-1 text-right">Difference</th>
                  </tr></thead>
                  <tbody>
                    {reconMonths.map(m => {
                      if (m.collAdj === 0 && m.revenue === 0) return null;
                      const isOpen = expanded === `collAdj-${m.mKey}`;
                      const isClientOpen = clientDrill?.mKey === m.mKey;
                      return (
                        <Fragment key={m.mKey}>
                        <tr className="border-b border-gray-100 cursor-pointer hover:bg-blue-50/50" onClick={() => toggle(`collAdj-${m.mKey}`)}>
                          <td className="py-1 pr-3 text-gray-600">
                            <span className="text-gray-400 mr-1 text-[9px]">{isOpen ? '▼' : '▶'}</span>
                            {m.month}
                            {m.isPast ? <span className="ml-1.5 text-[9px] bg-green-100 text-green-700 px-1 py-0.5 rounded-full">ACTUAL</span>
                              : m.isCurrent ? <span className="ml-1.5 text-[9px] bg-amber-100 text-amber-700 px-1 py-0.5 rounded-full">CURRENT</span>
                              : <span className="ml-1.5 text-[9px] bg-violet-100 text-violet-700 px-1 py-0.5 rounded-full">PROJECTED</span>}
                          </td>
                          <td className="py-1 pr-2 text-right text-blue-600">{fmtFn(m.revenue)}</td>
                          <td className="py-1 pr-2 text-right text-green-600">{fmtFn(m.collected)}</td>
                          <td className={`py-1 text-right font-medium ${m.collAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{m.collAdj >= 0 ? '+' : ''}{fmtFn(m.collAdj)}</td>
                        </tr>
                        {/* Difference breakdown */}
                        {isOpen && (
                          <tr><td colSpan={4} className="p-0">
                            <div className="bg-blue-50/40 border-l-2 border-blue-300 ml-4 px-3 py-2 mb-1">
                              <p className="text-[10px] text-gray-500 font-medium mb-1.5 uppercase">Difference breakdown — {m.month}</p>
                              <table className="w-full text-[10px]">
                                <tbody>
                                  <tr className="border-b border-blue-100">
                                    <td className="py-0.5 text-blue-600">{hasSF ? 'Snowflake' : 'NS Budget'} Revenue</td>
                                    <td className="py-0.5 text-right text-blue-600 font-medium">{fmtFn(m.revenue)}</td>
                                  </tr>
                                  {m.isPast ? (
                                    <>
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-green-700 pl-2">Actual cash collected (NetSuite)</td>
                                        <td className="py-0.5 text-right text-green-700 font-medium">{fmtFn(m.actualColl || m.collected)}</td>
                                      </tr>
                                      {m.paid > 0 && (
                                        <tr className="border-b border-blue-100">
                                          <td className="py-0.5 text-gray-500 pl-4 text-[9px]">of which: SF Paid Revenue</td>
                                          <td className="py-0.5 text-right text-gray-500 text-[9px]">{fmtFn(m.paid)}</td>
                                        </tr>
                                      )}
                                      {m.unpaid > 0 && (
                                        <tr className="border-b border-blue-100">
                                          <td className="py-0.5 text-gray-500 pl-4 text-[9px]">of which: SF Unpaid Revenue (→ carry to next month)</td>
                                          <td className="py-0.5 text-right text-orange-500 text-[9px]">{fmtFn(m.unpaid)}</td>
                                        </tr>
                                      )}
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-gray-500 pl-2">Timing / I/C / other <span className="text-[9px] text-gray-400">(NS cash − SF revenue)</span></td>
                                        <td className={`py-0.5 text-right font-medium ${m.collAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{m.collAdj >= 0 ? '+' : ''}{fmtFn(m.collAdj)}</td>
                                      </tr>
                                    </>
                                  ) : m.isCurrent ? (
                                    <>
                                      {m.actualColl > 0 && (
                                        <tr className="border-b border-blue-100">
                                          <td className="py-0.5 text-green-700 pl-2">Cash collected so far (NetSuite)</td>
                                          <td className="py-0.5 text-right text-green-700 font-medium">{fmtFn(m.actualColl)}</td>
                                        </tr>
                                      )}
                                      {m.remaining > 0 && (
                                        <tr className="border-b border-blue-100">
                                          <td className="py-0.5 text-violet-600 pl-2">Remaining projected <span className="text-[9px] text-gray-400">({m.collPct}% × forecast − collected)</span></td>
                                          <td className="py-0.5 text-right text-violet-600 font-medium">+{fmtFn(m.remaining)}</td>
                                        </tr>
                                      )}
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-gray-500 pl-2">Difference vs revenue</td>
                                        <td className={`py-0.5 text-right font-medium ${m.collAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{m.collAdj >= 0 ? '+' : ''}{fmtFn(m.collAdj)}</td>
                                      </tr>
                                    </>
                                  ) : (
                                    <>
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-violet-600 pl-2">Collection rate applied</td>
                                        <td className="py-0.5 text-right text-violet-600 font-medium">{m.collPct}%</td>
                                      </tr>
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-green-700 pl-2">Projected collection <span className="text-[9px] text-gray-400">(revenue × {m.collPct}%)</span></td>
                                        <td className="py-0.5 text-right text-green-700 font-medium">{fmtFn(Math.round(m.revenue * m.collPct / 100))}</td>
                                      </tr>
                                      {m.collPctAdj !== 0 && (
                                        <tr className="border-b border-blue-100">
                                          <td className="py-0.5 text-amber-600 pl-4 text-[9px]">Collection % adjustment ({m.collPct}% vs 100%)</td>
                                          <td className={`py-0.5 text-right text-[9px] ${m.collPctAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{m.collPctAdj >= 0 ? '+' : ''}{fmtFn(m.collPctAdj)}</td>
                                        </tr>
                                      )}
                                      <tr className="border-b border-blue-100">
                                        <td className="py-0.5 text-gray-500 pl-2">Difference vs revenue</td>
                                        <td className={`py-0.5 text-right font-medium ${m.collAdj >= 0 ? 'text-green-600' : 'text-red-500'}`}>{m.collAdj >= 0 ? '+' : ''}{fmtFn(m.collAdj)}</td>
                                      </tr>
                                    </>
                                  )}
                                </tbody>
                              </table>
                              {/* Client drilldown link */}
                              <button
                                onClick={e => { e.stopPropagation(); loadClients(m.mKey); }}
                                className={`mt-2 text-[10px] ${isClientOpen ? 'text-blue-700 font-medium' : 'text-blue-500 hover:text-blue-700'}`}
                              >{isClientOpen ? '▼' : '▶'} View client breakdown</button>
                            </div>
                          </td></tr>
                        )}
                        {isOpen && isClientOpen && renderClientDrill(4)}
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </td></tr>
          )}
          {totalCarry !== 0 && (
            <>
              <tr className="border-b border-gray-100 cursor-pointer hover:bg-amber-50/50" onClick={() => toggle('carry')}>
                <td className="py-2 pr-8 text-gray-600">{expanded === 'carry' ? '▼' : '▶'} Unpaid carry-forward <span className="text-gray-400 text-[10px]">(prior month receivables)</span></td>
                <td className="py-2 text-right font-medium text-amber-600">{totalCarry >= 0 ? '+' : ''}{fmtFn(totalCarry)}</td>
              </tr>
              {renderSimpleBreakdown('carry', m => m.carry, 'text-amber-600')}
            </>
          )}
          {totalPipeline !== 0 && (
            <>
              <tr className="border-b border-gray-100 cursor-pointer hover:bg-teal-50/50" onClick={() => toggle('pipeline')}>
                <td className="py-2 pr-8 text-gray-600">{expanded === 'pipeline' ? '▼' : '▶'} Pipeline <span className="text-gray-400 text-[10px]">({winRate}% win rate)</span></td>
                <td className="py-2 text-right font-medium text-teal-600">+{fmtFn(totalPipeline)}</td>
              </tr>
              {renderSimpleBreakdown('pipeline', m => m.pipeline, 'text-teal-600')}
            </>
          )}
          <tr className="border-t-2 border-gray-300 cursor-pointer hover:bg-green-50/50" onClick={() => toggle('total')}>
            <td className="py-2 pr-8 text-green-800 font-bold">{expanded === 'total' ? '▼' : '▶'} Cashflow Inflows</td>
            <td className="py-2 text-right text-green-800 font-bold text-sm">{fmtFn(totalInflows)}</td>
          </tr>
          {renderSimpleBreakdown('total', m => m.inflows, 'text-green-800')}
        </tbody>
      </table>
    </div>
  );
}

type CompanyView = 'lsports' | 'statscore' | 'consolidated';
const COMPANY_CONFIG: Record<string, { name: string; subsidiary: number; hasSF: boolean; hasDepts: boolean }> = {
  lsports: { name: 'LSports', subsidiary: 3, hasSF: true, hasDepts: true },
  statscore: { name: 'Statscore', subsidiary: 6, hasSF: false, hasDepts: false },
};

export default function App() {
  const [activeCompany, setActiveCompany] = useState<CompanyView>(() => {
    try { return (localStorage.getItem('banks-active-company') as CompanyView) || 'lsports'; } catch { return 'lsports'; }
  });
  useEffect(() => { try { localStorage.setItem('banks-active-company', activeCompany); } catch {} }, [activeCompany]);

  // ── Per-company year selector (for multi-year budget planning) ──
  const currentYear = new Date().getFullYear();
  const [activeYears, setActiveYears] = useState<Record<string, number>>(() => {
    try {
      const saved = localStorage.getItem('banks-active-years');
      if (saved) return JSON.parse(saved);
      // Migrate from old single-year format
      const old = parseInt(localStorage.getItem('banks-active-year') || '');
      if (old) return { lsports: old, statscore: old };
    } catch {}
    return { lsports: currentYear, statscore: currentYear };
  });
  const [availableYearsByCompany, setAvailableYearsByCompany] = useState<Record<string, number[]>>({ lsports: [currentYear], statscore: [currentYear] });
  const [isRollingForward, setIsRollingForward] = useState(false);
  useEffect(() => { try { localStorage.setItem('banks-active-years', JSON.stringify(activeYears)); } catch {} }, [activeYears]);
  useEffect(() => {
    fetch('/api/budget-years').then(r => r.json()).then(d => {
      if (d.byCompany) setAvailableYearsByCompany(d.byCompany);
    }).catch(() => {});
  }, []);

  // Derived: active year for current tab (keeps most downstream code unchanged)
  const activeYear = activeCompany === 'consolidated' ? Math.max(activeYears.lsports || currentYear, activeYears.statscore || currentYear) : (activeYears[activeCompany] || currentYear);
  const yearParam = `&year=${activeYear}`;
  const companyConfig = COMPANY_CONFIG[activeCompany] || COMPANY_CONFIG.lsports;
  const subsidiaryParam = activeCompany !== 'consolidated' ? `?subsidiary=${companyConfig.subsidiary}${yearParam}` : `?subsidiary=3${yearParam}`;

  // NS Budget (for subsidiaries without Snowflake — e.g., STATSCORE)
  const [nsBudget, setNsBudget] = useState<{ byMonth: Record<string, { salary: number; vendors: number; total: number; details: any[] }> }>({ byMonth: {} });

  const [bankData, setBankData] = useState<BankData>({ openingBalance: 0, dailyBalances: [], currentBalance: 0 });
  const [bankAccounts, setBankAccounts] = useState<BankAccount[]>([]);
  const [vendorBills, setVendorBills] = useState<VendorBill[]>([]);
  const [arForecast, setARForecast] = useState<ARForecastItem[]>([]);
  const [salaryData, setSalaryData] = useState<SalaryMonth[]>([]);
  const [vendorHistory, setVendorHistory] = useState<VendorHistoryRecord[]>([]);
  const [expenseCategories, setExpenseCategories] = useState<{ byMonth: Record<string, Record<string, number>>; categories: string[] }>({ byMonth: {}, categories: [] });
  const [actualCollections, setActualCollections] = useState<Record<string, number>>({});
  const [sfBudget, setSfBudget] = useState<{ byMonth?: Record<string, Record<string, number>>; totalByMonth: Record<string, { eur: number; ils: number }>; overrides?: { account: string; fromMonth: string; toMonth: string; department: string; location: string; amountEUR: number; mode: string; comments: string; mKey: string; category: string; oldVal: number; newVal: number }[] }>({ totalByMonth: {} });
  const [sfRevenue, setSfRevenue] = useState<{ budget: Record<string, { eur: number }>; actuals: Record<string, { eur: number }>; targets: Record<string, number> }>({ budget: {}, actuals: {}, targets: {} });
  const [sfActualsSplit, setSfActualsSplit] = useState<Record<string, { salary: number; salaryILS: number; vendors: number; vendorsILS: number }>>({});
  const [sfRevenuePaid, setSfRevenuePaid] = useState<Record<string, { revenue: number; paid: number; unpaid: number; customers: number }>>({});
  const [sfPipeline, setSfPipeline] = useState<{ name: string; stage: string; amount: number; probability: number; weighted: number; currency: string; closeDate: string; type: string; feedType: string; owner: string }[]>([]);
  const [pipelineMinProb, setPipelineMinProb] = useState(100); // min probability filter for pipeline
  const [sfConversion, setSfConversion] = useState<{ yearly: { year: number; won: number; lost: number; winRate: number; avgWonDays: number }[]; stages: any[]; customers: any[]; projection: any[] }>({ yearly: [], stages: [], customers: [], projection: [] });
  const [monthlyReval, setMonthlyReval] = useState<{ byMonth: Record<string, { eur: number; ils: number; hasBothEnds?: boolean }>; preYear: { eur: number; ils: number } }>({ byMonth: {}, preYear: { eur: 0, ils: 0 } });
  const [sfSalaryBudget, setSfSalaryBudget] = useState<Record<string, { eur: number; ils: number }>>({});
  const [sfSalaryOverrides, setSfSalaryOverrides] = useState<{ account: string; fromMonth: string; toMonth: string; department: string; location: string; amountEUR: number; mode: string; comments: string; mKey: string; oldVal: number; newVal: number }[]>([]);
  const [prevMonthEndBalance, setPrevMonthEndBalance] = useState<{ eur: number; ils: number } | null>(null);
  const [churnData, setChurnData] = useState<{ year: number; totalCustomers: number; totalRevenue: number; churnedClients: number; lostRevenue: number; churnPct: number; clientChurnPct: number; monthlyImpact: number; monthsCount: number }[]>([]);
  const [churnMonthlyAvg, setChurnMonthlyAvg] = useState(0); // 6-month rolling avg
  const [churnDrilldown, setChurnDrilldown] = useState<{ year: number; data: any[] | 'loading' } | null>(null);
  const [nsAccountId, setNsAccountId] = useState('');
  const [asOfDate, setAsOfDate] = useState<string>(''); // YYYY-MM-DD or empty for live
  const [yoyRevenue, setYoyRevenue] = useState<{ currentYear: number; priorYear: number; throughMonth: number; currentYearRev?: number; priorYearRev?: number; currentYearPaid?: number; priorYearPaid?: number; currentYearCustomers?: number; priorYearCustomers?: number } | null>(null);
  const [asOfDateRaw, setAsOfDateRaw] = useState(''); // raw text typed in date input
  // ── Per-company data cache (avoid re-fetching when switching tabs) ──
  const companyDataCache = useRef<Record<string, any>>({});
  // ── Live current-year cashflow per company (for propagating to next year) ──
  const sourceYearCashflowRef = useRef<Record<string, any[]>>({});

  // ── Consolidated view state ──
  const [consolidatedData, setConsolidatedData] = useState<any>(null);
  const [consLsScenarioId, setConsLsScenarioId] = useState<string | null>(null);
  const [consStScenarioId, setConsStScenarioId] = useState<string | null>(null);
  const [consElimExpanded, setConsElimExpanded] = useState(true);
  const [consElimDetailMonth, setConsElimDetailMonth] = useState<string | null>(null);
  const [consDrilldown, setConsDrilldown] = useState<{ type: string; title: string; rows: { label: string; ls: number; st: number; total: number; color?: string }[]; accounts?: { ls: { account: string; name: string; amount: number }[]; st: { account: string; name: string; amount: number }[] }; loading?: boolean } | null>(null);
  const [consBankExpanded, setConsBankExpanded] = useState<'ls' | 'st' | null>(null);
  const [expandedChart, setExpandedChart] = useState<string | null>(null);
  const [expandedKpi, setExpandedKpi] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [lastRefreshed, setLastRefreshed] = useState('');
  const [currency, setCurrency] = useState<'EUR' | 'ILS'>('EUR');
  const [salaryAdjPctByMonth, setSalaryAdjPctByMonth] = useState<Record<number, number>>(() => {
    // Year-specific: load adjustments for the initial active year
    try {
      const initYear = (() => { try { const y = localStorage.getItem('banks-active-years'); return y ? JSON.parse(y) : {}; } catch { return {}; } })();
      const yr = initYear.lsports || new Date().getFullYear();
      const yearSaved = localStorage.getItem(`banks-salary-adj-${yr}`);
      if (yearSaved) return JSON.parse(yearSaved);
      // Fall back to legacy non-year key for current year
      const apSaved = localStorage.getItem('ap-salary-adj');
      if (apSaved) return JSON.parse(apSaved);
      const saved = localStorage.getItem('banks-salary-adj');
      return saved ? JSON.parse(saved) : {};
    } catch { return {}; }
  });
  // Sync: on each render, check if AP tab has newer adjustments (current year only)
  useEffect(() => {
    const handler = () => { if (activeYear !== currentYear) return; try { const ap = localStorage.getItem('ap-salary-adj'); if (ap) setSalaryAdjPctByMonth(JSON.parse(ap)); } catch {} };
    window.addEventListener('focus', handler);
    return () => window.removeEventListener('focus', handler);
  }, [activeYear, currentYear]);
  useEffect(() => { try { localStorage.setItem(`banks-salary-adj-${activeYear}`, JSON.stringify(salaryAdjPctByMonth)); } catch {} }, [salaryAdjPctByMonth, activeYear]);
  // Collection % adjustment per month (default 100% — expected collection rate vs forecast)
  const [collPctByMonth, setCollPctByMonth] = useState<Record<number, number>>(() => {
    try {
      const initYear = (() => { try { const y = localStorage.getItem('banks-active-years'); return y ? JSON.parse(y) : {}; } catch { return {}; } })();
      const yr = initYear.lsports || new Date().getFullYear();
      const yearSaved = localStorage.getItem(`banks-coll-pct-${yr}`);
      if (yearSaved) return JSON.parse(yearSaved);
      const saved = localStorage.getItem('banks-coll-pct');
      return saved ? JSON.parse(saved) : {};
    } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem(`banks-coll-pct-${activeYear}`, JSON.stringify(collPctByMonth)); } catch {} }, [collPctByMonth, activeYear]);
  // Reset adjustments when switching years (each year has independent assumptions)
  useEffect(() => {
    try {
      const salKey = `banks-salary-adj-${activeYear}`;
      const colKey = `banks-coll-pct-${activeYear}`;
      const salSaved = localStorage.getItem(salKey);
      const colSaved = localStorage.getItem(colKey);
      setSalaryAdjPctByMonth(salSaved ? JSON.parse(salSaved) : {});
      setCollPctByMonth(colSaved ? JSON.parse(colSaved) : {});
    } catch {
      setSalaryAdjPctByMonth({});
      setCollPctByMonth({});
    }
  }, [activeYear]);
  const getCollPct = (i: number) => collPctByMonth[i] ?? 100; // default 100%
  // Per-department salary % overrides — key: mKey, value: { department: pct }
  const [salaryDeptAdj, setSalaryDeptAdj] = useState<Record<string, Record<string, number>>>(() => {
    try { const saved = localStorage.getItem('banks-salary-dept-adj'); return saved ? JSON.parse(saved) : {}; } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem('banks-salary-dept-adj', JSON.stringify(salaryDeptAdj)); } catch {} }, [salaryDeptAdj]);
  // Cache of salary budget per department per month — populated when drilldown opens
  const [salaryDeptBudgets, setSalaryDeptBudgets] = useState<Record<string, Record<string, number>>>(() => {
    try { const saved = localStorage.getItem('banks-salary-dept-budgets'); return saved ? JSON.parse(saved) : {}; } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem('banks-salary-dept-budgets', JSON.stringify(salaryDeptBudgets)); } catch {} }, [salaryDeptBudgets]);
  // Per-vendor-category adjustments — persisted across sessions
  const [vendorCatAdj, setVendorCatAdj] = useState<Record<string, Record<string, number>>>(() => {
    try { const saved = localStorage.getItem('banks-vendor-cat-adj'); return saved ? JSON.parse(saved) : {}; } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem('banks-vendor-cat-adj', JSON.stringify(vendorCatAdj)); } catch {} }, [vendorCatAdj]);
  // Per-vendor-department adjustments (detail level within category) — persisted
  const [vendorDetailAdj, setVendorDetailAdj] = useState<Record<string, Record<string, { pct: number; base: number }>>>(() => {
    try { const saved = localStorage.getItem('banks-vendor-detail-adj'); return saved ? JSON.parse(saved) : {}; } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem('banks-vendor-detail-adj', JSON.stringify(vendorDetailAdj)); } catch {} }, [vendorDetailAdj]);
  // Per-employee headcount lever overrides — persisted across sessions
  const [leverOverrides, setLeverOverrides] = useState<Record<string, Record<number, number>>>(() => {
    try { const saved = localStorage.getItem('banks-lever-overrides'); return saved ? JSON.parse(saved) : {}; } catch { return {}; }
  });
  useEffect(() => { try { localStorage.setItem('banks-lever-overrides', JSON.stringify(leverOverrides)); } catch {} }, [leverOverrides]);

  // ── Scenario Management ──
  const [scenarios, setScenarios] = useState<Scenario[]>([]);
  const [activeScenarioId, setActiveScenarioId] = useState<string | null>(() => {
    try { return localStorage.getItem('banks-active-scenario') || null; } catch { return null; }
  });
  const [scenarioMenuOpen, setScenarioMenuOpen] = useState(false);
  const [scenarioNameEdit, setScenarioNameEdit] = useState<string | null>(null);
  const [scenarioNewName, setScenarioNewName] = useState('');
  const [compareScenarioId, setCompareScenarioId] = useState<string | null>(null);
  const [showComparePanel, setShowComparePanel] = useState(false);
  const [compareLeftId, setCompareLeftId] = useState<string | null>(null);
  const [compareRightId, setCompareRightId] = useState<string | null>(null);
  const [hoveredScenarioId, setHoveredScenarioId] = useState<string | null>(null);
  const [expandedScenarioId, setExpandedScenarioId] = useState<string | null>(null);
  const scenarioMenuRef = useRef<HTMLDivElement>(null);
  const [_viewerEmail, _setViewerEmail] = useState<string|null>(null);
useEffect(() => {
  if (!_viewerEmail) return;
  try { localStorage.setItem('banks-scenarios:' + _viewerEmail.toLowerCase(), JSON.stringify(scenarios)); } catch {}
}, [scenarios, _viewerEmail]);
  const _srvRef = useRef(false);
  const [_shared, _setShared] = useState<Scenario[]>([]);
  const [_bdUsers, _setBdUsers] = useState<{email:string;displayName:string}[]>([]);
  const [_shareOpen, _setShareOpen] = useState<string|null>(null);
  const [_shareMap, _setShareMap] = useState<Record<string,{email:string;displayName:string}[]>>({});
  const [_sharePending, _setSharePending] = useState<Record<string, boolean>>({});
  const refetchScenarios = useCallback(() => {
    fetch('/api/scenarios', { credentials: 'include' }).then(r => {
      if (!r.ok) { console.error('[Scenarios] fetch failed:', r.status, r.statusText); return null; }
      return r.json();
    }).then(d => {
      if (!d || !Array.isArray(d.data)) return;
      if (d.error) { console.error('[Scenarios] server error:', d.error); return; }
      const ve = (d.viewerEmail as string) || '';
      if (ve) _setViewerEmail(ve);
      _srvRef.current = true;
      _setShared(Array.isArray(d.shared) ? d.shared : []);
      setScenarios(d.data);
      console.info('[Scenarios] loaded', d.data.length, 'own +', (d.shared || []).length, 'shared for', ve || '(no viewerEmail)');
    }).catch(e => { console.error('[Scenarios] network error:', e); });
  }, []);
  useEffect(() => {
    refetchScenarios();
    fetch('/api/bank-dashboard-users', { credentials: 'include' }).then(r => r.json()).then(d => { if (d.data) _setBdUsers(d.data); }).catch(() => {});
  }, [refetchScenarios]);
  const _syncSave = useCallback((id: string, name: string, data: ScenarioData) => {
    if (!_srvRef.current) return;
    fetch('/api/scenarios', { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({id, name, data, company: activeCompany}) }).catch(() => {});
  }, [activeCompany]);
  const _syncUpdate = useCallback((id: string, updates: {name?: string; data?: ScenarioData}) => {
    if (!_srvRef.current) return;
    fetch('/api/scenarios/' + id, { method: 'PUT', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(updates) }).catch(() => {});
  }, []);
  const _syncDelete = useCallback((id: string) => {
    if (!_srvRef.current) return;
    fetch('/api/scenarios/' + id, { method: 'DELETE', credentials: 'include' }).catch(() => {});
  }, []);
  const _loadShares = useCallback((sid: string) => {
    fetch('/api/scenarios/' + sid + '/shares', { credentials: 'include' }).then(r => r.json()).then(d => {
      _setShareMap(prev => ({...prev, [sid]: d.data || []}));
    }).catch(() => {});
  }, []);
  const _ensureSaved = useCallback((sid: string) => {
    const sc = scenarios.find((x: any) => x.id === sid);
    if (!sc) return Promise.reject(new Error('Scenario not found'));
    return fetch('/api/scenarios', { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({id: sc.id, name: sc.name, data: sc.data}) }).then(r => {
      if (!r.ok) return Promise.reject(new Error('Failed to save scenario'));
    });
  }, [scenarios]);
  const _toggleShare = useCallback((sid: string, email: string, isCurrentlyShared: boolean) => {
    const pendingKey = sid + '::' + email.toLowerCase();
    if (_sharePending[pendingKey]) return;
    const method = isCurrentlyShared ? 'DELETE' : 'POST';
    const url = isCurrentlyShared ? '/api/scenarios/' + sid + '/share/' + encodeURIComponent(email) : '/api/scenarios/' + sid + '/share';
    const opts: any = { method, credentials: 'include' };
    if (!isCurrentlyShared) { opts.headers = {'Content-Type':'application/json'}; opts.body = JSON.stringify({ email }); }
    const ready = isCurrentlyShared ? Promise.resolve() : _ensureSaved(sid);
    _setSharePending(prev => ({ ...prev, [pendingKey]: true }));
    ready.then(() => fetch(url, opts)).then(res => {
      if (!res.ok) {
        return res.text().then(t => {
          let msg = 'Share failed';
          try { const j = JSON.parse(t); if (j.error) msg = j.error; } catch {}
          return Promise.reject(new Error(msg));
        });
      }
      _loadShares(sid);
      refetchScenarios();
    }).catch((e: any) => {
      _loadShares(sid);
      if (e && e.message) alert(e.message);
    }).finally(() => {
      _setSharePending(prev => {
        const next = { ...prev };
        delete next[pendingKey];
        return next;
      });
    });
  }, [_loadShares, _ensureSaved, refetchScenarios, _sharePending]);
  useEffect(() => { try { if (activeScenarioId) localStorage.setItem('banks-active-scenario', activeScenarioId); else localStorage.removeItem('banks-active-scenario'); } catch {} }, [activeScenarioId]);

  // Auto-sync active scenario to consolidated pickers when switching tabs
  const prevCompanyRef = useRef<CompanyView>(activeCompany);
  useEffect(() => {
    const prev = prevCompanyRef.current;
    prevCompanyRef.current = activeCompany;
    if (activeCompany === 'consolidated' && prev !== 'consolidated' && activeScenarioId) {
      const s = scenarios.find(x => x.id === activeScenarioId);
      if (s) {
        if ((!s.company || s.company === 'lsports') && prev === 'lsports') setConsLsScenarioId(activeScenarioId);
        if ((!s.company || s.company === 'statscore') && prev === 'statscore') setConsStScenarioId(activeScenarioId);
      }
    }
  }, [activeCompany, activeScenarioId, scenarios]);

  // Close scenario menu on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => { if (scenarioMenuRef.current && !scenarioMenuRef.current.contains(e.target as Node)) { setScenarioMenuOpen(false); setScenarioNameEdit(null); } };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const getCurrentScenarioData = useCallback((): ScenarioData => ({
    salaryAdjPctByMonth: { ...salaryAdjPctByMonth },
    collPctByMonth: { ...collPctByMonth },
    salaryDeptAdj: JSON.parse(JSON.stringify(salaryDeptAdj)),
    vendorCatAdj: JSON.parse(JSON.stringify(vendorCatAdj)),
    vendorDetailAdj: JSON.parse(JSON.stringify(vendorDetailAdj)),
    leverOverrides: JSON.parse(JSON.stringify(leverOverrides)),
    pipelineMinProb,
  }), [salaryAdjPctByMonth, collPctByMonth, salaryDeptAdj, vendorCatAdj, vendorDetailAdj, leverOverrides, pipelineMinProb]);

  const applyScenarioData = useCallback((data: ScenarioData) => {
    setSalaryAdjPctByMonth(data.salaryAdjPctByMonth || {});
    setCollPctByMonth(data.collPctByMonth || {});
    setSalaryDeptAdj(data.salaryDeptAdj || {});
    setVendorCatAdj(data.vendorCatAdj || {});
    setVendorDetailAdj(data.vendorDetailAdj || {});
    setLeverOverrides(data.leverOverrides || {});
    setPipelineMinProb(data.pipelineMinProb ?? 100);
  }, []);

  const saveScenario = useCallback((name: string) => {
    const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    const now = new Date().toISOString();
    const newScenario: Scenario = { id, name, createdAt: now, updatedAt: now, data: getCurrentScenarioData() };
    setScenarios(prev => [...prev, newScenario]);
    setActiveScenarioId(id);
    setScenarioMenuOpen(false);
    _syncSave(id, name, getCurrentScenarioData());
  }, [getCurrentScenarioData, _syncSave]);

  const updateScenario = useCallback((id: string) => {
    setScenarios(prev => prev.map(s => s.id === id ? { ...s, updatedAt: new Date().toISOString(), data: getCurrentScenarioData() } : s));
    _syncUpdate(id, { data: getCurrentScenarioData() });
  }, [getCurrentScenarioData, _syncUpdate]);

  const loadScenario = useCallback((id: string) => {
    const scenario = scenarios.find(s => s.id === id);
    if (scenario) { applyScenarioData(scenario.data); setActiveScenarioId(id); setScenarioMenuOpen(false); }
  }, [scenarios, applyScenarioData]);

  const deleteScenario = useCallback((id: string) => {
    setScenarios(prev => prev.filter(s => s.id !== id));
    if (activeScenarioId === id) setActiveScenarioId(null);
    if (compareScenarioId === id) setCompareScenarioId(null);
    _syncDelete(id);
  }, [activeScenarioId, compareScenarioId, _syncDelete]);

  const renameScenario = useCallback((id: string, newName: string) => {
    if (!newName.trim()) return;
    setScenarios(prev => prev.map(s => s.id === id ? { ...s, name: newName.trim() } : s));
    setScenarioNameEdit(null);
    _syncUpdate(id, { name: newName.trim() });
  }, [_syncUpdate]);

  // Describe differences between two scenario data sets
  const describeScenarioDiff = useCallback((from: ScenarioData | null, to: ScenarioData, label: string): { label: string; items: { key: string; desc: string; color: string }[] } => {
    const items: { key: string; desc: string; color: string }[] = [];
    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    // Salary % changes
    for (let i = 0; i < 12; i++) {
      const fromVal = from?.salaryAdjPctByMonth?.[i] || 0;
      const toVal = to.salaryAdjPctByMonth?.[i] || 0;
      if (fromVal !== toVal) {
        if (from) items.push({ key: `sal-${i}`, desc: `${monthNames[i]} salary: ${fromVal}% → ${toVal}%`, color: 'text-amber-600' });
        else if (toVal !== 0) items.push({ key: `sal-${i}`, desc: `${monthNames[i]} salary: ${toVal > 0 ? '+' : ''}${toVal}%`, color: 'text-amber-600' });
      }
    }
    // Collection % changes
    for (let i = 0; i < 12; i++) {
      const fromVal = from?.collPctByMonth?.[i] ?? 100;
      const toVal = to.collPctByMonth?.[i] ?? 100;
      if (fromVal !== toVal) {
        if (from) items.push({ key: `coll-${i}`, desc: `${monthNames[i]} collection: ${fromVal}% → ${toVal}%`, color: 'text-green-600' });
        else if (toVal !== 100) items.push({ key: `coll-${i}`, desc: `${monthNames[i]} collection: ${toVal}%`, color: 'text-green-600' });
      }
    }
    // Dept adj changes
    const allDeptKeys = new Set([...Object.keys(from?.salaryDeptAdj || {}), ...Object.keys(to.salaryDeptAdj || {})]);
    for (const mKey of Array.from(allDeptKeys).sort()) {
      const fromDepts = from?.salaryDeptAdj?.[mKey] || {};
      const toDepts = to.salaryDeptAdj?.[mKey] || {};
      const allDepts = new Set([...Object.keys(fromDepts), ...Object.keys(toDepts)]);
      for (const dept of allDepts) {
        const fv = fromDepts[dept] || 0;
        const tv = toDepts[dept] || 0;
        if (fv !== tv) {
          const mo = monthNames[parseInt(mKey.split('-')[1]) - 1] || mKey;
          if (from) items.push({ key: `dept-${mKey}-${dept}`, desc: `${mo} ${dept}: ${fv}% → ${tv}%`, color: 'text-orange-600' });
          else if (tv !== 0) items.push({ key: `dept-${mKey}-${dept}`, desc: `${mo} ${dept}: ${tv > 0 ? '+' : ''}${tv}%`, color: 'text-orange-600' });
        }
      }
    }
    // Pipeline filter
    const fromPipe = from?.pipelineMinProb ?? 100;
    const toPipe = to.pipelineMinProb ?? 100;
    if (fromPipe !== toPipe) {
      items.push({ key: 'pipe', desc: from ? `Pipeline filter: ${fromPipe}% → ${toPipe}%` : `Pipeline filter: ${toPipe}%`, color: 'text-teal-600' });
    }
    return { label, items };
  }, []);

  const companyScenarios = useMemo(() => scenarios.filter(s => !s.company || s.company === activeCompany), [scenarios, activeCompany]);
  const activeScenario = scenarios.find(s => s.id === activeScenarioId);
  const hasAnyAdjustments = Object.values(salaryAdjPctByMonth).some(v => v !== 0) || Object.keys(collPctByMonth).length > 0 || Object.values(salaryDeptAdj).some(m => Object.values(m).some(v => v !== 0)) || Object.values(vendorCatAdj).some(m => Object.values(m).some(v => v !== 0)) || Object.keys(vendorDetailAdj).length > 0 || Object.keys(leverOverrides).length > 0;

  // Compare scenario: compute cashflow with comparison data
  const compareScenario = scenarios.find(s => s.id === compareScenarioId);

  // Save current LS/ST state to cache
  const saveToCache = useCallback((co: string) => {
    companyDataCache.current[co] = {
      bankData, bankAccounts, vendorBills, arForecast, salaryData, vendorHistory,
      expenseCategories, actualCollections, monthlyReval, nsBudget, nsAccountId,
      sfBudget, sfRevenue, sfActualsSplit, sfSalaryBudget, sfSalaryOverrides,
      sfRevenuePaid, sfPipeline, sfConversion, churnData, churnMonthlyAvg,
      yoyRevenue, prevMonthEndBalance, salaryDeptBudgets,
    };
  }, [bankData, bankAccounts, vendorBills, arForecast, salaryData, vendorHistory,
      expenseCategories, actualCollections, monthlyReval, nsBudget, nsAccountId,
      sfBudget, sfRevenue, sfActualsSplit, sfSalaryBudget, sfSalaryOverrides,
      sfRevenuePaid, sfPipeline, sfConversion, churnData, churnMonthlyAvg,
      yoyRevenue, prevMonthEndBalance, salaryDeptBudgets]);

  const restoreFromCache = useCallback((co: string) => {
    const c = companyDataCache.current[co];
    if (!c) return false;
    setBankData(c.bankData); setBankAccounts(c.bankAccounts); setVendorBills(c.vendorBills);
    setARForecast(c.arForecast); setSalaryData(c.salaryData); setVendorHistory(c.vendorHistory);
    setExpenseCategories(c.expenseCategories); setActualCollections(c.actualCollections);
    setMonthlyReval(c.monthlyReval); setNsBudget(c.nsBudget); setNsAccountId(c.nsAccountId);
    setSfBudget(c.sfBudget); setSfRevenue(c.sfRevenue); setSfActualsSplit(c.sfActualsSplit);
    setSfSalaryBudget(c.sfSalaryBudget); setSfSalaryOverrides(c.sfSalaryOverrides);
    setSfRevenuePaid(c.sfRevenuePaid); setSfPipeline(c.sfPipeline); setSfConversion(c.sfConversion);
    setChurnData(c.churnData); setChurnMonthlyAvg(c.churnMonthlyAvg);
    setYoyRevenue(c.yoyRevenue); setPrevMonthEndBalance(c.prevMonthEndBalance);
    setSalaryDeptBudgets(c.salaryDeptBudgets);
    return true;
  }, []);

  // Pre-fetch a company's data into cache without touching state (background load)
  const prefetchCompany = useCallback(async (co: CompanyView) => {
    if (companyDataCache.current[co]) return; // already cached
    const cfg = COMPANY_CONFIG[co] || COMPANY_CONFIG.lsports;
    const subQ = `?subsidiary=${cfg.subsidiary}`;
    const hasSF = cfg.hasSF;
    try {
      console.info(`[Prefetch] Loading ${co} data in background...`);
      const cache: any = {};
      const safe = async (url: string) => { try { const r = await fetch(url); if (r.ok) return await r.json(); } catch {} return null; };
      // Fire all NS calls in parallel (server queues them anyway)
      const [bankR, acctR, billsR, arR, salR, vhR, expR, collR, revalR] = await Promise.all([
        safe(`/api/bank-balance${subQ}`), safe(`/api/bank-accounts${subQ}`), safe(`/api/vendor-bills${subQ}`),
        safe(`/api/ar-forecast${subQ}`), safe(`/api/salary-data${subQ}`), safe(`/api/vendor-history${subQ}`),
        safe(`/api/expense-categories${subQ}`), safe(`/api/banks-collection-data${subQ}`), safe(`/api/monthly-reval${subQ}`),
      ]);
      if (bankR?.dailyBalances) cache.bankData = bankR;
      if (acctR?.data) cache.bankAccounts = acctR.data;
      if (billsR?.data) cache.vendorBills = billsR.data;
      if (arR?.data) cache.arForecast = arR.data;
      if (salR?.data) cache.salaryData = salR.data;
      if (vhR?.data) cache.vendorHistory = vhR.data;
      if (expR?.data) cache.expenseCategories = expR.data;
      if (collR?.data) cache.actualCollections = collR.data;
      if (revalR?.data) cache.monthlyReval = revalR.data;
      if (!hasSF) {
        const nsBudR = await safe(`/api/ns-budget${subQ}`);
        if (nsBudR) cache.nsBudget = nsBudR;
        cache.sfBudget = { totalByMonth: {} }; cache.sfRevenue = {}; cache.sfActualsSplit = {};
        cache.sfSalaryBudget = {}; cache.sfSalaryOverrides = []; cache.sfRevenuePaid = {};
        cache.sfPipeline = []; cache.sfConversion = { yearly: [], stages: [], customers: [], projection: [] };
        cache.churnData = []; cache.churnMonthlyAvg = 0; cache.yoyRevenue = null;
        cache.salaryDeptBudgets = {};
      } else {
        cache.nsBudget = { byMonth: {} };
        // Fire all SF calls in parallel
        const [budR, revR, splitR, salBudR, revPaidR, pipeR, convR, churnR, yoyR] = await Promise.all([
          safe('/api/sf-budget'), safe('/api/sf-revenue'), safe('/api/sf-actuals-split'),
          safe('/api/sf-salary-budget'), safe('/api/sf-revenue-paid'), safe('/api/sf-pipeline'),
          safe('/api/sf-conversion'), safe('/api/sf-churn-analysis'), safe('/api/sf-yoy-revenue'),
        ]);
        if (budR?.data) cache.sfBudget = budR.data;
        if (revR?.data) cache.sfRevenue = revR.data;
        if (splitR?.data) cache.sfActualsSplit = splitR.data;
        if (salBudR?.data) cache.sfSalaryBudget = salBudR.data;
        if (salBudR?.overrides) cache.sfSalaryOverrides = salBudR.overrides;
        if (revPaidR?.data) cache.sfRevenuePaid = revPaidR.data;
        if (pipeR?.data) cache.sfPipeline = pipeR.data;
        if (convR?.data) cache.sfConversion = convR.data;
        if (churnR?.data) cache.churnData = churnR.data;
        if (churnR?.recentMonthlyAvg) cache.churnMonthlyAvg = churnR.recentMonthlyAvg;
        if (yoyR?.currentYear) cache.yoyRevenue = yoyR;
      }
      // Fill defaults for missing fields
      cache.bankData = cache.bankData || { openingBalance: 0, dailyBalances: [], currentBalance: 0 };
      cache.bankAccounts = cache.bankAccounts || [];
      cache.vendorBills = cache.vendorBills || [];
      cache.arForecast = cache.arForecast || [];
      cache.salaryData = cache.salaryData || [];
      cache.vendorHistory = cache.vendorHistory || [];
      cache.expenseCategories = cache.expenseCategories || { byMonth: {}, categories: [] };
      cache.actualCollections = cache.actualCollections || {};
      cache.monthlyReval = cache.monthlyReval || { byMonth: {}, preYear: { eur: 0, ils: 0 } };
      cache.nsBudget = cache.nsBudget || { byMonth: {} };
      cache.nsAccountId = cache.nsAccountId || '';
      cache.sfBudget = cache.sfBudget || { totalByMonth: {} };
      cache.sfRevenue = cache.sfRevenue || {};
      cache.sfActualsSplit = cache.sfActualsSplit || {};
      cache.sfSalaryBudget = cache.sfSalaryBudget || {};
      cache.sfSalaryOverrides = cache.sfSalaryOverrides || [];
      cache.sfRevenuePaid = cache.sfRevenuePaid || {};
      cache.sfPipeline = cache.sfPipeline || [];
      cache.sfConversion = cache.sfConversion || { yearly: [], stages: [], customers: [], projection: [] };
      cache.churnData = cache.churnData || [];
      cache.churnMonthlyAvg = cache.churnMonthlyAvg || 0;
      cache.yoyRevenue = cache.yoyRevenue || null;
      cache.prevMonthEndBalance = cache.prevMonthEndBalance || null;
      cache.salaryDeptBudgets = cache.salaryDeptBudgets || {};
      companyDataCache.current[co] = cache;
      console.info(`[Prefetch] ${co} data cached successfully`);
    } catch (e: any) { console.error(`[Prefetch] ${co} failed:`, e.message); }
  }, []);

  const fetchData = useCallback(async (company?: CompanyView, forceRefresh?: boolean) => {
    const co = company || activeCompany;
    const cfg = COMPANY_CONFIG[co] || COMPANY_CONFIG.lsports;
    const subQ = `?subsidiary=${cfg.subsidiary}&year=${activeYear}`;
    const hasSF = cfg.hasSF;

    // ── Fast path: serve from cache instantly (no loading flash) ──
    if (!forceRefresh) {
      if (co === 'consolidated' && consolidatedData) {
        refetchScenarios();
        setLastRefreshed(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB'));
        return;
      }
      if (co !== 'consolidated' && restoreFromCache(co)) {
        refetchScenarios();
        setLastRefreshed(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB'));
        return;
      }
    }

    setIsLoading(true);
    try {
      // ── Consolidated: single endpoint fetches both subsidiaries ──
      if (co === 'consolidated') {
        try {
          const resp = await fetch(`/api/consolidated-data?lsYear=${activeYears.lsports || currentYear}&stYear=${activeYears.statscore || currentYear}${forceRefresh ? '&refresh=true' : ''}`);
          if (resp.ok) {
            const data = await resp.json();
            setConsolidatedData(data);
          }
        } catch (e: any) { console.error('[Consolidated] fetch failed:', e.message); }
        // Also fetch scenarios list
        refetchScenarios();
        setLastRefreshed(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB'));
        setIsLoading(false);
        return;
      }

      // ── Snapshot year: load from saved snapshot instead of live NS/Snowflake ──
      const coYear = activeYears[co] || currentYear;
      if (coYear !== currentYear) {
        try {
          const snapResp = await fetch(`/api/budget-snapshot?year=${coYear}&company=${co}`);
          const snapResult = await snapResp.json();
          if (snapResult.exists && snapResult.data) {
            const snap = snapResult.data;
            console.info(`[Snapshot] Loading ${co} ${coYear} from snapshot`);
            // ── Live propagation: derive opening balance, salary & vendor baselines from current-year cashflow ──
            const liveCf = sourceYearCashflowRef.current[co];
            const hasLiveCf = liveCf?.length === 12;
            if (hasLiveCf) {
              console.info(`[Snapshot] Using live ${currentYear} cashflow for ${co} ${coYear} baselines`);
            }
            // Opening balance: live Dec closing > snapshot projected > snapshot bankBalance
            const openBal = hasLiveCf ? liveCf[11].closingBalance : (snap.projectedDecClosing || snap.bankBalance?.openingBalance || 0);
            setBankData({ openingBalance: openBal, dailyBalances: [], currentBalance: openBal });
            setBankAccounts([]);
            setVendorBills([]);
            setARForecast([]);
            // Salary/vendor history from snapshot (used to derive budget baseline)
            setSalaryData(snap.salary || []);
            setVendorHistory(snap.vendorHistory || []);
            setExpenseCategories({ byMonth: {}, categories: [] });
            setActualCollections(snap.collections || {});
            // Zero out preYear reval since projectedDecClosing already includes it
            setMonthlyReval({ byMonth: {}, preYear: { eur: 0, ils: 0 } });
            setPrevMonthEndBalance(null);
            // Budget data from snapshot — with live overrides for salary & vendors
            if (hasLiveCf) {
              // Salary baseline: avg of last 3 months (Oct, Nov, Dec) from live current-year cashflow
              const avgSalary = Math.round((liveCf[9].salary + liveCf[10].salary + liveCf[11].salary) / 3);
              const liveSalaryBudget: Record<string, { eur: number; ils: number }> = {};
              for (let m = 1; m <= 12; m++) {
                liveSalaryBudget[`${coYear}-${String(m).padStart(2, '0')}`] = { eur: avgSalary, ils: 0 };
              }
              setSfSalaryBudget(liveSalaryBudget);
              console.info(`[Snapshot] ${co} ${coYear} salary baseline: avg(Oct/Nov/Dec) = €${avgSalary.toLocaleString()}`);
              // Vendor baseline: avg of full year from live current-year cashflow
              const avgVendors = Math.round(liveCf.reduce((s: number, r: any) => s + r.vendors, 0) / 12);
              const liveVendorTotal: Record<string, { eur: number; ils: number }> = {};
              for (let m = 1; m <= 12; m++) {
                liveVendorTotal[`${coYear}-${String(m).padStart(2, '0')}`] = { eur: avgVendors, ils: 0 };
              }
              // Keep byMonth categories from snapshot for category adjustments, override totals
              setSfBudget({ byMonth: snap.sfBudget?.byMonth || {}, totalByMonth: liveVendorTotal });
              console.info(`[Snapshot] ${co} ${coYear} vendor baseline: avg(12m) = €${avgVendors.toLocaleString()}`);
              // Also override nsBudget for non-SF subsidiaries (Statscore)
              const isNonSF = !snap.sfSalaryBudget || Object.keys(snap.sfSalaryBudget).length === 0;
              if (isNonSF) {
                const liveNsBud: Record<string, any> = {};
                for (let m = 1; m <= 12; m++) {
                  const mk = `${coYear}-${String(m).padStart(2, '0')}`;
                  const existing = snap.nsBudget?.byMonth?.[mk] || {};
                  liveNsBud[mk] = { salary: avgSalary, vendors: avgVendors, revenue: existing.revenue || 0 };
                }
                setNsBudget({ byMonth: liveNsBud });
              } else {
                if (snap.nsBudget) setNsBudget(snap.nsBudget);
                else setNsBudget({ byMonth: {} });
              }
            } else {
              if (snap.sfBudget) setSfBudget(snap.sfBudget);
              else setSfBudget({ totalByMonth: {} });
              if (snap.sfSalaryBudget) setSfSalaryBudget(snap.sfSalaryBudget);
              else setSfSalaryBudget({});
              if (snap.nsBudget) setNsBudget(snap.nsBudget);
              else setNsBudget({ byMonth: {} });
            }
            if (snap.sfRevenue) setSfRevenue(snap.sfRevenue);
            else setSfRevenue({});
            if (snap.sfActualsSplit) setSfActualsSplit(snap.sfActualsSplit);
            else setSfActualsSplit({});
            if (snap.sfRevenuePaid) setSfRevenuePaid(snap.sfRevenuePaid);
            else setSfRevenuePaid({});
            if (snap.sfPipeline) setSfPipeline(snap.sfPipeline);
            else setSfPipeline([]);
            if (snap.sfConversion) setSfConversion(snap.sfConversion);
            else setSfConversion({ yearly: [], stages: [], customers: [], projection: [] });
            setSfSalaryOverrides([]);
            setChurnData([]);
            setChurnMonthlyAvg(0);
            setYoyRevenue(null);
            setSalaryDeptBudgets({});
            // Persist updated opening balance to snapshot file for page refresh consistency
            if (hasLiveCf && Math.round(openBal) !== Math.round(snap.projectedDecClosing || 0)) {
              fetch('/api/budget-snapshot-patch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ year: coYear, company: co, projectedDecClosing: Math.round(openBal) }),
              }).catch(() => {}); // fire-and-forget
            }
            refetchScenarios();
            setLastRefreshed(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB'));
            setIsLoading(false);
            return;
          }
        } catch (e: any) { console.error(`[Snapshot] Failed to load ${co} ${coYear}:`, e.message); }
      }

      // Helper for safe parallel fetching — append refresh=true to bypass server cache
      const refreshQ = forceRefresh ? '&refresh=true' : '';
      const safe = async (url: string) => { try { const u = url + (url.includes('?') ? refreshQ : refreshQ.replace('&', '?')); const r = await fetch(u); if (r.ok) return await r.json(); } catch {} return null; };

      // Fire ALL requests in parallel — server queues NS calls, SF runs concurrently
      const nsUrls = [
        `/api/ns-config`, `/api/bank-balance${subQ}`, `/api/bank-accounts${subQ}`,
        `/api/vendor-bills${subQ}`, `/api/ar-forecast${subQ}`, `/api/salary-data${subQ}`,
        `/api/vendor-history${subQ}`, `/api/expense-categories${subQ}`,
        `/api/banks-collection-data${subQ}`, `/api/monthly-reval${subQ}`,
      ];
      const sfUrls = hasSF ? [
        '/api/sf-budget', '/api/sf-revenue', '/api/sf-actuals-split', '/api/sf-salary-budget',
        '/api/sf-revenue-paid', '/api/sf-pipeline', '/api/sf-conversion', '/api/sf-churn-analysis',
        '/api/sf-yoy-revenue',
      ] : !hasSF ? [`/api/ns-budget${subQ}`] : [];

      const allResults = await Promise.all([...nsUrls, ...sfUrls].map(u => safe(u)));
      const [cfgR, bankR, acctR, billsR, arR, salR, vhR, expR, collR, revalR, ...sfResults] = allResults;

      // Apply NS results
      if (cfgR?.accountId) setNsAccountId(cfgR.accountId);
      if (bankR?.dailyBalances) setBankData(bankR);
      if (acctR?.data) setBankAccounts(acctR.data);
      if (billsR?.data) setVendorBills(billsR.data);
      if (arR?.data) setARForecast(arR.data);
      if (salR?.data) setSalaryData(salR.data);
      if (vhR?.data) setVendorHistory(vhR.data);
      if (expR?.data) setExpenseCategories(expR.data);
      if (collR?.data) setActualCollections(collR.data);
      if (revalR?.data) setMonthlyReval(revalR.data);

      // Fetch previous month-end bank balances for cashflow anchoring (non-blocking)
      try {
        const refDate = asOfDate ? new Date(asOfDate + 'T12:00:00') : new Date();
        const prevEnd = new Date(refDate.getFullYear(), refDate.getMonth(), 0);
        const asOfStr = `${prevEnd.getFullYear()}-${String(prevEnd.getMonth()+1).padStart(2,'0')}-${String(prevEnd.getDate()).padStart(2,'0')}`;
        fetch(`/api/ns-bank-accounts-asof?date=${asOfStr}&subsidiary=${cfg.subsidiary}`).then(r => r.json()).then(j => {
          if (j.data && j.data.length > 0) {
            const accounts = j.data as BankAccount[];
            const ccKeywords = ['AMEX', 'MasterCard', 'Isracard', 'Visa'];
            const bankOnly = accounts.filter(a => !ccKeywords.some(k => a.name.includes(k)));
            const eur = bankOnly.reduce((s: number, a: BankAccount) => s + a.primaryBalance, 0);
            const ils = bankOnly.reduce((s: number, a: BankAccount) => s + a.localBalance, 0);
            setPrevMonthEndBalance({ eur, ils });
          }
        }).catch(() => {});
      } catch {}

      // Apply SF or NS budget results
      if (hasSF) {
        setNsBudget({ byMonth: {} });
        const [budR, revR, splitR, salBudR, revPaidR, pipeR, convR, churnR, yoyR] = sfResults;
        if (budR?.data) setSfBudget(budR.data);
        if (revR?.data) setSfRevenue(revR.data);
        if (splitR?.data) setSfActualsSplit(splitR.data);
        if (salBudR?.data) setSfSalaryBudget(salBudR.data);
        if (salBudR?.overrides) setSfSalaryOverrides(salBudR.overrides);
        if (revPaidR?.data) setSfRevenuePaid(revPaidR.data);
        if (pipeR?.data) setSfPipeline(pipeR.data);
        if (convR?.data) setSfConversion(convR.data);
        if (churnR?.data) setChurnData(churnR.data);
        if (churnR?.recentMonthlyAvg) setChurnMonthlyAvg(churnR.recentMonthlyAvg);
        if (yoyR?.currentYear) setYoyRevenue(yoyR);
      } else {
        const [nsBudR] = sfResults;
        if (nsBudR) setNsBudget(nsBudR);
        setSfBudget({ totalByMonth: {} }); setSfRevenue({}); setSfActualsSplit({});
        setSfSalaryBudget({}); setSfSalaryOverrides([]); setSfRevenuePaid({});
        setSfPipeline([]); setSfConversion({ yearly: [], stages: [], customers: [], projection: [] });
        setChurnData([]); setChurnMonthlyAvg(0); setYoyRevenue(null); setSalaryDeptBudgets({});
      }
      setLastRefreshed(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB'));
    } catch (e: any) {
      console.error('Banks data fetch failed:', e.message);
    } finally {
      setIsLoading(false);
    }
  }, [activeCompany, activeYears]);

  // Invalidate cache when year changes, then fetch
  const prevYearsRef = useRef({ ...activeYears });
  useEffect(() => {
    const prevYrs = prevYearsRef.current;
    const yearChanged = activeCompany !== 'consolidated'
      ? prevYrs[activeCompany] !== activeYears[activeCompany]
      : prevYrs.lsports !== activeYears.lsports || prevYrs.statscore !== activeYears.statscore;
    prevYearsRef.current = { ...activeYears };
    if (yearChanged) {
      // Only invalidate cache when switching TO a snapshot year (needs fresh snapshot data)
      // When switching BACK to current year, keep cache so it's instant
      const newYear = activeYears[activeCompany] || currentYear;
      if (newYear !== currentYear) {
        if (activeCompany === 'consolidated') { companyDataCache.current = {}; }
        else { delete companyDataCache.current[activeCompany]; }
        setConsolidatedData(null);
      }
      fetchData(undefined, newYear !== currentYear);
    } else {
      fetchData();
    }
  }, [activeCompany, activeYears]);

  // On initial mount: pre-fetch the OTHER company in the background so switching is instant
  const prefetchDone = useRef(false);
  useEffect(() => {
    if (prefetchDone.current) return;
    prefetchDone.current = true;
    // Wait a bit for active company to start loading, then prefetch the other one
    setTimeout(() => {
      const other: CompanyView = activeCompany === 'lsports' ? 'statscore' : 'lsports';
      prefetchCompany(other);
      // Also prefetch consolidated data (server-side cache will speed up subsequent requests)
      if (activeCompany !== 'consolidated') {
        fetch(`/api/consolidated-data?lsYear=${activeYears.lsports || currentYear}&stYear=${activeYears.statscore || currentYear}`).then(r => r.json()).then(data => {
          setConsolidatedData(data);
          console.info('[Prefetch] Consolidated data pre-loaded');
        }).catch(() => {});
      }
    }, 2000); // 2s delay so active company loads first
  }, []);

  // Auto-save LS/ST data to cache after loading completes
  useEffect(() => {
    if (!isLoading && activeCompany !== 'consolidated' && bankData.dailyBalances.length > 0) {
      saveToCache(activeCompany);
    }
  }, [isLoading, activeCompany, bankData, saveToCache]);

  // Reset active scenario when switching companies (scenarios are per-company)
  useEffect(() => {
    if (activeScenarioId) {
      const s = scenarios.find(x => x.id === activeScenarioId);
      if (s && s.company && s.company !== activeCompany) {
        setActiveScenarioId(null);
        // Reset all adjustments to baseline
        setSalaryAdjPctByMonth({});
        setCollPctByMonth({});
        setSalaryDeptAdj({});
        setVendorCatAdj({});
        setVendorDetailAdj({});
        setLeverOverrides({});
      }
    }
  }, [activeCompany]);

  // Re-fetch prevMonthEndBalance anchor when as-of date changes
  useEffect(() => {
    // Always re-fetch YoY revenue (even when clearing date back to live)
    fetch(`/api/sf-yoy-revenue${asOfDate ? `?asOfDate=${asOfDate}` : ''}`).then(r => r.json()).then(r => {
      if (r.currentYear) setYoyRevenue(r);
    }).catch(() => {});
    if (!asOfDate) return;
    const refDate = new Date(asOfDate + 'T12:00:00');
    const prevEnd = new Date(refDate.getFullYear(), refDate.getMonth(), 0);
    const prevStr = `${prevEnd.getFullYear()}-${String(prevEnd.getMonth()+1).padStart(2,'0')}-${String(prevEnd.getDate()).padStart(2,'0')}`;
    fetch(`/api/ns-bank-accounts-asof?date=${prevStr}`).then(r => r.json()).then(j => {
      if (j.data && j.data.length > 0) {
        const ccKeywords = ['AMEX', 'MasterCard', 'Isracard', 'Visa'];
        const bankOnly = (j.data as BankAccount[]).filter((a: BankAccount) => !ccKeywords.some(k => a.name.includes(k)));
        const eur = bankOnly.reduce((s: number, a: BankAccount) => s + a.primaryBalance, 0);
        const ils = bankOnly.reduce((s: number, a: BankAccount) => s + a.localBalance, 0);
        setPrevMonthEndBalance({ eur, ils });
      }
    }).catch(() => {});
  }, [asOfDate]);

  // Derived bank data — filtered by as-of date when set
  const book = useMemo(() => {
    const raw = bankData.primary || bankData;
    if (!asOfDate || !raw?.dailyBalances?.length) return raw;
    const filtered = raw.dailyBalances.filter((d: any) => d.date <= asOfDate);
    const lastDay = filtered.length > 0 ? filtered[filtered.length - 1] : null;
    return { ...raw, dailyBalances: filtered, currentBalance: lastDay?.balance || 0, adjustedCurrentBalance: lastDay?.adjustedBalance || lastDay?.balance || 0 };
  }, [bankData, asOfDate]);
  const bookLocal = useMemo(() => {
    const raw = bankData.local;
    if (!asOfDate || !raw?.dailyBalances?.length) return raw;
    const filtered = raw.dailyBalances.filter((d: any) => d.date <= asOfDate);
    const lastDay = filtered.length > 0 ? filtered[filtered.length - 1] : null;
    return { ...raw, dailyBalances: filtered, currentBalance: lastDay?.balance || 0, adjustedCurrentBalance: lastDay?.adjustedBalance || lastDay?.balance || 0 };
  }, [bankData, asOfDate]);
  const hasAdjusted = book?.dailyBalances?.some((d: any) => d.adjustedBalance !== undefined && d.adjustedBalance !== d.balance);
  const hasAdjustedLocal = bookLocal?.dailyBalances?.some((d: any) => d.adjustedBalance !== undefined && d.adjustedBalance !== d.balance);
  const adjustedCurrent = (book as any)?.adjustedCurrentBalance || book?.currentBalance || 0;
  const adjustedCurrentLocal = (bookLocal as any)?.adjustedCurrentBalance || bookLocal?.currentBalance || 0;

  // Monthly cashflow forecast — Snowflake as single source of truth for actuals + projections
  const cashflowForecast = useMemo(() => {
    const now = asOfDate ? new Date(asOfDate + 'T12:00:00') : new Date();
    const forecastYear = activeYear;
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

    // Fallbacks from NS (only used when Snowflake has no data)
    const completedSalaries = salaryData.filter(s => s.month < currentMonth && s.amountEUR > 0);
    const lastSalary = completedSalaries.length > 0 ? completedSalaries[completedSalaries.length - 1].amountEUR : 0;
    const openBillsTotal = vendorBills.reduce((s, b) => s + b.amountEUR, 0);

    // Pipeline impact: filtered opps add recurring MRR from their close month onward
    const filteredPipeline = pipelineMinProb > 0 ? sfPipeline.filter(o => o.probability >= pipelineMinProb) : sfPipeline;
    const pipelineByMonth: Record<string, number> = {};
    // Low-conf pipeline: opps below threshold, weighted by historical win rate with delay
    // Use calculated values from Snowflake conversion analysis when available
    const recentYears = sfConversion.yearly.filter(y => y.year >= 2023);
    const calcWinRate = recentYears.length > 0 ? Math.round(recentYears.reduce((s, y) => s + y.winRate, 0) / recentYears.length) : 33;
    const calcAvgDays = recentYears.length > 0 ? Math.round(recentYears.reduce((s, y) => s + (y.avgWonDays || 0), 0) / recentYears.length) : 60;
    const pipelineHistWinRate = calcWinRate; // historical close-won ratio %
    const pipelineDelayMonths = Math.max(1, Math.round(calcAvgDays / 30)); // avg days to close → months
    const lowConfPipeline = sfPipeline.filter(o => o.probability < pipelineMinProb && o.probability > 0);
    const pipelineLowByMonth: Record<string, { weighted: number; total: number; count: number; opps: typeof lowConfPipeline }> = {};
    for (let mi = 0; mi < 12; mi++) {
      const mKey = `${forecastYear}-${String(mi + 1).padStart(2, '0')}`;
      // Opps closing on or before this month → recurring revenue
      pipelineByMonth[mKey] = filteredPipeline.filter(o => o.closeDate.substring(0, 7) <= mKey).reduce((s, o) => s + o.amount, 0);
      // Low-conf: opps that closed at least delayMonths ago (shifted), weighted by win rate
      const delayedMonth = new Date(forecastYear, mi - pipelineDelayMonths, 1);
      const delayedKey = `${delayedMonth.getFullYear()}-${String(delayedMonth.getMonth() + 1).padStart(2, '0')}`;
      const matchingOpps = lowConfPipeline.filter(o => o.closeDate.substring(0, 7) <= delayedKey);
      const total = matchingOpps.reduce((s, o) => s + o.amount, 0);
      pipelineLowByMonth[mKey] = {
        weighted: Math.round(total * pipelineHistWinRate / 100),
        total,
        count: matchingOpps.length,
        opps: matchingOpps,
      };
    }

    // EUR→ILS ratio from bank balances
    const eurIlsRatio = adjustedCurrent > 0 ? adjustedCurrentLocal / adjustedCurrent : 3.7;

    // Jan 1 opening = bank balance (excl reval) + cumulative pre-year reval
    let runningBalance = (book?.openingBalance || 0) + (monthlyReval.preYear?.eur || 0);
    let runningBalanceILS = (bookLocal?.openingBalance || 0) + (monthlyReval.preYear?.ils || 0);
    const rows: { month: string; mKey: string; openingBalance: number; openingBalanceILS: number; salary: number; salaryILS: number; vendors: number; vendorsILS: number; totalOutflow: number; totalOutflowILS: number; collections: number; collectionsILS: number; collectionsActual: number; collectionsRemaining: number; collectionsForecast: number; collectionsRevenue: number; collectionsUnpaidCarry: number; collectionsUnpaidCarryMonth: string; collectionsPipeline: number; customers: number; pipelineWeighted: number; pipelineWeightedILS: number; pipelineTotal: number; pipelineCount: number; pipelineOpps: typeof lowConfPipeline; pipelineHistWinRate: number; pipelineDelayMonths: number; net: number; netILS: number; revalImpact: number; revalImpactILS: number; revalHasBothEnds: boolean; closingBalance: number; closingBalanceILS: number; isCurrent: boolean; isPast: boolean }[] = [];
    let prevMonthSalary = 0;
    let prevMonthUnpaid = 0; // unpaid from previous month rolls forward

    for (let mi = 0; mi < 12; mi++) {
      const i = mi;
      const d = new Date(forecastYear, mi, 1);
      const mKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const label = `${monthNames[d.getMonth()]} ${d.getFullYear()}`;
      const isCurMonth = forecastYear === now.getFullYear() && mi === now.getMonth();
      const isPastMonth = forecastYear < now.getFullYear() || (forecastYear === now.getFullYear() && mi < now.getMonth());
      // Anchor current month to actual previous month-end bank balance (includes FxReval)
      // Only in live mode — in historical (asOfDate) mode, let running balance flow from opening
      if (isCurMonth && prevMonthEndBalance && !asOfDate) {
        runningBalance = prevMonthEndBalance.eur;
        runningBalanceILS = prevMonthEndBalance.ils;
      }
      const openingBalance = runningBalance;

      const monthAdj = salaryAdjPctByMonth[i] || 0;
      const monthMultiplier = 1 + (monthAdj / 100);
      // Per-department salary adjustment delta — cascades from earlier months
      // For each department, use the adjustment from this month if set, otherwise inherit from the most recent earlier month
      const effectiveDeptAdj: Record<string, number> = {};
      const allAdjMonths = Object.keys(salaryDeptAdj).filter(k => k <= mKey).sort();
      for (const adjMKey of allAdjMonths) {
        for (const [dept, pct] of Object.entries(salaryDeptAdj[adjMKey])) {
          if (pct !== 0) effectiveDeptAdj[dept] = pct;
          else delete effectiveDeptAdj[dept]; // explicitly set to 0 = clear
        }
      }
      let deptAdjDelta = 0;
      if (Object.keys(effectiveDeptAdj).length > 0 && salaryDeptBudgets[mKey]) {
        for (const [dept, pct] of Object.entries(effectiveDeptAdj)) {
          const deptBudget = salaryDeptBudgets[mKey][dept] || 0;
          deptAdjDelta += Math.round(deptBudget * (pct / 100));
        }
      }

      // ── SALARY: SF actuals (past/current) → NS actuals (past) → SF budget → NS budget → fallback ──
      let salary: number;
      let salaryBase: number; // base salary WITHOUT scenario adjustments (for delta display)
      const actualSalaryEntry = salaryData.find(s => s.month === mKey);
      if ((isPastMonth || isCurMonth) && sfActualsSplit[mKey]?.salary > 0) {
        salary = sfActualsSplit[mKey].salary;
        salaryBase = salary;
      } else if ((isPastMonth || isCurMonth) && actualSalaryEntry && actualSalaryEntry.amountEUR > 0) {
        // NS actual salary data (used for non-SF subsidiaries like Statscore)
        salary = actualSalaryEntry.amountEUR;
        salaryBase = salary;
      } else if (sfSalaryBudget[mKey]?.eur > 0) {
        salaryBase = Math.round(sfSalaryBudget[mKey].eur);
        salary = Math.round(sfSalaryBudget[mKey].eur * monthMultiplier) + deptAdjDelta;
      } else if (nsBudget.byMonth[mKey]?.salary > 0) {
        salaryBase = Math.round(nsBudget.byMonth[mKey].salary);
        salary = Math.round(nsBudget.byMonth[mKey].salary * monthMultiplier);
      } else {
        if (actualSalaryEntry && actualSalaryEntry.amountEUR > 0) {
          salary = actualSalaryEntry.amountEUR;
          salaryBase = salary;
        } else if (prevMonthSalary > 0) {
          salaryBase = prevMonthSalary;
          salary = Math.round(prevMonthSalary * monthMultiplier);
        } else {
          salary = lastSalary;
          salaryBase = salary;
        }
      }
      prevMonthSalary = salary;

      // ── VENDORS: SF actuals (past only) → NS vendor actuals (past) → SF budget → NS budget → NS fallback ──
      // Current month: use budget, not partial actuals (bills post throughout the month)
      let vendors: number;
      const nsVendorActual = isPastMonth ? vendorHistory.filter(v => v.paidDate.startsWith(mKey)).reduce((s, v) => s + v.amountEUR, 0) : 0;
      if (isPastMonth && sfActualsSplit[mKey]?.vendors > 0) {
        vendors = sfActualsSplit[mKey].vendors;
      } else if (isPastMonth && nsVendorActual > 0) {
        // NS actual vendor payments (used for non-SF subsidiaries like Statscore)
        vendors = nsVendorActual;
      } else if (sfBudget.totalByMonth[mKey]) {
        vendors = Math.round(sfBudget.totalByMonth[mKey].eur);
      } else if (nsBudget.byMonth[mKey]?.vendors) {
        vendors = Math.round(nsBudget.byMonth[mKey].vendors);
      } else if (expenseCategories.byMonth[mKey]) {
        vendors = Object.values(expenseCategories.byMonth[mKey] as Record<string, number>).reduce((s: number, v: number) => s + v, 0);
      } else {
        vendors = isCurMonth ? openBillsTotal : 0;
      }
      const vendorsBase = vendors; // base vendors BEFORE scenario adjustments

      // Apply per-category vendor adjustments from scenario
      if (!isPastMonth && Object.keys(vendorCatAdj).length > 0) {
        const effectiveVendorAdj: Record<string, number> = {};
        const allVendorAdjMonths = Object.keys(vendorCatAdj).filter(k => k <= mKey).sort();
        for (const adjM of allVendorAdjMonths) {
          for (const [cat, pct] of Object.entries(vendorCatAdj[adjM])) {
            if (pct !== 0) effectiveVendorAdj[cat] = pct;
            else delete effectiveVendorAdj[cat];
          }
        }
        if (Object.keys(effectiveVendorAdj).length > 0) {
          const catData = sfBudget.byMonth?.[mKey] || nsBudget.byMonth[mKey]?.categories || expenseCategories.byMonth?.[mKey] || {};
          let vendorDelta = 0;
          for (const [cat, pct] of Object.entries(effectiveVendorAdj)) {
            const catBudget = (catData as Record<string, number>)[cat] || 0;
            vendorDelta += Math.round(catBudget * (pct / 100));
          }
          vendors += vendorDelta;
        }
      }

      // Apply per-department vendor detail adjustments from scenario
      if (!isPastMonth && Object.keys(vendorDetailAdj).length > 0) {
        const effectiveDetailAdj: Record<string, { pct: number; base: number }> = {};
        const allDetailAdjMonths = Object.keys(vendorDetailAdj).filter(k => k <= mKey).sort();
        for (const adjM of allDetailAdjMonths) {
          for (const [key, val] of Object.entries(vendorDetailAdj[adjM])) {
            if (val.pct !== 0) effectiveDetailAdj[key] = val;
            else delete effectiveDetailAdj[key];
          }
        }
        let detailDelta = 0;
        for (const [, val] of Object.entries(effectiveDetailAdj)) {
          detailDelta += Math.round(val.base * (val.pct / 100));
        }
        if (detailDelta !== 0) vendors += detailDelta;
      }

      // ── INFLOWS: NS collections for past/current, SF REVENUE_AMOUNT_EUR for future ──
      // Unpaid carry only from past months where there's real paid data (not future where unpaid=revenue)
      const revPaid = sfRevenuePaid[mKey];
      const actualColl = (isCurMonth || isPastMonth) ? (actualCollections[mKey] || 0) : 0;
      const collPct = getCollPct(i);
      const collMultiplier = collPct / 100;
      let collections: number;
      let collectionsActual = 0;
      let collectionsRemaining = 0;
      let collectionsForecast = revPaid?.revenue || sfRevenue.budget?.[mKey]?.eur || nsBudget.byMonth[mKey]?.revenue || 0;
      let collectionsRevenue = revPaid?.revenue || nsBudget.byMonth[mKey]?.revenue || 0;
      let collectionsUnpaidCarry = prevMonthUnpaid; // real unpaid from past months only
      const prevD = new Date(d.getFullYear(), d.getMonth() - 1, 1);
      const collectionsUnpaidCarryMonth = prevMonthUnpaid > 0 ? `${prevD.getFullYear()}-${String(prevD.getMonth() + 1).padStart(2, '0')}` : '';
      const collectionsPipeline = (!isPastMonth && !isCurMonth) ? (pipelineByMonth[mKey] || 0) : 0;
      const customers = revPaid?.customers || 0;
      if (isPastMonth && actualColl > 0) {
        // Past: NS actual collections (cash received, incl I/C)
        collections = actualColl;
        collectionsActual = actualColl;
      } else if (isCurMonth && actualColl > 0) {
        // Current: NS actual so far + remaining projected × %
        collectionsActual = actualColl;
        collectionsRemaining = Math.max(0, Math.round(collectionsForecast * collMultiplier) - actualColl);
        collections = actualColl + collectionsRemaining + collectionsUnpaidCarry;
      } else if (collectionsRevenue > 0) {
        // Future: REVENUE_AMOUNT_EUR × collection% + unpaid carry + pipeline impact
        collections = Math.round(collectionsRevenue * collMultiplier) + collectionsUnpaidCarry + collectionsPipeline;
      } else if (collectionsForecast > 0) {
        collections = Math.round(collectionsForecast * collMultiplier) + collectionsPipeline;
      } else {
        collections = collectionsPipeline;
      }
      // Only carry forward unpaid from fully completed past months
      // A month is "complete" if it's past AND most of its revenue was collected (paid > 50% of revenue)
      // This avoids carrying forward months where paid is tiny (just started) or future (paid=null)
      if (isPastMonth && revPaid && revPaid.paid > 0 && revPaid.revenue > 0 && revPaid.paid / revPaid.revenue > 0.5) {
        prevMonthUnpaid = revPaid.unpaid || 0;
      } else {
        prevMonthUnpaid = 0;
      }
      // Low-conf pipeline for this month (not added to collections — shown separately)
      const pipelineLow = pipelineLowByMonth[mKey] || { weighted: 0, total: 0, count: 0, opps: [] as typeof lowConfPipeline };
      const pipelineWeighted = pipelineLow.weighted;
      const pipelineWeightedILS = Math.round(pipelineWeighted * eurIlsRatio);
      const pipelineTotal = pipelineLow.total;
      const pipelineCount = pipelineLow.count;
      const pipelineOpps = pipelineLow.opps;

      // Prorate current month when as-of date is mid-month
      const daysInMonth = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
      const prorateFactor = (asOfDate && isCurMonth) ? now.getDate() / daysInMonth : 1;
      if (prorateFactor < 1) {
        salary = Math.round(salary * prorateFactor);
        vendors = Math.round(vendors * prorateFactor);
        collections = Math.round(collections * prorateFactor);
      }

      const totalOutflow = salary + vendors;
      // Churn deduction: for future months, subtract 6-month rolling avg of monthly churn impact
      let churnDeduction = 0;
      if (!isPastMonth && !isCurMonth && churnMonthlyAvg > 0) {
        churnDeduction = churnMonthlyAvg;
      }
      const churnDeductionILS = Math.round(churnDeduction * eurIlsRatio);
      const net = collections + pipelineWeighted - totalOutflow - churnDeduction;
      const salaryILS = Math.round(salary * eurIlsRatio);
      const vendorsILS = Math.round(vendors * eurIlsRatio);
      const collectionsILS = Math.round(collections * eurIlsRatio);
      const totalOutflowILS = salaryILS + vendorsILS;
      const netILS = collectionsILS + pipelineWeightedILS - totalOutflowILS - churnDeductionILS;
      runningBalance += net;
      runningBalanceILS += netILS;

      // Revaluation impact from NS FxReval transactions
      const revalHasBothEnds = monthlyReval.byMonth?.[mKey]?.hasBothEnds || false;
      // Only apply reval when we have both beginning & end of month rates (complete month)
      const revalImpact = revalHasBothEnds ? (monthlyReval.byMonth?.[mKey]?.eur || 0) : 0;
      const revalImpactILS = revalHasBothEnds ? (monthlyReval.byMonth?.[mKey]?.ils || 0) : 0;
      runningBalance += revalImpact;
      runningBalanceILS += revalImpactILS;

      const openingBalanceILS = runningBalanceILS - netILS - revalImpactILS;
      rows.push({ month: label, mKey, openingBalance, openingBalanceILS, salary, salaryBase, salaryILS, vendors, vendorsBase, vendorsILS, totalOutflow, totalOutflowILS, collections, collectionsILS, collectionsActual, collectionsRemaining, collectionsForecast, collectionsRevenue, collectionsUnpaidCarry, collectionsUnpaidCarryMonth, collectionsPipeline, customers, pipelineWeighted, pipelineWeightedILS, pipelineTotal, pipelineCount, pipelineOpps, pipelineHistWinRate, pipelineDelayMonths, churnDeduction, churnDeductionILS, net, netILS, revalImpact, revalImpactILS, revalHasBothEnds, closingBalance: runningBalance, closingBalanceILS: runningBalanceILS, isCurrent: isCurMonth, isPast: isPastMonth });
    }
    return rows;
  }, [vendorBills, arForecast, salaryData, vendorHistory, expenseCategories, book, bookLocal, actualCollections, sfBudget, sfRevenue, sfActualsSplit, salaryAdjPctByMonth, collPctByMonth, monthlyReval, sfSalaryBudget, sfRevenuePaid, sfPipeline, pipelineMinProb, sfConversion, salaryDeptAdj, salaryDeptBudgets, vendorCatAdj, vendorDetailAdj, prevMonthEndBalance, churnMonthlyAvg, asOfDate, nsBudget, activeYear]);

  // ── Capture current-year cashflow for propagation to next year ──
  useEffect(() => {
    if (activeYear === currentYear && cashflowForecast?.length === 12 && activeCompany !== 'consolidated') {
      sourceYearCashflowRef.current[activeCompany] = cashflowForecast;
    }
  }, [cashflowForecast, activeYear, currentYear, activeCompany]);

  // ── Compute cashflow for any ScenarioData ──
  const computeScenarioCashflow = useCallback((cd: ScenarioData) => {
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const completedSalaries = salaryData.filter(s => s.month < currentMonth && s.amountEUR > 0);
    const lastSalary = completedSalaries.length > 0 ? completedSalaries[completedSalaries.length - 1].amountEUR : 0;
    const openBillsTotal = vendorBills.reduce((s, b) => s + b.amountEUR, 0);
    const eurIlsRatio = adjustedCurrent > 0 ? adjustedCurrentLocal / adjustedCurrent : 3.7;
    let runBal = (book?.openingBalance || 0) + (monthlyReval.preYear?.eur || 0);
    let prevSal = 0;
    let prevUnpaid = 0;
    const rows: { salary: number; vendors: number; collections: number; totalOutflow: number; net: number; closingBalance: number }[] = [];
    for (let mi = 0; mi < 12; mi++) {
      const d = new Date(activeYear, mi, 1);
      const mKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const isCur = activeYear === now.getFullYear() && mi === now.getMonth();
      const isPast = activeYear < now.getFullYear() || (activeYear === now.getFullYear() && mi < now.getMonth());
      // Anchor current month to actual previous month-end bank balance
      if (isCur && prevMonthEndBalance) { runBal = prevMonthEndBalance.eur; }
      const cAdj = cd.salaryAdjPctByMonth?.[mi] || 0;
      const cMult = 1 + (cAdj / 100);
      // Dept adj delta
      const cEffDept: Record<string, number> = {};
      const cAllAdjM = Object.keys(cd.salaryDeptAdj || {}).filter(k => k <= mKey).sort();
      for (const ak of cAllAdjM) { for (const [dep, p] of Object.entries(cd.salaryDeptAdj[ak])) { if (p !== 0) cEffDept[dep] = p; else delete cEffDept[dep]; } }
      let cDeptDelta = 0;
      if (Object.keys(cEffDept).length > 0 && salaryDeptBudgets[mKey]) {
        for (const [dep, p] of Object.entries(cEffDept)) { cDeptDelta += Math.round((salaryDeptBudgets[mKey][dep] || 0) * (p / 100)); }
      }
      let salary: number;
      const cActSal = salaryData.find(s => s.month === mKey);
      if ((isPast || isCur) && sfActualsSplit[mKey]?.salary > 0) { salary = sfActualsSplit[mKey].salary; }
      else if ((isPast || isCur) && cActSal && cActSal.amountEUR > 0) { salary = cActSal.amountEUR; }
      else if (sfSalaryBudget[mKey]?.eur > 0) { salary = Math.round(sfSalaryBudget[mKey].eur * cMult) + cDeptDelta; }
      else if (nsBudget.byMonth[mKey]?.salary > 0) { salary = Math.round(nsBudget.byMonth[mKey].salary * cMult); }
      else { salary = prevSal > 0 ? Math.round(prevSal * cMult) : lastSalary; }
      prevSal = salary;
      let vendors: number;
      const cNsVendAct = isPast ? vendorHistory.filter(v => v.paidDate.startsWith(mKey)).reduce((s, v) => s + v.amountEUR, 0) : 0;
      if (isPast && sfActualsSplit[mKey]?.vendors > 0) { vendors = sfActualsSplit[mKey].vendors; }
      else if (isPast && cNsVendAct > 0) { vendors = cNsVendAct; }
      else if (sfBudget.totalByMonth[mKey]) { vendors = Math.round(sfBudget.totalByMonth[mKey].eur); }
      else if (nsBudget.byMonth[mKey]?.vendors) { vendors = Math.round(nsBudget.byMonth[mKey].vendors); }
      else { vendors = isCur ? openBillsTotal : 0; }
      // Pipeline weighted for this month
      const cPipelineFiltered = sfPipeline.filter(o => o.probability >= (cd.pipelineMinProb ?? 100));
      const cPipeWeighted = cPipelineFiltered.reduce((s, o) => {
        const oMonth = o.closeDate?.slice(0, 7);
        return oMonth === mKey ? s + o.weighted : s;
      }, 0);
      const revPaid = sfRevenuePaid[mKey];
      const actualColl = (isCur || isPast) ? (actualCollections[mKey] || 0) : 0;
      const cCollPct = (cd.collPctByMonth?.[mi] ?? 100) / 100;
      const collForecast = revPaid?.revenue || sfRevenue.budget?.[mKey]?.eur || nsBudget.byMonth[mKey]?.revenue || 0;
      const collRev = revPaid?.revenue || 0;
      const unpaidCarry = prevUnpaid;
      let collections: number;
      if (isPast && actualColl > 0) { collections = actualColl; }
      else if (isCur && actualColl > 0) { collections = actualColl + Math.max(0, Math.round(collForecast * cCollPct) - actualColl) + unpaidCarry; }
      else if (collRev > 0) { collections = Math.round(collRev * cCollPct) + unpaidCarry; }
      else if (collForecast > 0) { collections = Math.round(collForecast * cCollPct); }
      else { collections = 0; }
      if (isPast && revPaid && revPaid.paid > 0 && revPaid.revenue > 0 && revPaid.paid / revPaid.revenue > 0.5) { prevUnpaid = revPaid.unpaid || 0; } else { prevUnpaid = 0; }
      const totalOutflow = salary + vendors;
      const net = collections + cPipeWeighted - totalOutflow;
      runBal += net;
      const revalImpact = (monthlyReval.byMonth?.[mKey]?.hasBothEnds) ? (monthlyReval.byMonth?.[mKey]?.eur || 0) : 0;
      runBal += revalImpact;
      rows.push({ salary, vendors, collections, totalOutflow, net, closingBalance: runBal });
    }
    return rows;
  }, [salaryData, vendorBills, book, bookLocal, adjustedCurrent, adjustedCurrentLocal, monthlyReval, sfActualsSplit, sfSalaryBudget, sfBudget, sfRevenue, sfRevenuePaid, actualCollections, salaryDeptBudgets, sfPipeline, prevMonthEndBalance]);

  // Compare scenario cashflow (inline header delta)
  const compareCashflow = useMemo(() => {
    if (!compareScenario) return null;
    return computeScenarioCashflow(compareScenario.data);
  }, [compareScenario, computeScenarioCashflow]);

  // ── Consolidated Cashflow Computation ──
  const consolidatedCashflow = useMemo(() => {
    if (!consolidatedData) return null;
    const cd = consolidatedData;
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

    // Get scenarios for each subsidiary
    const lsScenario = consLsScenarioId ? scenarios.find(s => s.id === consLsScenarioId)?.data : null;
    const stScenario = consStScenarioId ? scenarios.find(s => s.id === consStScenarioId)?.data : null;

    // Helper to compute cashflow for one subsidiary (subYear = that subsidiary's active year)
    const computeSubCashflow = (sub: 'lsports' | 'statscore', scenarioData: ScenarioData | null, subYear?: number) => {
      const activeYear = subYear || currentYear;
      const d = cd[sub];
      const hasSF = sub === 'lsports';
      const salaryArr: SalaryMonth[] = d.salary || [];
      const vendHist: VendorHistoryRecord[] = d.vendorHistory || [];
      const coll: Record<string, number> = d.collections || {};
      let nsBud = d.nsBudget || { byMonth: {} };
      const mReval = d.monthlyReval || { byMonth: {}, preYear: { eur: 0, ils: 0 } };
      let sfBud = hasSF ? (d.sfBudget || { totalByMonth: {} }) : { totalByMonth: {} } as any;
      const sfRev = hasSF ? (d.sfRevenue || {}) : {};
      const sfSplit = hasSF ? (d.sfActualsSplit || {}) : {};
      let sfSalBud = hasSF ? (d.sfSalaryBudget || {}) : {} as any;
      const sfRevPaid = hasSF ? (d.sfRevenuePaid || {}) : {};

      // ── Live propagation: override baselines from current-year cashflow ──
      const liveCf = sourceYearCashflowRef.current[sub];
      const hasLive = activeYear !== currentYear && liveCf?.length === 12;
      let liveOpenBal: number | undefined;
      if (hasLive) {
        liveOpenBal = liveCf[11].closingBalance;
        // Salary baseline: avg of last 3 months (Oct, Nov, Dec)
        const avgSal = Math.round((liveCf[9].salary + liveCf[10].salary + liveCf[11].salary) / 3);
        const liveSalBud: Record<string, { eur: number; ils: number }> = {};
        for (let m = 1; m <= 12; m++) liveSalBud[`${activeYear}-${String(m).padStart(2, '0')}`] = { eur: avgSal, ils: 0 };
        sfSalBud = liveSalBud;
        // Vendor baseline: avg of full year
        const avgVend = Math.round(liveCf.reduce((s: number, r: any) => s + r.vendors, 0) / 12);
        const liveVendTotal: Record<string, { eur: number; ils: number }> = {};
        for (let m = 1; m <= 12; m++) liveVendTotal[`${activeYear}-${String(m).padStart(2, '0')}`] = { eur: avgVend, ils: 0 };
        sfBud = { ...sfBud, totalByMonth: liveVendTotal };
        // Also override nsBudget for non-SF subsidiaries (Statscore)
        if (!hasSF) {
          const liveNsBud: Record<string, any> = {};
          for (let m = 1; m <= 12; m++) {
            const mk = `${activeYear}-${String(m).padStart(2, '0')}`;
            liveNsBud[mk] = { salary: avgSal, vendors: avgVend, revenue: nsBud.byMonth?.[mk]?.revenue || 0 };
          }
          nsBud = { byMonth: liveNsBud };
        }
      }

      const sc = scenarioData || { salaryAdjPctByMonth: {}, collPctByMonth: {}, salaryDeptAdj: {}, vendorCatAdj: {}, vendorDetailAdj: {}, leverOverrides: {}, pipelineMinProb: 100 };

      const completedSals = salaryArr.filter((s: any) => s.month < currentMonth && s.amountEUR > 0);
      const lastSal = completedSals.length > 0 ? completedSals[completedSals.length - 1].amountEUR : 0;

      let runBal = hasLive ? liveOpenBal! : ((d.bankBalance?.openingBalance || 0) + (mReval.preYear?.eur || 0));
      let prevSal = 0;
      let prevUnpaid = 0;
      const rows: { mKey: string; salary: number; vendors: number; collections: number; totalOutflow: number; net: number; revalImpact: number; closingBalance: number; openingBalance: number; isPast: boolean; isCurrent: boolean }[] = [];

      for (let mi = 0; mi < 12; mi++) {
        const dt = new Date(activeYear, mi, 1);
        const mKey = `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
        const isCur = activeYear === now.getFullYear() && mi === now.getMonth();
        const isPast = activeYear < now.getFullYear() || (activeYear === now.getFullYear() && mi < now.getMonth());

        const openingBalance = runBal;
        const cAdj = sc.salaryAdjPctByMonth?.[mi] || 0;
        const cMult = 1 + (cAdj / 100);

        // Salary
        let salary: number;
        const actSal = salaryArr.find((s: any) => s.month === mKey);
        if ((isPast || isCur) && sfSplit[mKey]?.salary > 0) { salary = sfSplit[mKey].salary; }
        else if ((isPast || isCur) && actSal && actSal.amountEUR > 0) { salary = actSal.amountEUR; }
        else if (sfSalBud[mKey]?.eur > 0) { salary = Math.round(sfSalBud[mKey].eur * cMult); }
        else if (nsBud.byMonth?.[mKey]?.salary > 0) { salary = Math.round(nsBud.byMonth[mKey].salary * cMult); }
        else { salary = prevSal > 0 ? Math.round(prevSal * cMult) : lastSal; }
        prevSal = salary;

        // Vendors
        let vendors: number;
        const nsVendAct = isPast ? vendHist.filter((v: any) => v.paidDate?.startsWith(mKey)).reduce((s: number, v: any) => s + (v.amountEUR || 0), 0) : 0;
        if (isPast && sfSplit[mKey]?.vendors > 0) { vendors = sfSplit[mKey].vendors; }
        else if (isPast && nsVendAct > 0) { vendors = nsVendAct; }
        else if (sfBud.totalByMonth?.[mKey]) { vendors = Math.round(sfBud.totalByMonth[mKey].eur || 0); }
        else if (nsBud.byMonth?.[mKey]?.vendors) { vendors = Math.round(nsBud.byMonth[mKey].vendors); }
        else { vendors = 0; }

        // Apply vendor category adjustments from scenario
        if (!isPast && sc.vendorCatAdj && Object.keys(sc.vendorCatAdj).length > 0) {
          const effVAdj: Record<string, number> = {};
          const allVM = Object.keys(sc.vendorCatAdj).filter(k => k <= mKey).sort();
          for (const ak of allVM) { for (const [cat, pct] of Object.entries(sc.vendorCatAdj[ak])) { if (pct !== 0) effVAdj[cat] = pct; else delete effVAdj[cat]; } }
          if (Object.keys(effVAdj).length > 0) {
            const catData = sfBud.byMonth?.[mKey] || nsBud.byMonth?.[mKey]?.categories || {};
            let vDelta = 0;
            for (const [cat, pct] of Object.entries(effVAdj)) { vDelta += Math.round(((catData as any)[cat] || 0) * (pct / 100)); }
            vendors += vDelta;
          }
        }

        // Collections
        const revPaid = sfRevPaid[mKey];
        const actualColl = (isCur || isPast) ? (coll[mKey] || 0) : 0;
        const cCollPct = (sc.collPctByMonth?.[mi] ?? 100) / 100;
        const collForecast = revPaid?.revenue || sfRev.budget?.[mKey]?.eur || nsBud.byMonth?.[mKey]?.revenue || 0;
        const collRev = revPaid?.revenue || nsBud.byMonth?.[mKey]?.revenue || 0;
        const unpaidCarry = prevUnpaid;
        let collections: number;
        if (isPast && actualColl > 0) { collections = actualColl; }
        else if (isCur && actualColl > 0) { collections = actualColl + Math.max(0, Math.round(collForecast * cCollPct) - actualColl) + unpaidCarry; }
        else if (collRev > 0) { collections = Math.round(collRev * cCollPct) + unpaidCarry; }
        else if (collForecast > 0) { collections = Math.round(collForecast * cCollPct); }
        else { collections = 0; }
        if (isPast && revPaid && revPaid.paid > 0 && revPaid.revenue > 0 && revPaid.paid / revPaid.revenue > 0.5) { prevUnpaid = revPaid.unpaid || 0; } else { prevUnpaid = 0; }

        const totalOutflow = salary + vendors;
        const net = collections - totalOutflow;
        runBal += net;

        const revalHasBoth = mReval.byMonth?.[mKey]?.hasBothEnds || false;
        const revalImpact = revalHasBoth ? (mReval.byMonth?.[mKey]?.eur || 0) : 0;
        runBal += revalImpact;

        rows.push({ mKey, salary, vendors, collections, totalOutflow, net, revalImpact, closingBalance: runBal, openingBalance, isPast, isCurrent: isCur });
      }
      return rows;
    };

    const lsRows = computeSubCashflow('lsports', lsScenario, activeYears.lsports || currentYear);
    const stRows = computeSubCashflow('statscore', stScenario, activeYears.statscore || currentYear);
    const elim = cd.elimination || { actualByMonth: {}, projectedByMonth: {} };

    // Merge: sum both subsidiaries, apply I/C elimination
    const merged: {
      month: string; mKey: string; openingBalance: number; salary: number; vendors: number; collections: number;
      totalOutflow: number; net: number; revalImpact: number; closingBalance: number;
      isPast: boolean; isCurrent: boolean;
      lsSalary: number; lsVendors: number; lsCollections: number; lsClosing: number;
      stSalary: number; stVendors: number; stCollections: number; stClosing: number;
      icRevenue: number; icExpense: number; icNet: number; icSource: string;
    }[] = [];

    const displayYear = Math.max(activeYears.lsports || currentYear, activeYears.statscore || currentYear);
    for (let mi = 0; mi < 12; mi++) {
      const ls = lsRows[mi];
      const st = stRows[mi];
      const dt = new Date(displayYear, mi, 1);
      const mKey = `${displayYear}-${String(mi + 1).padStart(2, '0')}`;
      const label = `${monthNames[dt.getMonth()]} ${dt.getFullYear()}`;
      const isPast = ls.isPast && st.isPast;
      const isCurrent = ls.isCurrent || st.isCurrent;

      // I/C elimination: use actuals for past months, projected for current/future
      let icRevenue = 0;
      let icExpense = 0;
      let icSource = 'none';
      if (elim.actualByMonth[mKey]) {
        icRevenue = elim.actualByMonth[mKey].revenue || 0;
        icExpense = elim.actualByMonth[mKey].expense || 0;
        icSource = 'actual';
      } else if (elim.projectedByMonth[mKey]) {
        icRevenue = elim.projectedByMonth[mKey].revenue || 0;
        icExpense = elim.projectedByMonth[mKey].expense || 0;
        icSource = 'projected';
      }
      // I/C net impact: revenue elimination reduces collections, expense elimination reduces vendors
      const icNet = -icRevenue + icExpense; // removing I/C revenue is negative, removing I/C expense is positive

      const salary = ls.salary + st.salary;
      const vendors = ls.vendors + st.vendors - icExpense; // I/C expense elimination reduces vendors
      const collections = ls.collections + st.collections - icRevenue; // I/C revenue elimination reduces collections
      const totalOutflow = salary + (ls.vendors + st.vendors - icExpense);
      const revalImpact = ls.revalImpact + st.revalImpact;
      const net = collections - totalOutflow + revalImpact;
      const openingBalance = mi === 0 ? (ls.openingBalance + st.openingBalance) : merged[mi - 1].closingBalance;
      const closingBalance = openingBalance + collections - totalOutflow + revalImpact;

      merged.push({
        month: label, mKey, openingBalance, salary, vendors, collections, totalOutflow,
        net, revalImpact, closingBalance, isPast, isCurrent,
        lsSalary: ls.salary, lsVendors: ls.vendors, lsCollections: ls.collections, lsClosing: ls.closingBalance,
        stSalary: st.salary, stVendors: st.vendors, stCollections: st.collections, stClosing: st.closingBalance,
        icRevenue, icExpense, icNet, icSource,
      });
    }

    return merged;
  }, [consolidatedData, scenarios, consLsScenarioId, consStScenarioId, activeYears]);

  const [showReval, setShowReval] = useState(true);

  // ── AI Chat State ──
  const [chatOpen, setChatOpen] = useState(false);
  const [chatMessages, setChatMessages] = useState<{ role: 'user' | 'assistant'; content: string }[]>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [chatId, setChatId] = useState<string>(() => Date.now().toString(36) + Math.random().toString(36).slice(2, 6));
  const [chatHistoryList, setChatHistoryList] = useState<{ id: string; title: string; messages: any[]; updatedAt: string }[]>([]);
  const [chatShowHistory, setChatShowHistory] = useState(false);

  // Load chat history list from server
  useEffect(() => {
    fetch('/api/chat-history').then(r => r.json()).then(setChatHistoryList).catch(() => {});
  }, []);

  // Auto-save current chat to server after each AI reply
  const saveChatToServer = useCallback((msgs: { role: string; content: string }[], id: string) => {
    if (msgs.length === 0) return;
    const firstUserMsg = msgs.find(m => m.role === 'user');
    const title = firstUserMsg ? firstUserMsg.content.slice(0, 60) : 'New Chat';
    fetch('/api/chat-history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'save', id, title, messages: msgs }),
    }).then(r => r.json()).then(() => {
      fetch('/api/chat-history').then(r => r.json()).then(setChatHistoryList).catch(() => {});
    }).catch(() => {});
  }, []);

  const loadChat = useCallback((chat: { id: string; messages: any[] }) => {
    setChatId(chat.id);
    setChatMessages(chat.messages);
    setChatShowHistory(false);
  }, []);

  const startNewChat = useCallback(() => {
    setChatId(Date.now().toString(36) + Math.random().toString(36).slice(2, 6));
    setChatMessages([]);
    setChatShowHistory(false);
  }, []);

  const deleteChat = useCallback((id: string) => {
    fetch('/api/chat-history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'delete', id }),
    }).then(() => {
      setChatHistoryList(prev => prev.filter(h => h.id !== id));
      if (chatId === id) startNewChat();
    }).catch(() => {});
  }, [chatId, startNewChat]);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<HTMLInputElement>(null);
  const chatPanelInputRef = useRef<HTMLInputElement>(null);
  const chatFileInputRef = useRef<HTMLInputElement>(null);
  const [chatAttachment, setChatAttachment] = useState<{ name: string; content: string } | null>(null);

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [chatMessages]);
  useEffect(() => { if (chatOpen) chatInputRef.current?.focus(); }, [chatOpen]);

  const [showBankBreakdown, setShowBankBreakdown] = useState(false);
  const [bankBreakdownAsOf, setBankBreakdownAsOf] = useState<{ date: string; label: string; accounts: BankAccount[] | 'loading' } | null>(null);
  const [forecastDrilldown, setForecastDrilldown] = useState<{ type: 'vendors' | 'salary' | 'inflows' | 'pipeline'; month: string; mKey: string; data: any; categoryData?: Record<string, number>; categoryName?: string; adjPct?: number } | null>(null);
  const [wonOppsDrilldown, setWonOppsDrilldown] = useState<{ year: number; type: 'new' | 'upgrades'; data: any[] | 'loading' } | null>(null);
  const totalPrimaryBalance = bankAccounts.reduce((s, a) => s + a.primaryBalance, 0);
  const totalLocalBalance = bankAccounts.reduce((s, a) => s + a.localBalance, 0);
  const revalDiffEUR = adjustedCurrent - (book?.currentBalance || 0);
  const revalDiffILS = adjustedCurrentLocal - (bookLocal?.currentBalance || 0);

  // Current EUR/ILS rate from adjusted (revalued) balances — used for per-account revaluation
  const currentEurIlsRate = adjustedCurrent > 0 ? adjustedCurrentLocal / adjustedCurrent : 3.7;
  // Per-account revaluation based on account's native currency:
  // EUR accounts: EUR is real → revalue ILS = EUR × rate
  // Non-EUR accounts: ILS is real → revalue EUR = ILS / rate
  const accountReval = (a: BankAccount, isEur: boolean): { revalEUR: number; revaluedEUR: number; revalILS: number; revaluedILS: number } => {
    if (isEur) {
      // EUR is native — keep EUR, revalue ILS
      const revaluedILS = Math.round(a.primaryBalance * currentEurIlsRate);
      return { revalEUR: 0, revaluedEUR: a.primaryBalance, revalILS: revaluedILS - a.localBalance, revaluedILS };
    }
    if (a.localBalance === 0 && a.primaryBalance === 0) return { revalEUR: 0, revaluedEUR: 0, revalILS: 0, revaluedILS: 0 };
    // Non-EUR: ILS is native — keep ILS, revalue EUR
    // Guard: if no local currency rate available (e.g. Statscore has no ILS), use primaryBalance as-is
    if (currentEurIlsRate === 0 || a.localBalance === 0) return { revalEUR: 0, revaluedEUR: a.primaryBalance, revalILS: 0, revaluedILS: a.localBalance };
    const revaluedEUR = Math.round(a.localBalance / currentEurIlsRate);
    return { revalEUR: revaluedEUR - a.primaryBalance, revaluedEUR, revalILS: 0, revaluedILS: a.localBalance };
  };

  // Compute revalued totals using the SAME per-account accountReval() that subcategory headers use
  // This ensures Grand Total = sum of all subcategory totals (no mismatch)
  const revaluedTotalEUR = bankAccounts.reduce((s, a) => {
    const n = a.name.toUpperCase();
    const isEur = n.includes('EUR') || n.includes('EURO');
    return s + accountReval(a, isEur).revaluedEUR;
  }, 0);
  const revaluedTotalILS = bankAccounts.reduce((s, a) => {
    const n = a.name.toUpperCase();
    const isEur = n.includes('EUR') || n.includes('EURO');
    return s + accountReval(a, isEur).revaluedILS;
  }, 0);
  const displayTotalEUR = showReval ? revaluedTotalEUR : totalPrimaryBalance;
  const displayTotalILS = showReval ? revaluedTotalILS : totalLocalBalance;
  // Override adjustedCurrent to match account-list-based total so the box reconciles with Grand Total
  const reconciledAdjEUR = revaluedTotalEUR;
  const reconciledAdjILS = revaluedTotalILS;

  // Categorize bank accounts
  type AccountCategory = { name: string; icon: string; color: string; currencies: { currency: string; accounts: BankAccount[]; totalEUR: number; totalILS: number }[]; totalEUR: number; totalILS: number };
  const categorizedAccounts = useMemo((): AccountCategory[] => {
    const creditCardKeywords = ['AMEX', 'MasterCard', 'Isracard', 'Visa'];
    const bankKeywords = ['Bank HPoalim', 'Bank Poalim', 'Bank Leumi', 'Bank Dicount', 'Bank Discount', 'PKO'];
    const paypalKeywords = ['PAYPAL', 'PayPal'];
    const depositKeywords = ['Deposit'];
    const cryptoKeywords = ['Crypto'];

    const detectCategory = (name: string): string => {
      if (creditCardKeywords.some(k => name.includes(k))) return 'Credit Cards';
      if (bankKeywords.some(k => name.includes(k))) return 'Banks';
      if (paypalKeywords.some(k => name.includes(k))) return 'PayPal';
      if (depositKeywords.some(k => name.includes(k))) return 'Deposits';
      if (cryptoKeywords.some(k => name.includes(k))) return 'Crypto';
      return 'Other';
    };

    const detectCurrency = (name: string): string => {
      const n = name.toUpperCase();
      if (n.includes('EUR') || n.includes('EURO')) return 'EUR';
      if (n.includes('USD') || n.includes('US ')) return 'USD';
      if (n.includes('NIS') || n.includes('ILS') || n.includes(' IL ') || n.includes(' IL-')) return 'ILS';
      if (n.includes('GBP')) return 'GBP';
      if (n.includes('CAD')) return 'CAD';
      if (n.includes('CNY')) return 'CNY';
      if (n.includes('PLN')) return 'PLN';
      return 'Other';
    };

    const categoryMap: Record<string, Record<string, BankAccount[]>> = {};
    for (const a of bankAccounts) {
      const cat = detectCategory(a.name);
      const cur = detectCurrency(a.name);
      if (!categoryMap[cat]) categoryMap[cat] = {};
      if (!categoryMap[cat][cur]) categoryMap[cat][cur] = [];
      categoryMap[cat][cur].push(a);
    }

    const categoryOrder = ['Banks', 'Credit Cards', 'PayPal', 'Deposits', 'Crypto', 'Other'];
    const currencyOrder = ['EUR', 'USD', 'ILS', 'GBP', 'CAD', 'CNY', 'PLN', 'Other'];
    const categoryColors: Record<string, { icon: string; color: string }> = {
      'Banks': { icon: '🏦', color: 'emerald' },
      'Credit Cards': { icon: '💳', color: 'violet' },
      'PayPal': { icon: '🅿️', color: 'blue' },
      'Deposits': { icon: '🔒', color: 'amber' },
      'Crypto': { icon: '₿', color: 'orange' },
      'Other': { icon: '📋', color: 'gray' },
    };

    return categoryOrder
      .filter(cat => categoryMap[cat])
      .map(cat => {
        const currencies = currencyOrder
          .filter(cur => categoryMap[cat]?.[cur])
          .map(cur => {
            const accounts = categoryMap[cat][cur];
            return {
              currency: cur,
              accounts,
              totalEUR: accounts.reduce((s, a) => s + a.primaryBalance, 0),
              totalILS: accounts.reduce((s, a) => s + a.localBalance, 0),
            };
          });
        return {
          name: cat,
          ...categoryColors[cat] || { icon: '📋', color: 'gray' },
          currencies,
          totalEUR: currencies.reduce((s, c) => s + c.totalEUR, 0),
          totalILS: currencies.reduce((s, c) => s + c.totalILS, 0),
        };
      });
  }, [bankAccounts]);

  // Collapsible state for categories
  const [expandedCategories, setExpandedCategories] = useState<Record<string, boolean>>({});
  const toggleCategory = (key: string) => setExpandedCategories(prev => ({ ...prev, [key]: !prev[key] }));

  // ── AI Chat Callbacks (must be after displayTotalEUR, categorizedAccounts, cashflowForecast) ──
  const buildDashboardContext = useCallback(() => {
    const lines: string[] = [];
    const companyLabel = activeCompany === 'lsports' ? 'LSports' : activeCompany === 'statscore' ? 'Statscore' : 'Consolidated (LSports + Statscore)';
    lines.push(`ACTIVE VIEW: ${companyLabel}`);
    lines.push(`Date: ${asOfDate || new Date().toISOString().slice(0, 10)} (${asOfDate ? 'as-of historical' : 'live'})`);

    // Consolidated view context
    if (activeCompany === 'consolidated' && consolidatedCashflow && consolidatedCashflow.length > 0) {
      lines.push(`\nCONSOLIDATED CASHFLOW FORECAST (LSports + Statscore combined, EUR only):`);
      for (const r of consolidatedCashflow) {
        lines.push(`  ${r.month}: Collections €${r.collections.toLocaleString()} | Salary -€${Math.abs(r.salary).toLocaleString()} | Vendors -€${Math.abs(r.vendors).toLocaleString()} | Net €${r.net.toLocaleString()} | Closing €${r.closingBalance.toLocaleString()} (LS €${r.lsClosing.toLocaleString()} + SC €${r.stClosing.toLocaleString()})${r.isPast ? ' [actual]' : r.isCurrent ? ' [current]' : ' [forecast]'}`);
      }
      lines.push(`  I/C Elimination: Revenue €${consolidatedCashflow.reduce((s, r) => s + r.icRevenue, 0).toLocaleString()} | Expense €${consolidatedCashflow.reduce((s, r) => s + r.icExpense, 0).toLocaleString()}`);
    }

    if (bankAccounts.length > 0) {
      lines.push(`\nBANK BALANCES (${bankAccounts.length} accounts):`);
      lines.push(`  Grand Total EUR: €${displayTotalEUR.toLocaleString()}`);
      lines.push(`  Grand Total ILS: ₪${displayTotalILS.toLocaleString()}`);
      for (const cat of categorizedAccounts) {
        lines.push(`  ${cat.name}: €${cat.totalEUR.toLocaleString()}`);
      }
    }
    if (cashflowForecast.length > 0) {
      lines.push(`\nCASHFLOW FORECAST (monthly):`);
      for (const r of cashflowForecast) {
        lines.push(`  ${r.month}: Collections €${r.collections.toLocaleString()} | Salary -€${Math.abs(r.salary).toLocaleString()} | Vendors -€${Math.abs(r.vendors).toLocaleString()} | Net €${r.net.toLocaleString()} | Closing €${r.closingBalance.toLocaleString()}${r.isPast ? ' (actual)' : r.isCurrent ? ' (current)' : ' (forecast)'}`);
      }
    }
    // Department-level salary budgets
    const deptMonths = Object.keys(salaryDeptBudgets).sort();
    if (deptMonths.length > 0) {
      lines.push(`\nSALARY BY DEPARTMENT (monthly budget EUR):`);
      // Collect all departments
      const allDepts = new Set<string>();
      for (const m of deptMonths) for (const d of Object.keys(salaryDeptBudgets[m])) allDepts.add(d);
      // Show a recent month as representative
      const recentMonth = deptMonths[Math.min(deptMonths.length - 1, 3)]; // ~April or latest
      if (salaryDeptBudgets[recentMonth]) {
        lines.push(`  Sample month (${recentMonth}):`);
        const sorted = Object.entries(salaryDeptBudgets[recentMonth]).sort((a, b) => b[1] - a[1]);
        for (const [dept, amt] of sorted) {
          if (amt > 0) lines.push(`    ${dept}: €${Math.round(amt).toLocaleString()}`);
        }
        lines.push(`  Total departments: ${sorted.filter(([,a]) => a > 0).length}`);
      }
    }
    // Vendor budget by month and category breakdown
    const vendorMonths = Object.keys(sfBudget.totalByMonth || {}).sort();
    if (vendorMonths.length > 0) {
      lines.push(`\nVENDOR BUDGET BY MONTH:`);
      for (const m of vendorMonths) {
        const v = sfBudget.totalByMonth[m];
        if (v?.eur) lines.push(`  ${m}: €${Math.round(v.eur).toLocaleString()}`);
      }
    } else if (Object.keys(nsBudget.byMonth).length > 0) {
      lines.push(`\nVENDOR BUDGET BY MONTH (NS):`);
      for (const [m, v] of Object.entries(nsBudget.byMonth).sort(([a], [b]) => a.localeCompare(b))) {
        if ((v as any).vendors) lines.push(`  ${m}: €${Math.round((v as any).vendors).toLocaleString()}`);
      }
    }
    // Vendor category breakdown (sample month)
    const catMonths = Object.keys(sfBudget.byMonth || nsBudget.byMonth || expenseCategories.byMonth || {}).sort();
    if (catMonths.length > 0) {
      const sampleMonth = catMonths[Math.min(catMonths.length - 1, 4)]; // ~May or latest
      const catData = (sfBudget.byMonth?.[sampleMonth] || nsBudget.byMonth[sampleMonth]?.categories || expenseCategories.byMonth?.[sampleMonth] || {}) as Record<string, number>;
      if (Object.keys(catData).length > 0) {
        lines.push(`\nVENDOR EXPENSES BY CATEGORY (sample ${sampleMonth}):`);
        const sorted = Object.entries(catData).sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));
        for (const [cat, amt] of sorted) {
          if (Math.abs(amt) > 0) lines.push(`  ${cat}: €${Math.round(amt).toLocaleString()}`);
        }
        lines.push(`  Total categories: ${sorted.filter(([,a]) => Math.abs(a) > 0).length}`);
      }
    }
    if (yoyRevenue) {
      lines.push(`\nYoY REVENUE (Snowflake):`);
      lines.push(`  Current year (${yoyRevenue.currentYear}): €${yoyRevenue.currentYearRev?.toLocaleString()}`);
      lines.push(`  Prior year (${yoyRevenue.priorYear}): €${yoyRevenue.priorYearRev?.toLocaleString()}`);
    }
    // Include active scenario data so AI can adjust existing scenarios
    if (activeScenarioId) {
      const activeS = scenarios.find(s => s.id === activeScenarioId);
      if (activeS) {
        lines.push(`\nACTIVE SCENARIO: "${activeS.name}"`);
        lines.push(`  Current adjustments: ${JSON.stringify(activeS.data)}`);
        lines.push(`  (When asked to adjust, build on these values — don't start from scratch)`);
      }
    }
    // Include current live adjustments even if no named scenario
    if (hasAnyAdjustments) {
      const curData = getCurrentScenarioData();
      const activeAdjs: string[] = [];
      if (Object.values(curData.salaryDeptAdj).some(m => Object.keys(m).length > 0)) activeAdjs.push(`salaryDeptAdj: ${JSON.stringify(curData.salaryDeptAdj)}`);
      if (Object.values(curData.vendorCatAdj).some(m => Object.keys(m).length > 0)) activeAdjs.push(`vendorCatAdj: ${JSON.stringify(curData.vendorCatAdj)}`);
      if (Object.keys(curData.vendorDetailAdj).length > 0) activeAdjs.push(`vendorDetailAdj: ${JSON.stringify(curData.vendorDetailAdj)}`);
      if (activeAdjs.length > 0) {
        lines.push(`\nCURRENT LIVE ADJUSTMENTS (may differ from saved scenario):`);
        for (const a of activeAdjs) lines.push(`  ${a}`);
      }
    }
    return lines.join('\n');
  }, [activeCompany, consolidatedCashflow, asOfDate, bankAccounts, displayTotalEUR, displayTotalILS, categorizedAccounts, cashflowForecast, yoyRevenue, salaryDeptBudgets, sfBudget, activeScenarioId, scenarios, hasAnyAdjustments, getCurrentScenarioData]);

  const sendChatMessage = useCallback(async () => {
    // Read from whichever input has text (header or panel)
    const headerVal = chatInputRef.current?.value?.trim() || '';
    const panelVal = chatPanelInputRef.current?.value?.trim() || '';
    const text = headerVal || panelVal;
    if (!text || chatLoading) return;
    // Clear both inputs immediately
    if (chatInputRef.current) chatInputRef.current.value = '';
    if (chatPanelInputRef.current) chatPanelInputRef.current.value = '';
    // Include file attachment if present
    const fullText = chatAttachment
      ? `${text}\n\n📎 Attached file: **${chatAttachment.name}**\n\`\`\`\n${chatAttachment.content}\n\`\`\``
      : text;
    setChatAttachment(null);
    const userMsg = { role: 'user' as const, content: fullText };
    const newMessages = [...chatMessages, userMsg];
    setChatMessages(newMessages);
    setChatLoading(true);
    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: newMessages,
          dashboardContext: buildDashboardContext(),
        }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      let reply = data.reply as string;
      // Check for scenario JSON block and auto-save (lenient: triple or single backticks, or just ```scenario)
      const scenarioMatch = reply.match(/`{1,3}scenario\s*\n?([\s\S]*?)\n?`{1,3}/);
      if (scenarioMatch) {
        try {
          const scenarioData = JSON.parse(scenarioMatch[1]);
          const name = scenarioData.name || `AI Scenario ${new Date().toLocaleTimeString()}`;
          // Convert string keys to numbers for month-indexed objects
          const numericObj = (obj: any) => { const r: Record<number, number> = {}; for (const [k, v] of Object.entries(obj || {})) r[Number(k)] = Number(v); return r; };
          // Convert salaryDeptAdj: { "2026-05": { "R&D": -15 } } format
          const parseDeptAdj = (obj: any): Record<string, Record<string, number>> => {
            const r: Record<string, Record<string, number>> = {};
            for (const [month, depts] of Object.entries(obj || {})) {
              r[month] = {};
              for (const [dept, pct] of Object.entries(depts as Record<string, number>)) {
                r[month][dept] = Number(pct);
              }
            }
            return r;
          };
          const sData: ScenarioData = {
            salaryAdjPctByMonth: numericObj(scenarioData.salaryAdjPctByMonth),
            collPctByMonth: numericObj(scenarioData.collPctByMonth),
            salaryDeptAdj: parseDeptAdj(scenarioData.salaryDeptAdj),
            vendorCatAdj: parseDeptAdj(scenarioData.vendorCatAdj),
            leverOverrides: scenarioData.leverOverrides || {},
            pipelineMinProb: scenarioData.pipelineMinProb ?? 100,
          };
          const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
          const now2 = new Date().toISOString();
          setScenarios(prev => [...prev, { id, name, createdAt: now2, updatedAt: now2, data: sData }]);
          // Remove the raw JSON block from visible reply, add confirmation
          reply = reply.replace(/`{1,3}scenario\s*\n?[\s\S]*?\n?`{1,3}/, '').trim();
          reply += `\n\n✅ **Scenario "${name}" saved!** You can find it in the Scenarios dropdown.`;
        } catch (e2) { /* ignore parse errors */ }
      }
      const finalMessages = [...newMessages, { role: 'assistant' as const, content: reply }];
      setChatMessages(finalMessages);
      saveChatToServer(finalMessages, chatId);
    } catch (e: any) {
      const finalMessages = [...newMessages, { role: 'assistant' as const, content: `⚠️ Error: ${e.message}` }];
      setChatMessages(finalMessages);
      saveChatToServer(finalMessages, chatId);
    } finally {
      setChatLoading(false);
    }
  }, [chatLoading, chatMessages, buildDashboardContext, chatId, saveChatToServer]);

  // Currency-aware formatters
  const fmtC = (eur: number, ils: number) => currency === 'EUR' ? fmt(eur) : fmtILS(ils);
  const fmtCFull = (eur: number, ils: number) => currency === 'EUR' ? fmtFull(eur) : fmtILS(ils);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b shadow-sm sticky top-0 z-40">
        <div className="max-w-[1400px] mx-auto px-4 py-5 flex items-center gap-4">
          {/* Left: Title */}
          <div className="flex-shrink-0">
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
              <Landmark className="w-7 h-7 text-emerald-500" />
              Banks Dashboard
            </h1>
            <div className="flex items-center gap-1 mt-1">
              <span className="text-[10px] text-gray-400">
                {bankAccounts.length > 0 && <>{bankAccounts.length} Accounts</>}
                {lastRefreshed && <> • {lastRefreshed}</>}
                {typeof __GIT_HASH__ !== 'undefined' && <> • <span className="font-mono">{__GIT_HASH__}</span></>}
              </span>
            </div>
          </div>

          {/* Center: Company Switcher */}
          <div className="flex-1 flex justify-center">
            <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
              {(['lsports', 'statscore', 'consolidated'] as CompanyView[]).map(co => (
                <button key={co} onClick={() => setActiveCompany(co)}
                  className={`text-sm px-5 py-2 rounded-lg font-semibold transition-all ${activeCompany === co ? 'bg-emerald-500 text-white shadow-md' : 'text-gray-500 hover:bg-white hover:text-gray-700 hover:shadow-sm'}`}>
                  {co === 'lsports' ? 'LSports' : co === 'statscore' ? 'Statscore' : 'Consolidated'}
                </button>
              ))}
            </div>
          </div>

          {/* Year Selector — per-company or consolidated badge */}
          <div className="flex items-center gap-1.5 flex-shrink-0">
            {activeCompany !== 'consolidated' ? (<>
              <select
                value={activeYears[activeCompany] || currentYear}
                onChange={e => setActiveYears(prev => ({ ...prev, [activeCompany]: parseInt(e.target.value) }))}
                className="text-sm font-semibold border border-gray-200 rounded-lg px-2 py-1.5 bg-white text-gray-700 focus:ring-2 focus:ring-emerald-300 focus:border-emerald-400"
              >
                {(availableYearsByCompany[activeCompany] || [currentYear]).map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
              {(activeYears[activeCompany] || currentYear) === currentYear && !(availableYearsByCompany[activeCompany] || []).includes(currentYear + 1) && (
                <button
                  disabled={isRollingForward}
                  onClick={async () => {
                    const co = activeCompany;
                    const coLabel = co === 'lsports' ? 'LSports' : 'Statscore';
                    const srcYear = activeYears[co] || currentYear;
                    const nextYear = srcYear + 1;
                    if (!confirm(`Roll forward ${coLabel} ${srcYear} budget to ${nextYear}?\n\nThis will snapshot current ${coLabel} ${srcYear} data as the starting point for ${nextYear} planning.`)) return;
                    setIsRollingForward(true);
                    try {
                      const resp = await fetch('/api/budget-snapshot', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ sourceYear: srcYear, targetYear: nextYear, company: co, clientDecClosing: cashflowForecast?.[11]?.closingBalance }),
                      });
                      const result = await resp.json();
                      if (result.success) {
                        setAvailableYearsByCompany(prev => ({ ...prev, [co]: [...new Set([...(prev[co] || [currentYear]), nextYear])].sort() }));
                        setActiveYears(prev => ({ ...prev, [co]: nextYear }));
                      } else {
                        alert('Roll forward failed: ' + (result.error || 'Unknown error'));
                      }
                    } catch (e: any) { alert('Roll forward failed: ' + e.message); }
                    setIsRollingForward(false);
                  }}
                  className="text-[10px] text-blue-600 hover:text-blue-800 bg-blue-50 border border-blue-200 rounded-lg px-2 py-1.5 font-medium transition-colors whitespace-nowrap"
                  title={`Copy ${activeCompany === 'lsports' ? 'LSports' : 'Statscore'} ${activeYear} data to ${activeYear + 1} as budget baseline`}
                >
                  {isRollingForward ? 'Rolling...' : `→ ${activeYear + 1}`}
                </button>
              )}
              {(activeYears[activeCompany] || currentYear) !== currentYear && (<>
                <span className="text-[10px] px-2 py-1 rounded-full bg-amber-100 text-amber-700 font-medium">DRAFT</span>
                <button
                  disabled={isRollingForward}
                  onClick={async () => {
                    const co = activeCompany;
                    const yr = activeYears[co];
                    const coLabel = co === 'lsports' ? 'LSports' : 'Statscore';
                    if (!confirm(`Refresh ${coLabel} ${yr} from latest ${currentYear} data?\n\nThis will re-snapshot ${currentYear} actuals + projections into ${yr}.`)) return;
                    setIsRollingForward(true);
                    try {
                      const resp = await fetch('/api/budget-snapshot', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ sourceYear: currentYear, targetYear: yr, company: co }),
                      });
                      const result = await resp.json();
                      if (result.success) {
                        // Force re-fetch with updated snapshot
                        delete companyDataCache.current[co];
                        setConsolidatedData(null);
                        fetchData(undefined, true);
                      } else {
                        alert('Refresh failed: ' + (result.error || 'Unknown error'));
                      }
                    } catch (e: any) { alert('Refresh failed: ' + e.message); }
                    setIsRollingForward(false);
                  }}
                  className="text-[10px] text-blue-600 hover:text-blue-800 bg-blue-50 border border-blue-200 rounded-lg px-2 py-1 font-medium transition-colors whitespace-nowrap"
                  title={`Re-snapshot ${currentYear} data into ${activeYear}`}
                >
                  {isRollingForward ? 'Refreshing...' : `↻ from ${currentYear}`}
                </button>
                <button
                  onClick={async () => {
                    const co = activeCompany;
                    const yr = activeYears[co];
                    const coLabel = co === 'lsports' ? 'LSports' : 'Statscore';
                    if (!confirm(`Delete ${coLabel} ${yr} snapshot?\n\nThis cannot be undone. You'll go back to ${currentYear} live data.`)) return;
                    try {
                      await fetch(`/api/budget-snapshot?year=${yr}&company=${co}`, { method: 'DELETE' });
                      setAvailableYearsByCompany(prev => ({ ...prev, [co]: (prev[co] || []).filter(y => y !== yr) }));
                      setActiveYears(prev => ({ ...prev, [co]: currentYear }));
                    } catch (e: any) { alert('Delete failed: ' + e.message); }
                  }}
                  className="text-[10px] text-red-500 hover:text-red-700 bg-red-50 border border-red-200 rounded-lg px-1.5 py-1 font-medium transition-colors"
                  title={`Delete ${activeCompany === 'lsports' ? 'LSports' : 'Statscore'} ${activeYear} snapshot`}
                >
                  ✕
                </button>
              </>)}
            </>) : (
              <div className="flex items-center gap-2 text-[11px] font-medium">
                <span className={`px-2 py-1 rounded-lg ${(activeYears.lsports || currentYear) !== currentYear ? 'bg-amber-50 text-amber-700 border border-amber-200' : 'bg-gray-100 text-gray-600'}`}>
                  LS: {activeYears.lsports || currentYear}
                </span>
                <span className={`px-2 py-1 rounded-lg ${(activeYears.statscore || currentYear) !== currentYear ? 'bg-amber-50 text-amber-700 border border-amber-200' : 'bg-gray-100 text-gray-600'}`}>
                  ST: {activeYears.statscore || currentYear}
                </span>
              </div>
            )}
          </div>

          {/* Right: AI Chat Input */}
          <div className="flex-1 flex justify-end">
            <div className="flex items-center gap-1.5 border rounded-xl px-3 py-1.5 bg-white border-gray-300 hover:border-emerald-400 focus-within:border-emerald-500 focus-within:ring-2 focus-within:ring-emerald-200 transition-all w-full max-w-[480px]">
              <Sparkles className="w-4 h-4 text-emerald-500 flex-shrink-0" />
              {chatAttachment && (
                <span className="flex items-center gap-1 bg-emerald-50 text-emerald-700 text-[10px] px-1.5 py-0.5 rounded-full flex-shrink-0">
                  <Paperclip className="w-3 h-3" />{chatAttachment.name.length > 15 ? chatAttachment.name.slice(0, 12) + '...' : chatAttachment.name}
                  <button onClick={() => setChatAttachment(null)} className="hover:text-red-500"><X className="w-3 h-3" /></button>
                </span>
              )}
              <input
                ref={chatInputRef}
                type="text"
                defaultValue=""
                onKeyDown={e => {
                  if (e.key === 'Enter' && (e.target as HTMLInputElement).value.trim()) { sendChatMessage(); }
                  if (e.key === 'Escape') { setChatOpen(false); if (chatInputRef.current) chatInputRef.current.value = ''; }
                }}
                onFocus={() => setChatOpen(true)}
                placeholder={chatAttachment ? "Ask about the file..." : "Ask AI about your data..."}
                className="text-sm bg-transparent border-none outline-none flex-1 py-0.5 placeholder-gray-400"
              />
              <input ref={chatFileInputRef} type="file" accept=".txt,.csv,.json,.md,.xlsx,.xls,.tsv" className="hidden" onChange={async (e) => {
                const file = e.target.files?.[0];
                if (!file) return;
                if (file.size > 2 * 1024 * 1024) { alert('File too large (max 2MB)'); return; }
                if (file.name.match(/\.xlsx?$/i)) {
                  const buf = await file.arrayBuffer();
                  const wb = XLSX.read(buf, { type: 'array' });
                  const rows: string[] = [];
                  for (const name of wb.SheetNames) {
                    rows.push(`== Sheet: ${name} ==`);
                    const csv = XLSX.utils.sheet_to_csv(wb.Sheets[name]);
                    rows.push(csv);
                  }
                  setChatAttachment({ name: file.name, content: rows.join('\n') });
                } else {
                  const text = await file.text();
                  setChatAttachment({ name: file.name, content: text });
                }
                e.target.value = '';
                setChatOpen(true);
              }} />
              <button onClick={() => chatFileInputRef.current?.click()} title="Attach file"
                className="text-gray-400 hover:text-emerald-600 flex-shrink-0">
                <Paperclip className="w-3.5 h-3.5" />
              </button>
              <button onClick={sendChatMessage} disabled={chatLoading}
                className="text-emerald-600 hover:text-emerald-800 disabled:opacity-40 flex-shrink-0">
                {chatLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Send className="w-3.5 h-3.5" />}
              </button>
            </div>
          </div>

          {/* Right: Date + Refresh */}
          <div className="flex items-center gap-2 flex-shrink-0">
            <div className="flex items-center gap-1">
              <input type="text"
                     value={asOfDateRaw || (asOfDate ? new Date(asOfDate + 'T12:00:00').toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' }) : '')}
                     placeholder="dd/mm/yyyy"
                     onChange={e => {
                       const v = e.target.value.replace(/[^0-9/]/g, '');
                       setAsOfDateRaw(v);
                       const digits = v.replace(/\//g, '');
                       let d = 0, m = 0, y = 0;
                       if (digits.length === 6) { d = parseInt(digits.slice(0,2)); m = parseInt(digits.slice(2,4)); y = parseInt('20' + digits.slice(4,6)); }
                       else if (digits.length === 8) { d = parseInt(digits.slice(0,2)); m = parseInt(digits.slice(2,4)); y = parseInt(digits.slice(4,8)); }
                       else {
                         const parts = v.split('/');
                         if (parts.length === 3) { d = parseInt(parts[0]); m = parseInt(parts[1]); y = parseInt(parts[2]); if (y < 100) y += 2000; }
                       }
                       if (d >= 1 && d <= 31 && m >= 1 && m <= 12 && y >= 2020 && y <= 2099) {
                         setAsOfDate(`${y}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}`);
                         setAsOfDateRaw('');
                       } else if (v === '') { setAsOfDate(''); }
                     }}
                     onBlur={() => setAsOfDateRaw('')}
                     onKeyDown={e => { if (e.key === 'Escape') { setAsOfDate(''); setAsOfDateRaw(''); } }}
                     className={`text-sm border rounded-lg px-2 py-1.5 w-28 text-center ${asOfDate ? 'border-amber-400 bg-amber-50 text-amber-800' : 'border-gray-300 text-gray-500'}`}
                     title="Type date: 010126 = 01/01/2026" />
              {asOfDate && (
                <button onClick={() => setAsOfDate('')}
                        className="text-[10px] text-amber-600 hover:text-amber-800 bg-amber-100 rounded px-1.5 py-1 font-medium">
                  Live
                </button>
              )}
            </div>
            <button onClick={() => fetchData(undefined, true)} disabled={isLoading}
                    className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm shadow-sm ${isLoading ? 'bg-gray-300 text-gray-500' : 'bg-emerald-600 text-white hover:bg-emerald-700'}`}>
              <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} /> {isLoading ? 'Loading...' : 'Refresh'}
            </button>
          </div>
        </div>
      </div>

      {asOfDate && (
        <div className="bg-amber-50 border-b border-amber-200 px-4 py-2 text-center">
          <span className="text-amber-800 font-medium text-sm">
            Historical View — showing cashflow as of {new Date(asOfDate + 'T12:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })}
          </span>
          <button onClick={() => setAsOfDate('')} className="ml-3 text-xs text-amber-600 hover:text-amber-800 underline">Back to live</button>
        </div>
      )}

      <div className="max-w-[1400px] mx-auto px-4 py-6 space-y-6">

        {isLoading && (activeCompany === 'consolidated' ? !consolidatedData : (activeYears[activeCompany] || currentYear) === currentYear ? bankData.dailyBalances.length === 0 : !cashflowForecast?.length) && (
          <div className="flex items-center justify-center py-16 gap-3">
            <Loader2 className="w-6 h-6 animate-spin text-emerald-500" />
            <span className="text-gray-500">{activeCompany === 'consolidated' ? 'Loading consolidated data from LSports + Statscore...' : 'Loading bank data from NetSuite...'}</span>
          </div>
        )}

        {/* ── Bank Balance Charts ── */}
        {activeCompany !== 'consolidated' && (bankData.dailyBalances.length > 0 || (activeYears[activeCompany] || currentYear) !== currentYear) && (
          <div className="bg-gradient-to-r from-emerald-50 to-teal-50 rounded-xl shadow-sm border border-emerald-200 p-5">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-emerald-700 uppercase tracking-wider flex items-center gap-2">
                <DollarSign className="w-4 h-4" /> Bank Balance
              </h2>
              {bankData.revaluation && bankData.revaluation.estimatedMissing > 0 && (activeYears[activeCompany] || currentYear) === currentYear && (
                <div className="flex items-center gap-2 bg-amber-50 border border-amber-300 rounded-lg px-3 py-1.5 text-xs">
                  <AlertTriangle className="w-3.5 h-3.5 text-amber-500" />
                  <span className="text-amber-700 font-medium">FX Revaluation pending</span>
                  <span className="text-amber-600">
                    Last reval: {bankData.revaluation.lastRevalDate ? new Date(bankData.revaluation.lastRevalDate).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }) : 'N/A'}
                    ({fmt(bankData.revaluation.lastRevalImpact)})
                  </span>
                  <span className="text-amber-700 font-semibold">Est. adjusted balance: {fmt(reconciledAdjEUR)}</span>
                </div>
              )}
            </div>

            <div className={`grid grid-cols-1 ${bookLocal?.dailyBalances?.length > 0 && (activeYears[activeCompany] || currentYear) === currentYear ? 'lg:grid-cols-2' : ''} gap-4`}>
              {/* Primary Book (EUR) */}
              <div className="bg-white rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium text-gray-700">{(activeYears[activeCompany] || currentYear) !== currentYear ? 'Projected Balance (EUR)' : ((book as any).label || 'Primary Book (EUR)')}</span>
                  <div className="text-right">
                    <span className="text-lg font-bold text-emerald-700">{(activeYears[activeCompany] || currentYear) !== currentYear && cashflowForecast?.length ? fmtFull(cashflowForecast[11]?.closingBalance || 0) : fmtFull(book.currentBalance)}</span>
                    {hasAdjusted && (activeYears[activeCompany] || currentYear) === currentYear && <div className="text-xs text-amber-600">Adj. (est. reval): {fmtFull(reconciledAdjEUR)}</div>}
                  </div>
                </div>
                <ResponsiveContainer width="100%" height={120}>
                  {(() => {
                    // Build monthly end-of-month balances for all 12 months
                    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                    const isSnapshotYear = (activeYears[activeCompany] || currentYear) !== currentYear;
                    const monthlyData = monthNames.map((name, mi) => {
                      if (isSnapshotYear && cashflowForecast) {
                        // Use cashflow forecast closing balances for snapshot years
                        const row = cashflowForecast[mi];
                        return { month: name, balance: row ? row.closingBalance : null, adjustedBalance: row ? row.closingBalance : null };
                      }
                      const mKey = `${activeYear}-${String(mi + 1).padStart(2, '0')}`;
                      // Find last daily balance in this month
                      const monthBalances = book.dailyBalances.filter((d: any) => d.date && d.date.substring(0, 7) === mKey);
                      const last = monthBalances.length > 0 ? monthBalances[monthBalances.length - 1] : null;
                      return {
                        month: name,
                        balance: last ? (last.balance || 0) : null,
                        adjustedBalance: last ? (last.adjustedBalance || last.balance || 0) : null,
                      };
                    });
                    const dataKey = hasAdjusted && !isSnapshotYear ? 'adjustedBalance' : 'balance';
                    return (
                      <BarChart data={monthlyData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis dataKey="month" tick={{ fontSize: 9 }} />
                        <YAxis tick={{ fontSize: 9 }} tickFormatter={(v: number) => v >= 1000000 ? `€${(v/1000000).toFixed(1)}M` : `€${(v/1000).toFixed(0)}k`} />
                        <Tooltip formatter={(v: any, name: string) => [v != null ? fmtFull(v as number) : 'No data', name === 'adjustedBalance' ? 'Adjusted Balance' : 'Balance']} />
                        <Bar dataKey={dataKey} radius={[2, 2, 0, 0]}>
                          {monthlyData.map((d, i) => (
                            <Cell key={i} fill={d[dataKey] == null ? '#e5e7eb' : (d[dataKey] as number) >= 0 ? '#10b981' : '#ef4444'} />
                          ))}
                        </Bar>
                      </BarChart>
                    );
                  })()}
                </ResponsiveContainer>
                <div className="flex items-center justify-between text-xs text-emerald-600 mt-1">
                  <span>Opening: {fmtFull(book.openingBalance)}</span>
                  {hasAdjusted && !((activeYears[activeCompany] || currentYear) !== currentYear) && <span className="text-amber-500">■ Estimated reval adjustment</span>}
                  {(activeYears[activeCompany] || currentYear) !== currentYear && cashflowForecast?.length ? (
                    <span>Dec Closing: {fmtFull(cashflowForecast[11]?.closingBalance || 0)}</span>
                  ) : (
                    <span>YTD: {fmtFull(reconciledAdjEUR - book.openingBalance)} ({book.openingBalance !== 0 ? ((reconciledAdjEUR - book.openingBalance) / Math.abs(book.openingBalance) * 100).toFixed(1) : '0'}%)</span>
                  )}
                </div>
              </div>

              {/* Local Book (ILS) — hide for snapshot years */}
              {bookLocal && bookLocal.dailyBalances && bookLocal.dailyBalances.length > 0 && (activeYears[activeCompany] || currentYear) === currentYear && (
                <div className="bg-white rounded-lg p-4">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium text-gray-700">{bookLocal.label || 'Local Book (ILS)'}</span>
                    <div className="text-right">
                      <span className="text-lg font-bold text-blue-700">{fmtILS(bookLocal.currentBalance)}</span>
                      {hasAdjustedLocal && <div className="text-xs text-amber-600">Adj. (est. reval): {fmtILS(reconciledAdjILS)}</div>}
                    </div>
                  </div>
                  <ResponsiveContainer width="100%" height={120}>
                    {(() => {
                      const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                      const monthlyData = monthNames.map((name, mi) => {
                        const mKey = `${activeYear}-${String(mi + 1).padStart(2, '0')}`;
                        const monthBalances = bookLocal.dailyBalances.filter((d: any) => d.date && d.date.substring(0, 7) === mKey);
                        const last = monthBalances.length > 0 ? monthBalances[monthBalances.length - 1] : null;
                        return {
                          month: name,
                          balance: last ? (last.balance || 0) : null,
                          adjustedBalance: last ? (last.adjustedBalance || last.balance || 0) : null,
                        };
                      });
                      const dataKey = hasAdjustedLocal ? 'adjustedBalance' : 'balance';
                      return (
                        <BarChart data={monthlyData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#dbeafe" />
                          <XAxis dataKey="month" tick={{ fontSize: 9 }} />
                          <YAxis tick={{ fontSize: 9 }} tickFormatter={(v: number) => Math.abs(v) >= 1000000 ? `₪${(v/1000000).toFixed(1)}M` : `₪${(v/1000).toFixed(0)}k`} />
                          <Tooltip formatter={(v: any, name: string) => [v != null ? fmtILS(v as number) : 'No data', name === 'adjustedBalance' ? 'Adjusted Balance' : 'Balance']} />
                          <Bar dataKey={dataKey} radius={[2, 2, 0, 0]}>
                            {monthlyData.map((d, i) => (
                              <Cell key={i} fill={d[dataKey] == null ? '#dbeafe' : (d[dataKey] as number) >= 0 ? '#3b82f6' : '#ef4444'} />
                            ))}
                          </Bar>
                        </BarChart>
                      );
                    })()}
                  </ResponsiveContainer>
                  <div className="flex items-center justify-between text-xs text-blue-600 mt-1">
                    <span>Opening: {fmtILS(bookLocal.openingBalance)}</span>
                    {hasAdjustedLocal && <span className="text-amber-500">■ Estimated reval adjustment</span>}
                    <span>YTD: {fmtILS(reconciledAdjILS - bookLocal.openingBalance)} ({bookLocal.openingBalance !== 0 ? ((reconciledAdjILS - bookLocal.openingBalance) / Math.abs(bookLocal.openingBalance) * 100).toFixed(1) : '0'}%)</span>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* ── Bank Account List — Categorized ── */}
        {activeCompany !== 'consolidated' && categorizedAccounts.length > 0 && (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider flex items-center gap-2">
                <Building2 className="w-4 h-4" /> Bank Account Balances
              </h2>
              <div className="flex items-center gap-3">
                {/* Reval toggle */}
                {(revalDiffEUR !== 0 || revalDiffILS !== 0) && (
                  <div className="flex bg-gray-100 rounded-lg p-0.5">
                    <button onClick={() => setShowReval(false)}
                            className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${!showReval ? 'bg-white shadow-sm text-gray-700' : 'text-gray-500 hover:text-gray-700'}`}>
                      Book
                    </button>
                    <button onClick={() => setShowReval(true)}
                            className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${showReval ? 'bg-white shadow-sm text-amber-700' : 'text-gray-500 hover:text-gray-700'}`}>
                      + Reval
                    </button>
                  </div>
                )}
                {/* Currency labels */}
                {displayTotalILS !== 0 && (
                  <div className="flex items-center gap-1 text-[10px] text-gray-400">
                    <span className="text-emerald-600 font-medium">EUR</span> / <span className="text-blue-600 font-medium">ILS</span>
                  </div>
                )}
              </div>
            </div>

            {/* Grand Total */}
            <div className="bg-gradient-to-r from-gray-50 to-gray-100 rounded-xl border border-gray-200 p-4 flex items-center justify-between">
              <div>
                <span className="font-semibold text-gray-700">Grand Total ({bankAccounts.length} accounts)</span>
                {showReval && (revalDiffEUR !== 0 || revalDiffILS !== 0) && (
                  <span className="ml-2 text-xs text-amber-600 font-medium">incl. est. reval</span>
                )}
              </div>
              <div className="text-right flex items-baseline gap-4">
                <div className={`text-xl font-bold ${displayTotalEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                  {fmt(displayTotalEUR)}
                </div>
                {displayTotalILS !== 0 && (
                  <div className={`text-base font-semibold ${displayTotalILS >= 0 ? 'text-blue-700' : 'text-red-600'}`}>
                    {fmtILS(displayTotalILS)}
                  </div>
                )}
              </div>
            </div>

            {/* Reval adjustment line removed — monthly reval impact shown in cashflow table only when both beginning & end entries exist */}

            {/* Category rows — collapsed by default, click to expand */}
            {categorizedAccounts.map(cat => {
              const catKey = `cat-${cat.name}`;
              const isCatExpanded = expandedCategories[catKey];
              return (
                <div key={cat.name} className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
                  {/* Category header — clickable to expand */}
                  <div className="flex items-center justify-between px-5 py-3.5 cursor-pointer hover:bg-gray-50 transition-colors"
                       onClick={() => toggleCategory(catKey)}>
                    <div className="flex items-center gap-2">
                      {isCatExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
                      <span className="text-lg">{cat.icon}</span>
                      <span className="font-semibold text-gray-800">{cat.name}</span>
                      <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full">
                        {cat.currencies.reduce((s, c) => s + c.accounts.length, 0)} accounts
                      </span>
                    </div>
                    {(() => {
                      const adjCatEUR = showReval ? cat.currencies.reduce((s, c) => {
                        const isEur = c.currency === 'EUR';
                        return s + c.accounts.reduce((s2, a) => s2 + accountReval(a, isEur).revaluedEUR, 0);
                      }, 0) : cat.totalEUR;
                      const adjCatILS = showReval ? cat.currencies.reduce((s, c) => {
                        const isEur = c.currency === 'EUR';
                        return s + c.accounts.reduce((s2, a) => s2 + accountReval(a, isEur).revaluedILS, 0);
                      }, 0) : cat.totalILS;
                      return (
                        <div className="flex items-baseline gap-3">
                          <span className={`font-bold text-lg ${adjCatEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(adjCatEUR)}</span>
                          {displayTotalILS !== 0 && <span className={`font-semibold text-sm ${adjCatILS >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(adjCatILS)}</span>}
                        </div>
                      );
                    })()}
                  </div>

                  {/* Expanded: currency groups */}
                  {isCatExpanded && (
                    <div className="border-t border-gray-100 divide-y divide-gray-50">
                      {cat.currencies.map(cur => {
                        const curKey = `${cat.name}-${cur.currency}`;
                        const isCurExpanded = expandedCategories[curKey];
                        return (
                          <div key={curKey}>
                            <div className="flex items-center justify-between px-5 py-2.5 pl-10 cursor-pointer hover:bg-gray-50"
                                 onClick={() => toggleCategory(curKey)}>
                              <div className="flex items-center gap-2">
                                {isCurExpanded ? <ChevronDown className="w-3.5 h-3.5 text-gray-400" /> : <ChevronRight className="w-3.5 h-3.5 text-gray-400" />}
                                <span className="text-sm font-medium text-gray-700">{cur.currency}</span>
                                <span className="text-xs text-gray-400">{cur.accounts.length} accounts</span>
                                {showReval && cur.currency !== 'EUR' && cur.accounts.some(a => accountReval(a, false).revalEUR !== 0) && (
                                  <span className="text-[10px] text-amber-600 font-medium">incl. reval</span>
                                )}
                              </div>
                              {(() => {
                                const isEur = cur.currency === 'EUR';
                                const adjEUR = showReval
                                  ? cur.accounts.reduce((s, a) => s + accountReval(a, isEur).revaluedEUR, 0)
                                  : cur.totalEUR;
                                const adjILS = showReval
                                  ? cur.accounts.reduce((s, a) => s + accountReval(a, isEur).revaluedILS, 0)
                                  : cur.totalILS;
                                return (
                                  <div className="flex items-baseline gap-3">
                                    <span className={`text-sm font-medium ${adjEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(adjEUR)}</span>
                                    <span className={`text-xs font-medium ${adjILS >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(adjILS)}</span>
                                  </div>
                                );
                              })()}
                            </div>

                            {isCurExpanded && (
                              <div className="px-5 pl-14 pb-3">
                                <table className="w-full text-xs">
                                  <thead>
                                    <tr className="text-left text-gray-400 uppercase border-b border-gray-100">
                                      <th className="pb-1 pr-3">Account</th>
                                      <th className="pb-1 pr-3">Account #</th>
                                      <th className="pb-1 pr-3 text-right">EUR</th>
                                      <th className="pb-1 pr-3 text-right">ILS</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {cur.accounts.map((a, i) => {
                                      const isEur = cur.currency === 'EUR';
                                      const rv = showReval ? accountReval(a, isEur) : { revalEUR: 0, revaluedEUR: a.primaryBalance, revalILS: 0, revaluedILS: a.localBalance };
                                      return (
                                        <tr key={i} className="border-b border-gray-50">
                                          <td className="py-1.5 pr-3 text-gray-700">{a.name}</td>
                                          <td className="py-1.5 pr-3 text-gray-400">{a.number || '-'}</td>
                                          <td className={`py-1.5 pr-3 text-right font-medium ${rv.revaluedEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                                            {fmt(rv.revaluedEUR)}
                                            {rv.revalEUR !== 0 && <div className="text-[9px] text-amber-600">book {fmt(a.primaryBalance)} {rv.revalEUR >= 0 ? '+' : ''}{fmt(rv.revalEUR)} reval</div>}
                                          </td>
                                          <td className={`py-1.5 pr-3 text-right font-medium ${rv.revaluedILS >= 0 ? 'text-blue-700' : 'text-red-600'}`}>
                                            {fmtILS(rv.revaluedILS)}
                                            {rv.revalILS !== 0 && <div className="text-[9px] text-amber-600">book {fmtILS(a.localBalance)} {rv.revalILS >= 0 ? '+' : ''}{fmtILS(rv.revalILS)} reval</div>}
                                          </td>
                                        </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* ── Cashflow Charts ── */}
        {activeCompany !== 'consolidated' && cashflowForecast.length > 0 && (() => {
          const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
          const chartData = cashflowForecast.map(r => {
            const mi = parseInt(r.mKey.substring(5)) - 1;
            return {
              name: monthNames[mi] || r.mKey,
              salaryBudget: Math.round((sfSalaryBudget[r.mKey]?.eur || nsBudget.byMonth[r.mKey]?.salary || 0) / 1000),
              salaryActual: Math.round((sfActualsSplit[r.mKey]?.salary || (salaryData.find(s => s.month === r.mKey)?.amountEUR) || 0) / 1000),
              vendorBudget: Math.round((sfBudget.totalByMonth[r.mKey]?.eur || nsBudget.byMonth[r.mKey]?.vendors || 0) / 1000),
              vendorActual: Math.round((sfActualsSplit[r.mKey]?.vendors || vendorHistory.filter(v => v.paidDate.startsWith(r.mKey)).reduce((s, v) => s + v.amountEUR, 0) || 0) / 1000),
              closing: Math.round(r.closingBalance / 1000),
              opening: Math.round(r.openingBalance / 1000),
              net: Math.round(r.net / 1000),
              inflows: Math.round((r.collections + r.pipelineWeighted) / 1000),
              outflows: Math.round(r.totalOutflow / 1000),
              revBudget: Math.round((sfRevenue.budget?.[r.mKey]?.eur || nsBudget.byMonth[r.mKey]?.revenue || 0) / 1000),
              revActual: Math.round((sfRevenuePaid[r.mKey]?.revenue || 0) / 1000),
              revVariance: (() => { const act = Math.round((sfRevenuePaid[r.mKey]?.revenue || 0) / 1000); const bud = Math.round((sfRevenue.budget?.[r.mKey]?.eur || nsBudget.byMonth[r.mKey]?.revenue || 0) / 1000); return act > 0 && bud > 0 ? act - bud : undefined; })(),
              isPast: r.isPast,
              isCurrent: r.isCurrent,
            };
          });
          const futureMonths = cashflowForecast.filter(r => !r.isPast && !r.isCurrent);
          const avgMonthlyNet = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.net, 0) / futureMonths.length) : 0;
          const avgBurn = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.totalOutflow, 0) / futureMonths.length) : 0;
          const currentBalance = cashflowForecast.find(r => r.isCurrent)?.closingBalance || cashflowForecast[cashflowForecast.length - 1]?.closingBalance || 0;
          const runwayMonths = avgBurn > 0 ? Math.round(currentBalance / avgBurn * 10) / 10 : 0;
          const ytdInflows = cashflowForecast.filter(r => r.isPast || r.isCurrent).reduce((s, r) => s + r.collections, 0);
          const ytdOutflows = cashflowForecast.filter(r => r.isPast || r.isCurrent).reduce((s, r) => s + r.totalOutflow, 0);
          const renderLabel = (props: any) => {
            const { x, y, width, value } = props;
            if (!value || value === 0) return null;
            return <text x={x + width / 2} y={y - 4} fill="#666" fontSize={9} textAnchor="middle">{value}K</text>;
          };
          const avgSalary = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.salary, 0) / futureMonths.length) : 0;
          const avgVendors = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.vendors, 0) / futureMonths.length) : 0;
          const avgInflows = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.collections, 0) / futureMonths.length) : 0;
          const avgPipeline = futureMonths.length > 0 ? Math.round(futureMonths.reduce((s, r) => s + r.pipelineWeighted, 0) / futureMonths.length) : 0;
          const kpiAsOfLabel = asOfDate ? new Date(asOfDate + 'T12:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '';
          return (
          <div className="space-y-4">
            {/* KPI Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
                <div className="flex items-center justify-between mb-1">
                  <p className="text-[10px] text-gray-400 uppercase">Avg Monthly Net</p>
                  {asOfDate && <span className="text-[9px] px-1 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">as of {kpiAsOfLabel}</span>}
                </div>
                <p className={`text-lg font-bold ${avgMonthlyNet >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(avgMonthlyNet)}</p>
                <p className="text-[10px] text-gray-400">projected {futureMonths.length}m avg</p>
                <div className="mt-3 pt-3 border-t border-gray-100 text-[11px] space-y-1">
                  <div className="flex justify-between"><span className="text-gray-500">Avg Inflows</span><span className="text-green-600">{fmt(avgInflows)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">Avg Pipeline</span><span className="text-teal-600">{fmt(avgPipeline)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">Avg Salary</span><span className="text-red-500">-{fmt(avgSalary)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">Avg Vendors</span><span className="text-red-500">-{fmt(avgVendors)}</span></div>
                  <div className="flex justify-between border-t border-gray-100 pt-1 font-semibold"><span className="text-gray-600">Net</span><span className={avgMonthlyNet >= 0 ? 'text-emerald-700' : 'text-red-600'}>{fmt(avgMonthlyNet)}</span></div>
                  <p className="text-[10px] text-gray-400 pt-1">Based on {futureMonths.length} projected months (May–Dec)</p>
                </div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
                <div className="flex items-center justify-between mb-1">
                  <p className="text-[10px] text-gray-400 uppercase">Avg Monthly Burn</p>
                  {asOfDate && <span className="text-[9px] px-1 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">as of {kpiAsOfLabel}</span>}
                </div>
                <p className="text-lg font-bold text-red-600">{fmt(avgBurn)}</p>
                <p className="text-[10px] text-gray-400">salary + vendors</p>
                <div className="mt-3 pt-3 border-t border-gray-100 text-[11px] space-y-1">
                  <div className="flex justify-between"><span className="text-gray-500">Avg Salary</span><span className="text-amber-600">{fmt(avgSalary)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">Avg Vendors</span><span className="text-violet-600">{fmt(avgVendors)}</span></div>
                  <div className="flex justify-between border-t border-gray-100 pt-1 font-semibold"><span className="text-gray-600">Total Burn</span><span className="text-red-600">{fmt(avgBurn)}</span></div>
                  <p className="text-[10px] text-gray-400 pt-1">Salary is {avgBurn > 0 ? Math.round(avgSalary / avgBurn * 100) : 0}% of total burn</p>
                </div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
                <div className="flex items-center justify-between mb-1">
                  <p className="text-[10px] text-gray-400 uppercase">Cash Runway</p>
                  {asOfDate && <span className="text-[9px] px-1 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">as of {kpiAsOfLabel}</span>}
                </div>
                <p className="text-lg font-bold text-blue-700">{runwayMonths}m</p>
                <p className="text-[10px] text-gray-400">at current burn rate</p>
                <div className="mt-3 pt-3 border-t border-gray-100 text-[11px] space-y-1">
                  <div className="flex justify-between"><span className="text-gray-500">Current Balance</span><span className="text-blue-700 font-semibold">{fmt(currentBalance)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">÷ Avg Monthly Burn</span><span className="text-red-600">{fmt(avgBurn)}</span></div>
                  <div className="flex justify-between border-t border-gray-100 pt-1 font-semibold"><span className="text-gray-600">= Runway</span><span className="text-blue-700">{runwayMonths} months</span></div>
                  <p className="text-[10px] text-gray-400 pt-1">Burn-only runway (excludes inflows). With net cash flow: {avgMonthlyNet < 0 ? Math.round(currentBalance / Math.abs(avgMonthlyNet) * 10) / 10 + 'm' : '∞ (net positive)'}</p>
                </div>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
                <div className="flex items-center justify-between mb-1">
                  <p className="text-[10px] text-gray-400 uppercase">YTD Net Cash</p>
                  {asOfDate && <span className="text-[9px] px-1 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">as of {kpiAsOfLabel}</span>}
                </div>
                <p className={`text-lg font-bold ${ytdInflows - ytdOutflows >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(ytdInflows - ytdOutflows)}</p>
                <p className="text-[10px] text-gray-400">in {fmt(ytdInflows)} / out {fmt(ytdOutflows)}</p>
                <div className="mt-3 pt-3 border-t border-gray-100 text-[11px] space-y-1">
                  <div className="flex justify-between"><span className="text-gray-500">YTD Inflows</span><span className="text-green-600">{fmt(ytdInflows)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">YTD Salary</span><span className="text-red-500">-{fmt(cashflowForecast.filter(r => r.isPast || r.isCurrent).reduce((s, r) => s + r.salary, 0))}</span></div>
                  <div className="flex justify-between"><span className="text-gray-500">YTD Vendors</span><span className="text-red-500">-{fmt(cashflowForecast.filter(r => r.isPast || r.isCurrent).reduce((s, r) => s + r.vendors, 0))}</span></div>
                  <div className="flex justify-between border-t border-gray-100 pt-1 font-semibold"><span className="text-gray-600">YTD Net</span><span className={ytdInflows - ytdOutflows >= 0 ? 'text-emerald-700' : 'text-red-600'}>{fmt(ytdInflows - ytdOutflows)}</span></div>
                </div>
              </div>
            </div>

            {/* OKR Cards */}
            {(() => {
              // OKR 1: YoY Revenue Growth — target 18%
              // Current year YTD: use cashflow collections (past+current months) — same source as the table
              const currentYearYTD = cashflowForecast.filter(r => r.isPast || r.isCurrent).reduce((s, r) => s + r.collections, 0);
              const priorYearYTDRaw = yoyRevenue?.priorYearPaid || yoyRevenue?.priorYearRev || 0;
              // Prorate prior year by same factor when as-of date is mid-month
              const refDate = asOfDate ? new Date(asOfDate + 'T12:00:00') : new Date();
              const asOfDay = refDate.getDate();
              const asOfDaysInMonth = new Date(refDate.getFullYear(), refDate.getMonth() + 1, 0).getDate();
              const midMonth = asOfDate && asOfDay < asOfDaysInMonth;
              const priorYearYTD = midMonth
                ? Math.round(priorYearYTDRaw - (priorYearYTDRaw / (yoyRevenue?.throughMonth || 1)) * (1 - asOfDay / asOfDaysInMonth))
                : priorYearYTDRaw;
              const yoyGrowthPct = priorYearYTD > 0
                ? Math.round((currentYearYTD - priorYearYTD) / priorYearYTD * 1000) / 10
                : null;
              const yoyTarget = 18; // same for both companies
              const yoyOnTrack = yoyGrowthPct !== null && yoyGrowthPct >= yoyTarget;
              const yoyProgress = yoyGrowthPct !== null ? Math.min(100, Math.max(0, (yoyGrowthPct / yoyTarget) * 100)) : 0;

              // Projected full-year revenue: sum all collections from cashflow forecast
              const projectedFullYearRev = cashflowForecast.reduce((s, r) => s + r.collections, 0);
              // Prior year full-year extrapolation: priorYTD / months * 12
              const throughMonth = yoyRevenue?.throughMonth || (cashflowForecast.filter(r => r.isPast || r.isCurrent).length || 1);
              const priorFullYearEst = priorYearYTD > 0 && throughMonth > 0
                ? Math.round(priorYearYTD / throughMonth * 12)
                : 0;
              const projectedGrowthPct = priorFullYearEst > 0
                ? Math.round((projectedFullYearRev - priorFullYearEst) / priorFullYearEst * 1000) / 10
                : null;
              const projectedOnTrack = projectedGrowthPct !== null && projectedGrowthPct >= yoyTarget;

              // OKR 2: Net Cash Growth — per-company target
              const netCashTarget = activeCompany === 'consolidated' ? 9500000 : (companyConfig.hasSF ? 8500000 : 1000000); // Consolidated €9.5M, LSports €8.5M, Statscore €1M
              const janOpening = cashflowForecast.length > 0 ? cashflowForecast[0].openingBalance : 0;
              // Use ACTUAL bank balance (revalued Grand Total) as the authoritative current balance
              // instead of cashflow forecast closing balance which includes pipeline/forecast items
              const pastOrCurrent = cashflowForecast.filter(r => r.isPast || r.isCurrent);
              const latestClosing = displayTotalEUR > 0 ? displayTotalEUR : (pastOrCurrent.length > 0 ? pastOrCurrent[pastOrCurrent.length - 1].closingBalance : janOpening);
              const netCashGrowth = latestClosing - janOpening;
              const netCashOnTrack = netCashGrowth >= netCashTarget;
              const netCashProgress = Math.min(100, Math.max(0, (netCashGrowth / netCashTarget) * 100));

              // Projected year-end: Dec closing from forecast
              const decClosing = cashflowForecast.length > 0 ? cashflowForecast[cashflowForecast.length - 1].closingBalance : janOpening;
              const projNetCashGrowth = decClosing - janOpening;
              const projNetCashOnTrack = projNetCashGrowth >= netCashTarget;
              const projNetCashProgress = Math.min(100, Math.max(0, (projNetCashGrowth / netCashTarget) * 100));

              const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
              const pastCurrentMonths = cashflowForecast.filter(r => r.isPast || r.isCurrent);
              const throughLabel = pastCurrentMonths.length > 0 ? monthNames[pastCurrentMonths.length - 1] : (yoyRevenue ? monthNames[(yoyRevenue.throughMonth || 1) - 1] : '');
              const asOfLabel = asOfDate ? new Date(asOfDate + 'T12:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '';

              return (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {/* OKR: YoY Revenue Growth */}
                <div className={`rounded-xl shadow-sm border p-4 ${yoyOnTrack ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200'}`}>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">OKR</span>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${yoyOnTrack ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'}`}>{yoyOnTrack ? 'On Track' : 'Behind'}</span>
                    {asOfDate && <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-700 font-medium ml-auto">as of {asOfLabel}</span>}
                  </div>
                  <p className="text-xs font-medium text-gray-700 mb-2">Increase YoY Revenue (cash collection) by 18%</p>
                  <div className="flex items-end gap-3 mb-2">
                    <p className={`text-2xl font-bold ${yoyOnTrack ? 'text-emerald-700' : 'text-amber-700'}`}>{yoyGrowthPct !== null ? `${yoyGrowthPct > 0 ? '+' : ''}${yoyGrowthPct}%` : '—'}</p>
                    <p className="text-xs text-gray-400 mb-1">target: +{yoyTarget}%</p>
                  </div>
                  {/* Progress bar */}
                  <div className="w-full bg-gray-200 rounded-full h-2 mb-2">
                    <div className={`h-2 rounded-full transition-all ${yoyOnTrack ? 'bg-emerald-500' : 'bg-amber-500'}`} style={{ width: `${yoyProgress}%` }}></div>
                  </div>
                  <div className="text-[11px] space-y-1 text-gray-600">
                    <p className="text-[10px] text-gray-400 font-semibold uppercase mt-1">YTD Actual (Collections)</p>
                    <div className="flex justify-between"><span>Prior Year (Jan–{throughLabel} {yoyRevenue?.priorYear || ''})</span><span className="font-medium">{fmt(priorYearYTD)}</span></div>
                    <div className="flex justify-between"><span>Current Year (Jan–{throughLabel} {yoyRevenue?.currentYear || ''})</span><span className="font-medium">{fmt(currentYearYTD)}</span></div>
                    <div className="flex justify-between"><span>YTD Growth</span><span className={`font-semibold ${currentYearYTD - priorYearYTD >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>{fmt(currentYearYTD - priorYearYTD)} ({yoyGrowthPct !== null ? `${yoyGrowthPct > 0 ? '+' : ''}${yoyGrowthPct}%` : '—'})</span></div>
                    <p className="text-[10px] text-gray-400 font-semibold uppercase mt-2">Projected Full Year</p>
                    <div className="flex justify-between"><span>Prior Year (annualised)</span><span className="font-medium">{fmt(priorFullYearEst)}</span></div>
                    <div className="flex justify-between"><span>Current Year (forecast)</span><span className="font-medium">{fmt(projectedFullYearRev)}</span></div>
                    <div className="flex justify-between"><span>Projected Growth</span><span className={`font-semibold ${projectedOnTrack ? 'text-emerald-600' : 'text-amber-600'}`}>{fmt(projectedFullYearRev - priorFullYearEst)} ({projectedGrowthPct !== null ? `${projectedGrowthPct > 0 ? '+' : ''}${projectedGrowthPct}%` : '—'})</span></div>
                  </div>
                </div>

                {/* OKR: Net Cash Growth */}
                <div className={`rounded-xl shadow-sm border p-4 ${netCashOnTrack ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200'}`}>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">KR5</span>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${netCashOnTrack ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'}`}>{netCashOnTrack ? 'On Track' : 'Behind'}</span>
                    {asOfDate && <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-700 font-medium ml-auto">as of {asOfLabel}</span>}
                  </div>
                  <p className="text-xs font-medium text-gray-700 mb-2">Maintain min. net growth in cash of {fmt(netCashTarget)}</p>
                  <div className="flex items-end gap-3 mb-2">
                    <p className={`text-2xl font-bold ${netCashOnTrack ? 'text-emerald-700' : 'text-amber-700'}`}>{fmt(netCashGrowth)}</p>
                    <p className="text-xs text-gray-400 mb-1">target: {fmt(netCashTarget)}</p>
                  </div>
                  {/* Progress bar */}
                  <div className="w-full bg-gray-200 rounded-full h-2 mb-2">
                    <div className={`h-2 rounded-full transition-all ${netCashOnTrack ? 'bg-emerald-500' : 'bg-amber-500'}`} style={{ width: `${netCashProgress}%` }}></div>
                  </div>
                  <div className="text-[11px] space-y-1 text-gray-600">
                    <p className="text-[10px] text-gray-400 font-semibold uppercase mt-1">YTD Actual</p>
                    <div className="flex justify-between"><span>Jan Opening Balance</span><span className="font-medium">{fmt(janOpening)}</span></div>
                    <div className="flex justify-between"><span>Current Balance ({pastOrCurrent.length > 0 ? pastOrCurrent[pastOrCurrent.length - 1].month : '—'})</span><span className="font-medium">{fmt(latestClosing)}</span></div>
                    <div className="flex justify-between"><span>YTD Net Growth</span><span className={`font-semibold ${netCashGrowth >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>{fmt(netCashGrowth)} ({Math.round(netCashProgress)}%)</span></div>
                    <p className="text-[10px] text-gray-400 font-semibold uppercase mt-2">Projected Full Year</p>
                    <div className="flex justify-between"><span>Dec Closing (forecast)</span><span className="font-medium">{fmt(decClosing)}</span></div>
                    <div className="flex justify-between"><span>Projected Net Growth</span><span className={`font-semibold ${projNetCashOnTrack ? 'text-emerald-600' : 'text-amber-600'}`}>{fmt(projNetCashGrowth)} ({Math.round(projNetCashProgress)}%)</span></div>
                    <div className="flex justify-between"><span>vs Target</span><span className={`font-semibold ${projNetCashOnTrack ? 'text-emerald-600' : 'text-red-600'}`}>{projNetCashGrowth >= netCashTarget ? '+' : ''}{fmt(projNetCashGrowth - netCashTarget)}</span></div>
                  </div>
                </div>
              </div>
              );
            })()}

            {/* Chart expand modal */}
            {expandedChart && (
              <div className="fixed inset-0 bg-black/50 z-[9999] flex items-center justify-center p-8" onClick={() => setExpandedChart(null)}>
                <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl p-8 max-h-[90vh]" onClick={e => e.stopPropagation()}>
                  <div className="flex justify-between items-center mb-4">
                    <h3 className="text-lg font-semibold text-gray-700">
                      {expandedChart === 'cash' ? 'Cash Balance Forecast (€K)' : expandedChart === 'salary' ? 'Salary — Budget vs Actual (€K)' : expandedChart === 'netflow' ? 'Net Cashflow — Inflows vs Outflows (€K)' : expandedChart === 'revenue' ? 'Revenue — Budget vs Actual (€K)' : 'Vendors — Budget vs Actual (€K)'}
                    </h3>
                    <button onClick={() => setExpandedChart(null)} className="text-gray-400 hover:text-gray-600 text-2xl leading-none">&times;</button>
                  </div>
                  <ResponsiveContainer width="100%" height={500}>
                    {expandedChart === 'cash' ? (
                      <ComposedChart data={chartData} margin={{ top: 30, right: 20, left: 20, bottom: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                        <XAxis dataKey="name" tick={{ fontSize: 13 }} />
                        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `${v >= 1000 ? `${Math.round(v/1000)}M` : `${v}K`}`} />
                        <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                        <Area type="monotone" dataKey="closing" name="Closing Balance" fill="#dbeafe" stroke="#2563eb" strokeWidth={2} fillOpacity={0.3} />
                        <Bar dataKey="net" name="Monthly Net" radius={[3, 3, 0, 0]}>
                          {chartData.map((entry, idx) => (
                            <Cell key={idx} fill={entry.net >= 0 ? '#059669' : '#dc2626'} opacity={0.6} />
                          ))}
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Legend wrapperStyle={{ fontSize: 13 }} />
                      </ComposedChart>
                    ) : expandedChart === 'salary' ? (
                      <BarChart data={chartData} margin={{ top: 30, right: 20, left: 20, bottom: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                        <XAxis dataKey="name" tick={{ fontSize: 13 }} />
                        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `${v}K`} domain={[0, (max: number) => Math.ceil(max * 1.08)]} />
                        <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                        <Bar dataKey="salaryBudget" name="Budget" fill="#fca5a5" opacity={0.6} radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y + 16} fill="#991b1b" fontSize={10} fontWeight={500} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Bar dataKey="salaryActual" name="Actual" fill="#86efac" radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Legend wrapperStyle={{ fontSize: 13 }} />
                      </BarChart>
                    ) : expandedChart === 'netflow' ? (
                      <ComposedChart data={chartData} margin={{ top: 30, right: 20, left: 20, bottom: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                        <XAxis dataKey="name" tick={{ fontSize: 13 }} />
                        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `${v}K`} />
                        <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                        <Bar dataKey="inflows" name="Inflows" fill="#34d399" opacity={0.5} radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Bar dataKey="outflows" name="Outflows" fill="#f87171" opacity={0.5} radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Line type="monotone" dataKey="net" name="Net" stroke="#059669" strokeWidth={3} dot={{ r: 4, fill: '#059669', stroke: '#fff', strokeWidth: 1 }}>
                          <LabelList content={(props: any) => { const { x, y, value } = props; if (value == null || value === 0) return null; return <text x={x} y={y - 10} fill="#065f46" fontSize={13} fontWeight={700} textAnchor="middle">{value}K</text>; }} />
                        </Line>
                        <Legend wrapperStyle={{ fontSize: 13 }} />
                      </ComposedChart>
                    ) : expandedChart === 'revenue' ? (
                      <ComposedChart data={chartData} margin={{ top: 30, right: 20, left: 20, bottom: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                        <XAxis dataKey="name" tick={{ fontSize: 13 }} />
                        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `€${v}K`} />
                        <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                        <Bar dataKey="revBudget" name="Budget" fill="#93c5fd" opacity={0.4} radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y + 16} fill="#1e40af" fontSize={10} fontWeight={500} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Bar dataKey="revActual" name="Actual" fill="#3b82f6" radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Line type="monotone" dataKey={(entry: any) => { if (entry.revActual > 0 && entry.revBudget > 0) return entry.revActual - entry.revBudget; return null; }} name="Variance" stroke="#059669" strokeWidth={2.5} strokeDasharray="4 2" dot={{ r: 4, fill: '#059669', stroke: '#fff', strokeWidth: 1 }}>
                          <LabelList content={(props: any) => { const { x, y, value } = props; if (value == null || value === 0) return null; return <text x={x} y={y - 10} fill={value >= 0 ? '#059669' : '#dc2626'} fontSize={13} fontWeight={700} textAnchor="middle">{value > 0 ? '+' : ''}{value}K</text>; }} />
                        </Line>
                        <Legend wrapperStyle={{ fontSize: 13 }} />
                      </ComposedChart>
                    ) : (
                      <BarChart data={chartData} margin={{ top: 30, right: 20, left: 20, bottom: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                        <XAxis dataKey="name" tick={{ fontSize: 13 }} />
                        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `${v}K`} />
                        <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                        <Bar dataKey="vendorBudget" name="Budget" fill="#c4b5fd" opacity={0.35} radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Bar dataKey="vendorActual" name="Actual" fill="#8b5cf6" radius={[3, 3, 0, 0]}>
                          <LabelList content={(props: any) => { const { x, y, width, value } = props; if (!value || value === 0) return null; return <text x={x + width / 2} y={y - 6} fill="#333" fontSize={12} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                        </Bar>
                        <Legend wrapperStyle={{ fontSize: 13 }} />
                      </BarChart>
                    )}
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Cash Balance Chart */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 cursor-pointer hover:shadow-md transition-shadow" onClick={() => setExpandedChart('cash')} title="Click to expand">
              <h3 className="text-sm font-medium text-gray-700 mb-3">Cash Balance Forecast (€K) <span className="text-[10px] text-gray-400 font-normal ml-2">click to expand</span></h3>
              <ResponsiveContainer width="100%" height={200}>
                <ComposedChart data={chartData} margin={{ top: 20, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${v >= 1000 ? `${Math.round(v/1000)}M` : `${v}K`}`} />
                  <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                  <Area type="monotone" dataKey="closing" name="Closing Balance" fill="#dbeafe" stroke="#2563eb" strokeWidth={2} fillOpacity={0.3} />
                  <Bar dataKey="net" name="Monthly Net" radius={[2, 2, 0, 0]}>
                    {chartData.map((entry, idx) => (
                      <Cell key={idx} fill={entry.net >= 0 ? '#059669' : '#dc2626'} opacity={0.6} />
                    ))}
                    <LabelList content={renderLabel} />
                  </Bar>
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {/* Salary & Vendor Budget vs Actual */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 cursor-pointer hover:shadow-md transition-shadow" onClick={() => setExpandedChart('salary')} title="Click to expand">
                <h3 className="text-sm font-medium text-gray-700 mb-3">Salary — Budget vs Actual (€K) <span className="text-[10px] text-gray-400 font-normal ml-2">click to expand</span></h3>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={chartData} margin={{ top: 20, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                    <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${v}K`} />
                    <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                    <Bar dataKey="salaryBudget" name="Budget" fill="#fca5a5" opacity={0.6} radius={[2, 2, 0, 0]}>
                      <LabelList content={renderLabel} />
                    </Bar>
                    <Bar dataKey="salaryActual" name="Actual" fill="#86efac" radius={[2, 2, 0, 0]}>
                      <LabelList content={renderLabel} />
                    </Bar>
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 cursor-pointer hover:shadow-md transition-shadow" onClick={() => setExpandedChart('vendors')} title="Click to expand">
                <h3 className="text-sm font-medium text-gray-700 mb-3">Vendors — Budget vs Actual (€K) <span className="text-[10px] text-gray-400 font-normal ml-2">click to expand</span></h3>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={chartData} margin={{ top: 20, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                    <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${v}K`} />
                    <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                    <Bar dataKey="vendorBudget" name="Budget" fill="#c4b5fd" opacity={0.35} radius={[2, 2, 0, 0]}>
                      <LabelList content={renderLabel} />
                    </Bar>
                    <Bar dataKey="vendorActual" name="Actual" fill="#8b5cf6" radius={[2, 2, 0, 0]}>
                      <LabelList content={renderLabel} />
                    </Bar>
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Net Cashflow — Inflows vs Outflows */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 cursor-pointer hover:shadow-md transition-shadow" onClick={() => setExpandedChart('netflow')} title="Click to expand">
              <div className="flex items-start justify-between mb-3">
                <h3 className="text-sm font-medium text-gray-700">Net Cashflow — Inflows vs Outflows (€K) <span className="text-[10px] text-gray-400 font-normal ml-2">click to expand</span></h3>
                <div className="text-[10px] text-gray-400 text-right leading-tight">
                  <span className="inline-block w-2 h-2 rounded-full bg-emerald-400 mr-1"></span>Inflows (collections + pipeline)
                  <span className="inline-block w-2 h-2 rounded-full bg-red-400 mr-1 ml-2"></span>Outflows (salary + vendors)
                  <span className="inline-block w-2 h-2 rounded-full bg-emerald-700 mr-1 ml-2"></span>Net line
                </div>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <ComposedChart data={chartData} margin={{ top: 20, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${v}K`} />
                  <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                  <Bar dataKey="inflows" name="Inflows" fill="#34d399" opacity={0.5} radius={[2, 2, 0, 0]}>
                    <LabelList content={renderLabel} />
                  </Bar>
                  <Bar dataKey="outflows" name="Outflows" fill="#f87171" opacity={0.5} radius={[2, 2, 0, 0]}>
                    <LabelList content={renderLabel} />
                  </Bar>
                  <Line type="monotone" dataKey="net" name="Net" stroke="#059669" strokeWidth={2.5} dot={{ r: 3, fill: '#059669', stroke: '#fff', strokeWidth: 1 }}>
                    <LabelList content={(props: any) => { const { x, y, value } = props; if (value == null || value === 0) return null; return <text x={x} y={y - 8} fill="#065f46" fontSize={10} fontWeight={600} textAnchor="middle">{value}K</text>; }} />
                  </Line>
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {/* Revenue Budget vs Actual */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 cursor-pointer hover:shadow-md transition-shadow" onClick={() => setExpandedChart('revenue')} title="Click to expand">
              <div className="flex items-start justify-between mb-3">
                <h3 className="text-sm font-medium text-gray-700">Revenue — Budget vs Actual (€K) <span className="text-[10px] text-gray-400 font-normal ml-2">click to expand</span></h3>
                <div className="text-[10px] text-gray-400 text-right leading-tight">
                  {(() => {
                    const totalBudget = chartData.reduce((s, d) => s + d.revBudget, 0);
                    const totalActual = chartData.filter(d => d.revActual > 0).reduce((s, d) => s + d.revActual, 0);
                    const variance = totalActual - totalBudget;
                    const pct = totalBudget > 0 ? ((totalActual / totalBudget - 1) * 100).toFixed(1) : '0';
                    const actualMonths = chartData.filter(d => d.revActual > 0).length;
                    return actualMonths > 0 ? <span className={variance >= 0 ? 'text-green-600' : 'text-red-500'}>YTD: {variance >= 0 ? '+' : ''}{pct}% vs budget ({actualMonths}m)</span> : null;
                  })()}
                </div>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <ComposedChart data={chartData} margin={{ top: 20, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `€${v}K`} />
                  <Tooltip formatter={(value: number, name: string) => [`€${value.toLocaleString()}K`, name]} />
                  <Bar dataKey="revBudget" name="Budget" fill="#93c5fd" opacity={0.4} radius={[2, 2, 0, 0]}>
                    <LabelList content={renderLabel} />
                  </Bar>
                  <Bar dataKey="revActual" name="Actual" fill="#3b82f6" radius={[2, 2, 0, 0]}>
                    <LabelList content={renderLabel} />
                  </Bar>
                  <Line type="monotone" dataKey="revVariance" name="Variance" stroke="#059669" strokeWidth={2} strokeDasharray="4 2" dot={{ r: 3, fill: '#059669', stroke: '#fff', strokeWidth: 1 }} connectNulls={false}>
                    <LabelList content={(props: any) => { const { x, y, value } = props; if (value == null || value === 0) return null; return <text x={x} y={y - 8} fill={value >= 0 ? '#059669' : '#dc2626'} fontSize={10} fontWeight={600} textAnchor="middle">{value > 0 ? '+' : ''}{value}K</text>; }} />
                  </Line>
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </div>
          );
        })()}

        {/* ── Consolidated Summary Cards ── */}
        {activeCompany === 'consolidated' && consolidatedCashflow && consolidatedCashflow.length > 0 && (() => {
          const cc = consolidatedCashflow;
          const janOpening = cc[0].openingBalance;
          const decClosing = cc[cc.length - 1].closingBalance;
          const totalColl = cc.reduce((s, r) => s + r.collections, 0);
          const totalSalary = cc.reduce((s, r) => s + r.salary, 0);
          const totalVendors = cc.reduce((s, r) => s + r.vendors, 0);
          const totalICRev = cc.reduce((s, r) => s + r.icRevenue, 0);
          const totalICExp = cc.reduce((s, r) => s + r.icExpense, 0);
          const netCashGrowth = decClosing - janOpening;
          const netCashTarget = 9500000;
          const onTrack = netCashGrowth >= netCashTarget;
          return (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div className="bg-gradient-to-br from-emerald-50 to-teal-50 rounded-xl border border-emerald-200 p-4 cursor-pointer hover:shadow-md transition-shadow"
                onClick={() => { const stInit = consolidatedData?.statscore?.bankBalance?.openingBalance || 0; setConsDrilldown({ type: 'opening', title: 'Monthly Opening Balances', rows: cc.map((r, idx) => { const stOpen = idx === 0 ? stInit : cc[idx - 1].stClosing; return { label: r.month, ls: r.openingBalance - stOpen, st: stOpen, total: r.openingBalance }; }) }); }}>
                <p className="text-[10px] text-emerald-600 uppercase font-medium">Opening (Jan)</p>
                <p className="text-lg font-bold text-emerald-700 mt-1">{fmt(janOpening)}</p>
                <p className="text-[10px] text-gray-400 mt-0.5">LS {fmt(cc[0].openingBalance - (consolidatedData?.statscore?.bankBalance?.openingBalance || 0))} + SC {fmt(consolidatedData?.statscore?.bankBalance?.openingBalance || 0)}</p>
              </div>
              <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border border-blue-200 p-4 cursor-pointer hover:shadow-md transition-shadow"
                onClick={() => setConsDrilldown({ type: 'closing', title: 'Monthly Closing Balances', rows: cc.map(r => ({ label: r.month, ls: r.lsClosing, st: r.stClosing, total: r.closingBalance })) })}>
                <p className="text-[10px] text-blue-600 uppercase font-medium">Projected Dec Closing</p>
                <p className="text-lg font-bold text-blue-700 mt-1">{fmt(decClosing)}</p>
                <p className="text-[10px] text-gray-400 mt-0.5">LS {fmt(cc[cc.length-1].lsClosing)} + SC {fmt(cc[cc.length-1].stClosing)}</p>
              </div>
              <div className={`rounded-xl border p-4 cursor-pointer hover:shadow-md transition-shadow ${onTrack ? 'bg-gradient-to-br from-green-50 to-emerald-50 border-green-200' : 'bg-gradient-to-br from-amber-50 to-orange-50 border-amber-200'}`}
                onClick={() => { let cum = 0; setConsDrilldown({ type: 'growth', title: 'Cumulative Net Cash Growth by Month', rows: cc.map(r => { cum += r.net; return { label: r.month, ls: r.net > 0 ? r.lsCollections - r.lsSalary - r.lsVendors : r.lsCollections - r.lsSalary - r.lsVendors, st: r.net > 0 ? r.stCollections - r.stSalary - r.stVendors : r.stCollections - r.stSalary - r.stVendors, total: cum }; }) }); }}>
                <p className={`text-[10px] uppercase font-medium ${onTrack ? 'text-green-600' : 'text-amber-600'}`}>Net Cash Growth (OKR)</p>
                <p className={`text-lg font-bold mt-1 ${onTrack ? 'text-green-700' : 'text-amber-700'}`}>{netCashGrowth >= 0 ? '+' : ''}{fmt(netCashGrowth)}</p>
                <p className="text-[10px] text-gray-400 mt-0.5">Target: {fmt(netCashTarget)} {onTrack ? '-- On Track' : '-- Behind'}</p>
              </div>
              <div className="bg-gradient-to-br from-orange-50 to-red-50 rounded-xl border border-orange-200 p-4 cursor-pointer hover:shadow-md transition-shadow"
                onClick={() => {
                  const elim = consolidatedData?.elimination || { actualByMonth: {}, projectedByMonth: {} };
                  // Build account-level detail from all months
                  const accountMap: Record<string, { account: string; name: string; type: string; amount: number; source: string }> = {};
                  for (const r of cc) {
                    const src = r.isPast ? elim.actualByMonth[r.mKey] : elim.projectedByMonth[r.mKey];
                    if (src?.details) {
                      for (const d of src.details) {
                        const key = d.acctnumber;
                        if (!accountMap[key]) accountMap[key] = { account: d.acctnumber, name: d.acctname, type: d.accttype, amount: 0, source: r.isPast ? 'actual' : 'budget' };
                        accountMap[key].amount += (d.net || d.amount || 0);
                      }
                    }
                  }
                  const icAccounts = Object.values(accountMap).sort((a, b) => Math.abs(b.amount) - Math.abs(a.amount));
                  const revenueAccts = icAccounts.filter(a => ['Income'].includes(a.type) || ['400017','400020','400022','400023'].includes(a.account));
                  const expenseAccts = icAccounts.filter(a => !['Income'].includes(a.type) && !['400017','400020','400022','400023'].includes(a.account));
                  setConsDrilldown({
                    type: 'ic', title: 'I/C Elimination by Month',
                    rows: cc.map(r => ({ label: r.month, ls: r.icRevenue, st: r.icExpense, total: r.icRevenue + r.icExpense, color: 'orange' })),
                    accounts: {
                      ls: revenueAccts.map(a => ({ account: a.account, name: `${a.name} (Revenue)`, amount: a.amount })),
                      st: expenseAccts.map(a => ({ account: a.account, name: `${a.name} (Expense)`, amount: a.amount })),
                    },
                  });
                }}>
                <p className="text-[10px] text-orange-600 uppercase font-medium">I/C Elimination (YTD)</p>
                <p className="text-lg font-bold text-orange-700 mt-1">{fmt(totalICRev + totalICExp)}</p>
                <p className="text-[10px] text-gray-400 mt-0.5">Revenue: {fmt(totalICRev)} | Expense: {fmt(totalICExp)}</p>
              </div>
            </div>
          );
        })()}

        {/* ── Consolidated Cashflow View ── */}
        {activeCompany === 'consolidated' && consolidatedCashflow && consolidatedCashflow.length > 0 && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-3">
            <h3 className="text-sm font-medium text-gray-700 mb-1 flex items-center gap-2">
              <TrendingUp className="w-4 h-4" /> Consolidated Cashflow Forecast (Through December {activeYear})
              {(activeYears.lsports || currentYear) !== (activeYears.statscore || currentYear) && (
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-blue-50 text-blue-600 font-medium">LS:{activeYears.lsports || currentYear} + ST:{activeYears.statscore || currentYear}</span>
              )}
              <span className="ml-2 text-[10px] px-2 py-0.5 rounded-full bg-slate-200 text-slate-600 font-semibold uppercase tracking-wide">Read Only</span>
            </h3>
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs text-gray-400">
                LSports + Statscore combined | I/C elimination applied
              </p>
              <div className="flex items-center gap-3 ml-4 shrink-0">
                {/* Consolidated scenario pickers */}
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-gray-500 font-medium">LSports:</span>
                  <select
                    value={consLsScenarioId || ''}
                    onChange={e => setConsLsScenarioId(e.target.value || null)}
                    className="text-xs border border-gray-200 rounded-md px-2 py-1 bg-white focus:ring-1 focus:ring-emerald-300"
                  >
                    <option value="">Base (no adjustments)</option>
                    {[...scenarios, ..._shared].filter(s => !s.company || s.company === 'lsports').map(s => (
                      <option key={s.id} value={s.id}>{s.name}{s.ownerEmail ? ` (${s.ownerName || s.ownerEmail})` : ''}</option>
                    ))}
                  </select>
                </div>
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-gray-500 font-medium">Statscore:</span>
                  <select
                    value={consStScenarioId || ''}
                    onChange={e => setConsStScenarioId(e.target.value || null)}
                    className="text-xs border border-gray-200 rounded-md px-2 py-1 bg-white focus:ring-1 focus:ring-emerald-300"
                  >
                    <option value="">Base (no adjustments)</option>
                    {[...scenarios, ..._shared].filter(s => !s.company || s.company === 'statscore').map(s => (
                      <option key={s.id} value={s.id}>{s.name}{s.ownerEmail ? ` (${s.ownerName || s.ownerEmail})` : ''}</option>
                    ))}
                  </select>
                </div>
                <span className="text-[10px] px-2 py-1 bg-violet-100 text-violet-700 rounded-md font-medium">EUR only</span>
                <button
                  onClick={() => {
                    const cc = consolidatedCashflow!;
                    const hdrStyle = { font: { bold: true, color: { rgb: 'FFFFFF' }, sz: 10 }, fill: { fgColor: { rgb: '4B5563' } }, border: { bottom: { style: 'thin', color: { rgb: '9CA3AF' } } }, alignment: { horizontal: 'center' } };
                    const eurFmt = '#,##0';
                    const colColors: Record<string, string> = { green: '16A34A', amber: 'D97706', violet: '7C3AED', red: 'DC2626', orange: 'EA580C', blue: '1D4ED8', teal: '0D9488', gray: '374151' };
                    const statusColors: Record<string, { bg: string; fg: string }> = { ACTUAL: { bg: 'DCFCE7', fg: '15803D' }, CURRENT: { bg: 'FEF3C7', fg: 'B45309' }, PROJECTED: { bg: 'EDE9FE', fg: '6D28D9' } };

                    // Title rows
                    const titleRow = [{ v: 'Consolidated Cashflow Forecast (Through December ' + activeYear + ')', s: { font: { bold: true, sz: 14, color: { rgb: '1F2937' } } } }];
                    const subtitleRow = [{ v: 'LSports + Statscore combined | I/C elimination applied', s: { font: { sz: 10, color: { rgb: '9CA3AF' }, italic: true } } }];
                    const emptyRow: any[] = [];

                    // Header
                    const headers = ['Month', '', 'Opening Bal.', 'Collections', 'LS Collections', 'SC Collections', 'Salary', 'LS Salary', 'SC Salary', 'Vendors', 'LS Vendors', 'SC Vendors', 'Total Outflow', 'I/C Rev.', 'I/C Exp.', 'I/C Source', 'Reval', 'Net', 'Closing Balance', 'LS Closing', 'SC Closing'];
                    const hdrColors = ['gray', 'gray', 'gray', 'green', 'green', 'green', 'amber', 'amber', 'amber', 'violet', 'violet', 'violet', 'red', 'orange', 'orange', 'orange', 'amber', 'gray', 'blue', 'blue', 'blue'];
                    const hdrRow = headers.map((h, ci) => ({
                      v: h, s: { ...hdrStyle, font: { ...hdrStyle.font, color: { rgb: 'FFFFFF' } }, fill: { fgColor: { rgb: colColors[hdrColors[ci]] || '4B5563' } } }
                    }));

                    // Data rows
                    const dataRows = cc.map(r => {
                      const status = r.isPast ? 'ACTUAL' : r.isCurrent ? 'CURRENT' : 'PROJECTED';
                      const sc = statusColors[status];
                      const statusCell = { v: status, s: { font: { bold: true, sz: 9, color: { rgb: sc.fg } }, fill: { fgColor: { rgb: sc.bg } }, alignment: { horizontal: 'center' } } };
                      const rowBg = r.isPast ? 'F9FAFB' : r.isCurrent ? 'EFF6FF' : 'FFFFFF';
                      const numCell = (val: number, color: string, bold = false) => ({
                        v: val, t: 'n', s: { font: { color: { rgb: val < 0 ? 'DC2626' : colColors[color] || '374151' }, bold, sz: 10 }, fill: { fgColor: { rgb: rowBg } }, alignment: { horizontal: 'right' }, numFmt: eurFmt }
                      });
                      const subCell = (val: number, color: string) => ({
                        v: val, t: 'n', s: { font: { color: { rgb: '9CA3AF' }, sz: 9 }, fill: { fgColor: { rgb: rowBg } }, alignment: { horizontal: 'right' }, numFmt: eurFmt }
                      });
                      return [
                        { v: r.month, s: { font: { bold: true, color: { rgb: '374151' }, sz: 10 }, fill: { fgColor: { rgb: rowBg } } } },
                        statusCell,
                        numCell(r.openingBalance, 'gray', true),
                        numCell(r.collections, 'green', true),
                        subCell(r.lsCollections, 'gray'),
                        subCell(r.stCollections, 'gray'),
                        numCell(r.salary, 'amber'),
                        subCell(r.lsSalary, 'gray'),
                        subCell(r.stSalary, 'gray'),
                        numCell(r.vendors, 'violet'),
                        subCell(r.lsVendors, 'gray'),
                        subCell(r.stVendors, 'gray'),
                        numCell(r.totalOutflow, 'red', true),
                        { v: r.icRevenue !== 0 ? -r.icRevenue : 0, t: 'n', s: { font: { color: { rgb: 'DC2626' }, sz: 10 }, fill: { fgColor: { rgb: rowBg } }, alignment: { horizontal: 'right' }, numFmt: eurFmt } },
                        { v: r.icExpense !== 0 ? r.icExpense : 0, t: 'n', s: { font: { color: { rgb: '16A34A' }, sz: 10 }, fill: { fgColor: { rgb: rowBg } }, alignment: { horizontal: 'right' }, numFmt: eurFmt } },
                        { v: r.icSource || '--', s: { font: { sz: 9, color: { rgb: r.icSource === 'actual' ? '15803D' : '6D28D9' } }, fill: { fgColor: { rgb: r.icSource === 'actual' ? 'DCFCE7' : 'EDE9FE' } }, alignment: { horizontal: 'center' } } },
                        numCell(r.revalImpact, 'amber'),
                        { v: r.net, t: 'n', s: { font: { bold: true, color: { rgb: r.net >= 0 ? '15803D' : 'DC2626' }, sz: 10 }, fill: { fgColor: { rgb: rowBg } }, alignment: { horizontal: 'right' }, numFmt: eurFmt } },
                        numCell(r.closingBalance, 'blue', true),
                        subCell(r.lsClosing, 'gray'),
                        subCell(r.stClosing, 'gray'),
                      ];
                    });

                    // Total row
                    const totBg = 'F3F4F6';
                    const totStyle = (color: string, bold = true) => ({ font: { bold, color: { rgb: colColors[color] || '374151' }, sz: 10 }, fill: { fgColor: { rgb: totBg } }, alignment: { horizontal: 'right' as const }, numFmt: eurFmt, border: { top: { style: 'medium' as const, color: { rgb: '6B7280' } } } });
                    const totalRow = [
                      { v: 'TOTAL', s: { font: { bold: true, sz: 11, color: { rgb: '374151' } }, fill: { fgColor: { rgb: totBg } }, border: { top: { style: 'medium', color: { rgb: '6B7280' } } } } },
                      { v: '', s: { fill: { fgColor: { rgb: totBg } }, border: { top: { style: 'medium', color: { rgb: '6B7280' } } } } },
                      { v: '', s: { fill: { fgColor: { rgb: totBg } }, border: { top: { style: 'medium', color: { rgb: '6B7280' } } } } },
                      { v: cc.reduce((s, r) => s + r.collections, 0), t: 'n', s: totStyle('green') },
                      { v: cc.reduce((s, r) => s + r.lsCollections, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.stCollections, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.salary, 0), t: 'n', s: totStyle('amber') },
                      { v: cc.reduce((s, r) => s + r.lsSalary, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.stSalary, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.vendors, 0), t: 'n', s: totStyle('violet') },
                      { v: cc.reduce((s, r) => s + r.lsVendors, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.stVendors, 0), t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc.reduce((s, r) => s + r.totalOutflow, 0), t: 'n', s: totStyle('red') },
                      { v: -cc.reduce((s, r) => s + r.icRevenue, 0), t: 'n', s: { ...totStyle('red'), font: { bold: true, color: { rgb: 'DC2626' }, sz: 10 } } },
                      { v: cc.reduce((s, r) => s + r.icExpense, 0), t: 'n', s: { ...totStyle('green'), font: { bold: true, color: { rgb: '16A34A' }, sz: 10 } } },
                      { v: '', s: { fill: { fgColor: { rgb: totBg } }, border: { top: { style: 'medium', color: { rgb: '6B7280' } } } } },
                      { v: cc.reduce((s, r) => s + r.revalImpact, 0), t: 'n', s: totStyle('amber') },
                      { v: cc.reduce((s, r) => s + r.net, 0), t: 'n', s: { ...totStyle('gray'), font: { bold: true, color: { rgb: cc.reduce((s, r) => s + r.net, 0) >= 0 ? '15803D' : 'DC2626' }, sz: 10 } } },
                      { v: cc[cc.length - 1]?.closingBalance || 0, t: 'n', s: totStyle('blue') },
                      { v: cc[cc.length - 1]?.lsClosing || 0, t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                      { v: cc[cc.length - 1]?.stClosing || 0, t: 'n', s: { ...totStyle('gray', false), font: { ...totStyle('gray', false).font, color: { rgb: '9CA3AF' } } } },
                    ];

                    const sheetData = [titleRow, subtitleRow, emptyRow, hdrRow, ...dataRows, totalRow];
                    const ws = XLSX.utils.aoa_to_sheet(sheetData);
                    ws['!cols'] = [{ wch: 16 }, { wch: 11 }, { wch: 15 }, { wch: 15 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 12 }, { wch: 12 }, { wch: 10 }, { wch: 12 }, { wch: 14 }, { wch: 16 }, { wch: 14 }, { wch: 14 }];
                    ws['!merges'] = [{ s: { r: 0, c: 0 }, e: { r: 0, c: 6 } }, { s: { r: 1, c: 0 }, e: { r: 1, c: 6 } }];

                    const wb = XLSX.utils.book_new();
                    XLSX.utils.book_append_sheet(wb, ws, 'Consolidated Cashflow');
                    XLSX.writeFile(wb, `Consolidated_Cashflow_${new Date().toISOString().slice(0, 10)}.xlsx`);
                  }}
                  className="text-[10px] text-green-600 hover:text-green-800 bg-green-50 border border-green-200 rounded-lg px-2 py-1 transition-colors"
                  title="Download as Excel (styled)"
                >📥 Excel</button>
              </div>
            </div>

            {/* Consolidated Cashflow Table */}
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-left text-gray-400 uppercase border-b-2 border-gray-200">
                    <th className="pb-2 pr-1 whitespace-nowrap">Month</th>
                    <th className="pb-2 pr-1 text-right whitespace-nowrap">Opening Bal.</th>
                    <th className="pb-2 pr-1 text-right text-green-600 whitespace-nowrap">Collections</th>
                    <th className="pb-2 pr-1 text-right text-amber-600 whitespace-nowrap">Salary</th>
                    <th className="pb-2 pr-1 text-right text-violet-600 whitespace-nowrap">Vendors</th>
                    <th className="pb-2 pr-1 text-right text-red-600 whitespace-nowrap">Total Outflow</th>
                    <th className="pb-2 pr-1 text-right text-orange-500 whitespace-nowrap">I/C Elim.<div className="text-[8px] font-normal normal-case text-gray-400">Revenue | Expense</div></th>
                    <th className="pb-2 pr-1 text-right text-amber-500 whitespace-nowrap">Reval</th>
                    <th className="pb-2 pr-1 text-right whitespace-nowrap">Net</th>
                    <th className="pb-2 pr-1 text-right text-blue-700 whitespace-nowrap">Closing Balance</th>
                  </tr>
                </thead>
                <tbody>
                  {consolidatedCashflow.map((r, i) => (
                    <tr key={i} className={`border-b border-gray-100 h-[48px] ${r.isCurrent ? 'bg-blue-50/30' : r.isPast ? 'bg-gray-50/50' : ''}`}>
                      <td className="py-2 pr-3 whitespace-nowrap">
                        <span className="font-medium text-gray-700">{r.month}</span>
                        {r.isPast
                          ? <span className="ml-2 text-[10px] bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full font-medium">ACTUAL</span>
                          : r.isCurrent
                          ? <span className="ml-2 text-[10px] bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded-full font-medium">CURRENT</span>
                          : <span className="ml-2 text-[10px] bg-violet-100 text-violet-700 px-1.5 py-0.5 rounded-full font-medium">PROJECTED</span>
                        }
                      </td>
                      <td className={`py-2 pr-1 text-right font-medium ${r.openingBalance >= 0 ? 'text-gray-700' : 'text-red-600'}`}>
                        {fmt(r.openingBalance)}
                      </td>
                      <td className="py-2 pr-1 text-right text-green-600 font-medium cursor-pointer hover:bg-green-50/50 rounded transition-colors"
                        onClick={() => { setConsDrilldown({ type: 'collections', title: `Collections Breakdown - ${r.month}`, rows: [{ label: 'Collections', ls: r.lsCollections, st: r.stCollections, total: r.collections }], loading: true }); fetch(`/api/consolidated-account-breakdown?month=${r.mKey}&type=collections`).then(res => res.json()).then(data => { setConsDrilldown(prev => prev ? { ...prev, accounts: data, loading: false } : null); }).catch(() => setConsDrilldown(prev => prev ? { ...prev, loading: false } : null)); }}>
                        {fmt(r.collections)}
                        <div className="text-[9px] text-gray-400">LS {fmt(r.lsCollections)} | SC {fmt(r.stCollections)}</div>
                      </td>
                      <td className="py-2 pr-1 text-right text-amber-600 cursor-pointer hover:bg-amber-50/50 rounded transition-colors"
                        onClick={() => { setConsDrilldown({ type: 'salary', title: `Salary Breakdown - ${r.month}`, rows: [{ label: 'Salary', ls: r.lsSalary, st: r.stSalary, total: r.salary }], loading: true }); fetch(`/api/consolidated-account-breakdown?month=${r.mKey}&type=salary`).then(res => res.json()).then(data => { setConsDrilldown(prev => prev ? { ...prev, accounts: data, loading: false } : null); }).catch(() => setConsDrilldown(prev => prev ? { ...prev, loading: false } : null)); }}>
                        {fmt(r.salary)}
                        <div className="text-[9px] text-gray-400">LS {fmt(r.lsSalary)} | SC {fmt(r.stSalary)}</div>
                      </td>
                      <td className="py-2 pr-1 text-right text-violet-600 cursor-pointer hover:bg-violet-50/50 rounded transition-colors"
                        onClick={() => { setConsDrilldown({ type: 'vendors', title: `Vendors Breakdown - ${r.month}`, rows: [{ label: 'Vendors (pre-IC)', ls: r.lsVendors, st: r.stVendors, total: r.lsVendors + r.stVendors }, { label: 'I/C Expense Elim', ls: -r.icExpense, st: 0, total: -r.icExpense, color: 'orange' }, { label: 'Vendors (net)', ls: r.lsVendors - r.icExpense, st: r.stVendors, total: r.vendors }], loading: true }); fetch(`/api/consolidated-account-breakdown?month=${r.mKey}&type=vendors`).then(res => res.json()).then(data => { setConsDrilldown(prev => prev ? { ...prev, accounts: data, loading: false } : null); }).catch(() => setConsDrilldown(prev => prev ? { ...prev, loading: false } : null)); }}>
                        {fmt(r.vendors)}
                        <div className="text-[9px] text-gray-400">LS {fmt(r.lsVendors)} | SC {fmt(r.stVendors)}</div>
                      </td>
                      <td className="py-2 pr-1 text-right text-red-600 font-medium">
                        {fmt(r.totalOutflow)}
                      </td>
                      <td className={`py-2 pr-1 text-right text-orange-500 ${(r.icRevenue !== 0 || r.icExpense !== 0) ? 'cursor-pointer hover:bg-orange-50/50 rounded transition-colors' : ''}`}
                        onClick={() => {
                          if (r.icRevenue === 0 && r.icExpense === 0) return;
                          const elim = consolidatedData?.elimination || { actualByMonth: {}, projectedByMonth: {} };
                          const src = r.isPast ? elim.actualByMonth[r.mKey] : elim.projectedByMonth[r.mKey];
                          const details = src?.details || [];
                          const revenueAccts = ['400017','400020','400022','400023'];
                          const revDetails = details.filter((d: any) => revenueAccts.includes(d.acctnumber));
                          const expDetails = details.filter((d: any) => !revenueAccts.includes(d.acctnumber));
                          setConsDrilldown({
                            type: 'ic', title: `I/C Elimination — ${r.month} (${r.icSource})`,
                            rows: [
                              { label: 'Revenue Elimination', ls: r.icRevenue, st: 0, total: r.icRevenue, color: 'orange' },
                              { label: 'Expense Elimination', ls: 0, st: r.icExpense, total: r.icExpense, color: 'orange' },
                              { label: 'Total I/C Impact', ls: r.icRevenue, st: r.icExpense, total: r.icRevenue + r.icExpense, color: 'orange' },
                            ],
                            accounts: {
                              ls: revDetails.map((d: any) => ({ account: d.acctnumber, name: d.acctname + ' (Revenue)', amount: d.net || d.amount || 0 })),
                              st: expDetails.map((d: any) => ({ account: d.acctnumber, name: d.acctname + ' (Expense)', amount: d.net || d.amount || 0 })),
                            },
                          });
                        }}>
                        {(r.icRevenue !== 0 || r.icExpense !== 0) ? (
                          <div>
                            <span className="text-red-500">-{fmt(r.icRevenue)}</span>
                            <span className="text-gray-300 mx-0.5">|</span>
                            <span className="text-green-600">+{fmt(r.icExpense)}</span>
                            <div className="text-[9px] text-gray-400">{r.icSource}</div>
                          </div>
                        ) : <span className="text-gray-300">--</span>}
                      </td>
                      <td className="py-2 pr-1 text-right text-amber-500">
                        {r.revalImpact !== 0 ? fmt(r.revalImpact) : <span className="text-gray-300">--</span>}
                      </td>
                      <td className={`py-2 pr-1 text-right font-bold ${r.net >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                        {r.net >= 0 ? '+' : ''}{fmt(r.net)}
                      </td>
                      <td className={`py-2 pr-1 text-right font-bold cursor-pointer hover:bg-blue-50/50 rounded transition-colors ${r.closingBalance >= 0 ? 'text-blue-700' : 'text-red-600'}`}
                        onClick={() => setConsDrilldown({ type: 'closing-month', title: `Closing Balance Breakdown - ${r.month}`, rows: [{ label: 'Closing Balance', ls: r.lsClosing, st: r.stClosing, total: r.closingBalance }] })}>
                        {fmt(r.closingBalance)}
                        <div className="text-[9px] text-gray-400">LS {fmt(r.lsClosing)} | SC {fmt(r.stClosing)}</div>
                      </td>
                    </tr>
                  ))}
                  {/* Totals row */}
                  <tr className="border-t-2 border-gray-300 font-bold bg-gray-50">
                    <td className="py-2 pr-3 text-gray-700">TOTAL</td>
                    <td className="py-2 pr-1 text-right"></td>
                    <td className="py-2 pr-1 text-right text-green-600">{fmt(consolidatedCashflow.reduce((s, r) => s + r.collections, 0))}</td>
                    <td className="py-2 pr-1 text-right text-amber-600">{fmt(consolidatedCashflow.reduce((s, r) => s + r.salary, 0))}</td>
                    <td className="py-2 pr-1 text-right text-violet-600">{fmt(consolidatedCashflow.reduce((s, r) => s + r.vendors, 0))}</td>
                    <td className="py-2 pr-1 text-right text-red-600">{fmt(consolidatedCashflow.reduce((s, r) => s + r.totalOutflow, 0))}</td>
                    <td className="py-2 pr-1 text-right text-orange-500">
                      <span className="text-red-500">-{fmt(consolidatedCashflow.reduce((s, r) => s + r.icRevenue, 0))}</span>
                      <span className="text-gray-300 mx-0.5">|</span>
                      <span className="text-green-600">+{fmt(consolidatedCashflow.reduce((s, r) => s + r.icExpense, 0))}</span>
                    </td>
                    <td className="py-2 pr-1 text-right text-amber-500">{fmt(consolidatedCashflow.reduce((s, r) => s + r.revalImpact, 0))}</td>
                    <td className={`py-2 pr-1 text-right ${consolidatedCashflow.reduce((s, r) => s + r.net, 0) >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                      {consolidatedCashflow.reduce((s, r) => s + r.net, 0) >= 0 ? '+' : ''}{fmt(consolidatedCashflow.reduce((s, r) => s + r.net, 0))}
                    </td>
                    <td className="py-2 pr-1 text-right text-blue-700">{fmt(consolidatedCashflow[consolidatedCashflow.length - 1]?.closingBalance || 0)}</td>
                  </tr>
                </tbody>
              </table>
            </div>

            {/* ── I/C Elimination Breakdown ── */}
            <div className="mt-4 border-t border-gray-200 pt-3">
              <button
                onClick={() => setConsElimExpanded(!consElimExpanded)}
                className="flex items-center gap-2 text-xs font-medium text-orange-600 hover:text-orange-800 transition-colors"
              >
                {consElimExpanded ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                I/C Elimination Breakdown
                <span className="text-[10px] text-gray-400 font-normal ml-1">
                  (Revenue: {fmt(consolidatedCashflow.reduce((s, r) => s + r.icRevenue, 0))} | Expense: {fmt(consolidatedCashflow.reduce((s, r) => s + r.icExpense, 0))})
                </span>
              </button>
              {consElimExpanded && (
                <div className="mt-2 overflow-x-auto">
                  <table className="w-full text-[11px]">
                    <thead>
                      <tr className="text-left text-gray-400 uppercase border-b border-gray-200">
                        <th className="pb-1.5 pr-1">Month</th>
                        <th className="pb-1.5 pr-1 text-right">Revenue Eliminated</th>
                        <th className="pb-1.5 pr-1 text-right">Expense Eliminated</th>
                        <th className="pb-1.5 pr-1 text-right">Net Impact</th>
                        <th className="pb-1.5 pr-1 text-center">Source</th>
                        <th className="pb-1.5 pr-1"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {consolidatedCashflow.map((r, i) => {
                        const elim = consolidatedData?.elimination || { actualByMonth: {}, projectedByMonth: {} };
                        const actualElim = elim.actualByMonth[r.mKey];
                        const budgetElim = elim.projectedByMonth[r.mKey];
                        const showDetail = consElimDetailMonth === r.mKey;
                        const details = r.isPast && actualElim ? actualElim.details : budgetElim?.details || [];
                        return (
                          <Fragment key={i}>
                            <tr className={`border-b border-gray-50 ${r.isCurrent ? 'bg-blue-50/20' : r.isPast ? 'bg-gray-50/30' : ''}`}>
                              <td className="py-1.5 pr-1 font-medium text-gray-700">{r.month}
                                {r.isPast ? <span className="ml-1 text-[9px] text-green-600">ACTUAL</span> : <span className="ml-1 text-[9px] text-violet-500">BUDGET</span>}
                              </td>
                              <td className="py-1.5 pr-1 text-right text-red-500">{r.icRevenue !== 0 ? `-${fmt(r.icRevenue)}` : <span className="text-gray-300">--</span>}</td>
                              <td className="py-1.5 pr-1 text-right text-green-600">{r.icExpense !== 0 ? `+${fmt(r.icExpense)}` : <span className="text-gray-300">--</span>}</td>
                              <td className={`py-1.5 pr-1 text-right font-medium ${r.icNet >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                                {r.icNet !== 0 ? `${r.icNet >= 0 ? '+' : ''}${fmt(r.icNet)}` : <span className="text-gray-300">--</span>}
                              </td>
                              <td className="py-1.5 pr-1 text-center">
                                <span className={`text-[9px] px-1.5 py-0.5 rounded-full ${r.icSource === 'actual' ? 'bg-green-100 text-green-700' : r.icSource === 'budget' ? 'bg-violet-100 text-violet-700' : 'bg-gray-100 text-gray-400'}`}>
                                  {r.icSource}
                                </span>
                              </td>
                              <td className="py-1.5 pr-1">
                                {details.length > 0 && (
                                  <button onClick={() => setConsElimDetailMonth(showDetail ? null : r.mKey)}
                                          className="text-[10px] text-blue-500 hover:text-blue-700">
                                    {showDetail ? 'Hide' : 'Details'}
                                  </button>
                                )}
                              </td>
                            </tr>
                            {showDetail && details.length > 0 && (
                              <tr>
                                <td colSpan={6} className="px-4 py-1.5 bg-gray-50">
                                  <table className="w-full text-[10px]">
                                    <thead>
                                      <tr className="text-gray-400">
                                        <th className="text-left pb-1">Account</th>
                                        <th className="text-left pb-1">Name</th>
                                        <th className="text-left pb-1">Type</th>
                                        <th className="text-right pb-1">{r.isPast && actualElim ? 'Debit' : 'Amount'}</th>
                                        {r.isPast && actualElim && <th className="text-right pb-1">Credit</th>}
                                        <th className="text-right pb-1">Net</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {details.map((d: any, j: number) => (
                                        <tr key={j} className="border-t border-gray-100">
                                          <td className="py-0.5 font-mono text-gray-600">{d.acctnumber}</td>
                                          <td className="py-0.5 text-gray-700">{d.acctname}</td>
                                          <td className="py-0.5 text-gray-500">{d.accttype}</td>
                                          <td className="py-0.5 text-right">{fmt(r.isPast && actualElim ? d.debit : d.amount)}</td>
                                          {r.isPast && actualElim && <td className="py-0.5 text-right">{fmt(d.credit)}</td>}
                                          <td className={`py-0.5 text-right font-medium ${(d.net || d.amount || 0) >= 0 ? 'text-gray-700' : 'text-red-600'}`}>
                                            {fmt(d.net ?? d.amount ?? 0)}
                                          </td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </td>
                              </tr>
                            )}
                          </Fragment>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}

        {/* ── Consolidated Drilldown Modal ── */}
        {consDrilldown && (
          <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center" onClick={() => setConsDrilldown(null)}>
            <div className={`bg-white rounded-xl shadow-2xl border border-gray-200 p-5 w-full mx-4 max-h-[80vh] overflow-y-auto ${consDrilldown.accounts ? 'max-w-2xl' : 'max-w-lg'}`} onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-bold text-gray-800">{consDrilldown.title}</h3>
                <button onClick={() => setConsDrilldown(null)} className="text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
              </div>
              {/* Summary table */}
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-left text-gray-400 uppercase border-b-2 border-gray-200">
                    <th className="pb-2 pr-2">{consDrilldown.type === 'ic' ? 'Month' : 'Item'}</th>
                    <th className="pb-2 pr-2 text-right">{consDrilldown.type === 'ic' ? 'Revenue' : 'LSports'}</th>
                    <th className="pb-2 pr-2 text-right">{consDrilldown.type === 'ic' ? 'Expense' : 'Statscore'}</th>
                    <th className="pb-2 text-right">{consDrilldown.type === 'ic' ? 'Total' : 'Combined'}</th>
                  </tr>
                </thead>
                <tbody>
                  {consDrilldown.rows.map((row, idx) => (
                    <tr key={idx} className="border-b border-gray-100">
                      <td className="py-1.5 pr-2 font-medium text-gray-700">{row.label}</td>
                      <td className={`py-1.5 pr-2 text-right ${row.color === 'orange' ? 'text-orange-600' : row.ls >= 0 ? 'text-gray-700' : 'text-red-600'}`}>{fmt(row.ls)}</td>
                      <td className={`py-1.5 pr-2 text-right ${row.color === 'orange' ? 'text-orange-600' : row.st >= 0 ? 'text-gray-700' : 'text-red-600'}`}>{fmt(row.st)}</td>
                      <td className={`py-1.5 text-right font-bold ${row.color === 'orange' ? 'text-orange-700' : row.total >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmt(row.total)}</td>
                    </tr>
                  ))}
                </tbody>
                {consDrilldown.rows.length > 1 && !consDrilldown.rows.some(r => r.label.startsWith('Total')) && (() => {
                  const isBalance = consDrilldown.type === 'opening' || consDrilldown.type === 'closing';
                  const lastRow = consDrilldown.rows[consDrilldown.rows.length - 1];
                  return (
                  <tfoot>
                    <tr className="border-t-2 border-gray-300 font-bold">
                      <td className="py-1.5 pr-2 text-gray-700">{isBalance ? 'DECEMBER' : 'TOTAL'}</td>
                      <td className="py-1.5 pr-2 text-right">{fmt(isBalance ? lastRow.ls : consDrilldown.rows.reduce((s, r) => s + r.ls, 0))}</td>
                      <td className="py-1.5 pr-2 text-right">{fmt(isBalance ? lastRow.st : consDrilldown.rows.reduce((s, r) => s + r.st, 0))}</td>
                      <td className="py-1.5 text-right text-blue-700">{fmt(isBalance ? lastRow.total : consDrilldown.rows.reduce((s, r) => s + r.total, 0))}</td>
                    </tr>
                  </tfoot>
                  );
                })()}
              </table>

              {/* Account-level detail */}
              {consDrilldown.loading && (
                <div className="mt-4 text-center text-xs text-gray-400">Loading account details...</div>
              )}
              {consDrilldown.accounts && !consDrilldown.loading && (() => {
                const lsAccts = consDrilldown.accounts!.ls;
                const stAccts = consDrilldown.accounts!.st;
                const summaryRow = consDrilldown.rows[0];
                const lsSummary = summaryRow?.ls || 0;
                const stSummary = summaryRow?.st || 0;
                const hasLs = lsAccts.length > 0;
                const hasSt = stAccts.length > 0;
                if (!hasLs && Math.abs(lsSummary) === 0 && !hasSt && Math.abs(stSummary) === 0) return null;

                // Build merged rows: match by account number, fill gaps
                const lsMap = new Map(lsAccts.map(a => [a.account, a]));
                const stMap = new Map(stAccts.map(a => [a.account, a]));
                const allAccounts = new Set([...lsAccts.map(a => a.account), ...stAccts.map(a => a.account)]);
                // For projected ST with no accounts, inject projected amount into first LS account (Gross Salaries)
                const stProjectedAmount = !hasSt && Math.abs(stSummary) > 0 ? stSummary : 0;
                const lsProjectedAmount = !hasLs && Math.abs(lsSummary) > 0 ? lsSummary : 0;

                type MergedRow = { account: string; name: string; ls: number; st: number; lsProjected?: boolean; stProjected?: boolean };
                const merged: MergedRow[] = [];
                // Use LS account order as primary, then append ST-only accounts
                const addedAccounts = new Set<string>();
                let stProjectedPlaced = false;
                let lsProjectedPlaced = false;
                for (const a of lsAccts) {
                  const stEntry = stMap.get(a.account);
                  const row: MergedRow = { account: a.account, name: a.name, ls: a.amount, st: stEntry?.amount || 0 };
                  // Place projected ST on Gross Salaries row (first row typically)
                  if (stProjectedAmount !== 0 && !stProjectedPlaced && (a.name.toLowerCase().includes('gross') || merged.length === 0)) {
                    row.st = stProjectedAmount;
                    row.stProjected = true;
                    stProjectedPlaced = true;
                  }
                  merged.push(row);
                  addedAccounts.add(a.account);
                }
                // If LS is projected, add a single row
                if (!hasLs && lsProjectedAmount !== 0) {
                  merged.push({ account: '', name: 'Gross Salaries', ls: lsProjectedAmount, st: 0, lsProjected: true });
                  lsProjectedPlaced = true;
                }
                // Append ST-only accounts
                for (const a of stAccts) {
                  if (!addedAccounts.has(a.account)) {
                    merged.push({ account: a.account, name: a.name, ls: 0, st: a.amount });
                  }
                }
                // If projected ST wasn't placed yet (no Gross Salaries match), place it on first row
                if (stProjectedAmount !== 0 && !stProjectedPlaced && merged.length > 0) {
                  merged[0].st = stProjectedAmount;
                  merged[0].stProjected = true;
                }

                const lsTotal = merged.reduce((s, r) => s + r.ls, 0);
                const stTotal = merged.reduce((s, r) => s + r.st, 0);

                const isIC = consDrilldown.type === 'ic';

                return (
                <div className="mt-4">
                  <h4 className="text-[10px] font-bold uppercase mb-1 text-gray-500">Account Detail</h4>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-gray-400 uppercase border-b border-gray-200">
                        <th className="pb-1 pr-1 w-[60px]">Account</th>
                        <th className="pb-1 pr-1">Name</th>
                        <th className="pb-1 text-right pr-2 text-emerald-600">{isIC ? 'Revenue' : 'LSports'}</th>
                        <th className="pb-1 text-right pr-1 text-purple-600">{isIC ? 'Expense' : 'Statscore'}</th>
                        <th className="pb-1 text-right text-blue-600">Combined</th>
                      </tr>
                    </thead>
                    <tbody>
                      {merged.map((r, i) => (
                        <tr key={i} className="border-b border-gray-50">
                          <td className="py-1 pr-1 text-gray-400 font-mono text-[10px]">{r.account}</td>
                          <td className="py-1 pr-1 text-gray-700">{r.name}</td>
                          <td className={`py-1 text-right pr-2 font-medium ${r.lsProjected ? 'text-gray-400 italic' : r.ls >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                            {r.ls !== 0 ? fmt(r.ls) : <span className="text-gray-300">-</span>}
                            {r.lsProjected && <span className="text-[8px] ml-0.5">*</span>}
                          </td>
                          <td className={`py-1 text-right pr-1 font-medium ${r.stProjected ? 'text-gray-400 italic' : r.st >= 0 ? 'text-purple-700' : 'text-red-600'}`}>
                            {r.st !== 0 ? fmt(r.st) : <span className="text-gray-300">-</span>}
                            {r.stProjected && <span className="text-[8px] ml-0.5">*</span>}
                          </td>
                          <td className={`py-1 text-right font-medium ${(r.ls + r.st) >= 0 ? 'text-gray-700' : 'text-red-600'}`}>{fmt(r.ls + r.st)}</td>
                        </tr>
                      ))}
                    </tbody>
                    <tfoot>
                      <tr className="border-t-2 border-gray-300 font-bold">
                        <td className="py-1" colSpan={2}>Total</td>
                        <td className="py-1 text-right pr-2 text-emerald-700">{fmt(lsTotal)}</td>
                        <td className="py-1 text-right pr-1 text-purple-700">{fmt(stTotal)}</td>
                        <td className="py-1 text-right text-blue-700">{fmt(lsTotal + stTotal)}</td>
                      </tr>
                    </tfoot>
                  </table>
                  {(stProjectedAmount !== 0 || lsProjectedAmount !== 0) && (
                    <div className="mt-1 text-[9px] text-gray-400 italic">* Projected (no account detail available)</div>
                  )}
                </div>
                );
              })()}
            </div>
          </div>
        )}

        {/* ── Consolidated Bank Accounts ── */}
        {activeCompany === 'consolidated' && consolidatedData && (() => {
          const lsAccounts: BankAccount[] = consolidatedData.lsports?.bankAccounts || [];
          const stAccounts: BankAccount[] = consolidatedData.statscore?.bankAccounts || [];
          const lsTotal = lsAccounts.reduce((s: number, a: BankAccount) => s + (a.primaryBalance || 0), 0);
          const stTotal = stAccounts.reduce((s: number, a: BankAccount) => s + (a.primaryBalance || 0), 0);
          const grandTotal = lsTotal + stTotal;
          return (
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-3">
              <h3 className="text-sm font-medium text-gray-700 mb-2 flex items-center gap-2">
                <Building2 className="w-4 h-4" /> Consolidated Bank Accounts
              </h3>
              <div className="text-xs font-bold text-gray-800 mb-2">
                Grand Total (LS {lsAccounts.length} accounts + SC {stAccounts.length} accounts): <span className="text-blue-700">{fmt(grandTotal)}</span>
              </div>
              {/* LSports */}
              <div className="mb-1">
                <button onClick={() => setConsBankExpanded(consBankExpanded === 'ls' ? null : 'ls')}
                  className="flex items-center gap-2 w-full text-left text-xs font-medium text-emerald-700 hover:bg-emerald-50 rounded-lg px-2 py-1.5 transition-colors">
                  {consBankExpanded === 'ls' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                  LSports ({lsAccounts.length} accounts) <span className="ml-auto font-bold">{fmt(lsTotal)}</span>
                </button>
                {consBankExpanded === 'ls' && (
                  <div className="ml-6 mt-1 space-y-0.5">
                    {lsAccounts.map((a: BankAccount, idx: number) => (
                      <div key={idx} className="flex justify-between text-[11px] text-gray-600 px-2 py-0.5 hover:bg-gray-50 rounded">
                        <span>{a.name} <span className="text-gray-400">({a.number})</span></span>
                        <span className={`font-medium ${a.primaryBalance >= 0 ? 'text-gray-700' : 'text-red-600'}`}>{fmt(a.primaryBalance || 0)}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {/* Statscore */}
              <div>
                <button onClick={() => setConsBankExpanded(consBankExpanded === 'st' ? null : 'st')}
                  className="flex items-center gap-2 w-full text-left text-xs font-medium text-violet-700 hover:bg-violet-50 rounded-lg px-2 py-1.5 transition-colors">
                  {consBankExpanded === 'st' ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                  Statscore ({stAccounts.length} accounts) <span className="ml-auto font-bold">{fmt(stTotal)}</span>
                </button>
                {consBankExpanded === 'st' && (
                  <div className="ml-6 mt-1 space-y-0.5">
                    {stAccounts.map((a: BankAccount, idx: number) => (
                      <div key={idx} className="flex justify-between text-[11px] text-gray-600 px-2 py-0.5 hover:bg-gray-50 rounded">
                        <span>{a.name} <span className="text-gray-400">({a.number})</span></span>
                        <span className={`font-medium ${a.primaryBalance >= 0 ? 'text-gray-700' : 'text-red-600'}`}>{fmt(a.primaryBalance || 0)}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          );
        })()}

        {/* ── Cashflow Forecast (4 months) ── */}
        {activeCompany !== 'consolidated' && cashflowForecast.length > 0 && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-3">
            <h3 className="text-sm font-medium text-gray-700 mb-1 flex items-center gap-2">
              <TrendingUp className="w-4 h-4" /> Cashflow Forecast (Through December {activeYear})
            </h3>
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs text-gray-400">
                {companyConfig.hasSF ? 'Snowflake: salary + vendor actuals/projections | NetSuite: collections (incl I/C) + Snowflake forecast' : 'NetSuite: all data (collections, salary, vendors) + NS Budget forecast'}
              </p>
              <div className="flex items-center gap-2 ml-4 shrink-0">
                {(Object.values(salaryAdjPctByMonth).some(v => v !== 0) || Object.keys(collPctByMonth).length > 0 || Object.keys(salaryDeptAdj).length > 0) && (() => {
                  const hasSalary = Object.values(salaryAdjPctByMonth).some(v => v !== 0);
                  const hasInflows = Object.keys(collPctByMonth).length > 0;
                  const hasDeptAdj = Object.values(salaryDeptAdj).some(m => Object.values(m).some(v => v !== 0));
                  return (
                    <div className="relative group">
                      <button className="text-xs text-blue-500 hover:text-blue-700 bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5">Reset adj. ▾</button>
                      <div className="absolute right-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg py-1 hidden group-hover:block z-50 min-w-[180px]">
                        {hasSalary && (
                          <button onClick={() => { setSalaryAdjPctByMonth({}); setLeverOverrides({}); }} className="w-full text-left text-xs px-3 py-1.5 hover:bg-red-50 text-red-600">Reset Salaries %</button>
                        )}
                        {hasDeptAdj && (
                          <button onClick={() => setSalaryDeptAdj({})} className="w-full text-left text-xs px-3 py-1.5 hover:bg-amber-50 text-amber-600">Reset Departments %</button>
                        )}
                        {hasInflows && (
                          <button onClick={() => setCollPctByMonth({})} className="w-full text-left text-xs px-3 py-1.5 hover:bg-orange-50 text-orange-600">Reset Inflows %</button>
                        )}
                        <button onClick={() => { setSalaryAdjPctByMonth({}); setCollPctByMonth({}); setLeverOverrides({}); setSalaryDeptAdj({}); }} className="w-full text-left text-xs px-3 py-1.5 hover:bg-gray-100 text-gray-600 border-t border-gray-100">Reset All</button>
                      </div>
                    </div>
                  );
                })()}
                {/* ── Scenario Management ── */}
                <div className="relative" ref={scenarioMenuRef}>
                  <button
                    onClick={() => setScenarioMenuOpen(!scenarioMenuOpen)}
                    className={`text-xs border rounded-lg px-3 py-1.5 flex items-center gap-1.5 transition-colors ${
                      activeScenario ? 'text-violet-700 bg-violet-50 border-violet-200 hover:bg-violet-100' : 'text-gray-500 bg-gray-50 border-gray-200 hover:bg-gray-100'
                    }`}
                  >
                    <span className="text-[10px]">📋</span>
                    {activeScenario ? activeScenario.name : 'Scenarios'}
                    <span className="text-[9px]">▾</span>
                  </button>
                  {scenarioMenuOpen && (
                    <div className="absolute right-0 top-full mt-1 bg-white border border-gray-200 rounded-xl shadow-xl z-50 min-w-[380px] max-h-[500px] overflow-y-auto">
                      {/* Save current state as new scenario */}
                      <div className="px-3 py-2 border-b border-gray-100">
                        <p className="text-[10px] text-gray-400 uppercase font-medium mb-1.5">Save current adjustments</p>
                        <div className="flex items-center gap-1.5">
                          <input
                            type="text"
                            value={scenarioNewName}
                            onChange={e => setScenarioNewName(e.target.value)}
                            onKeyDown={e => { if (e.key === 'Enter' && scenarioNewName.trim()) { saveScenario(scenarioNewName.trim()); setScenarioNewName(''); } }}
                            placeholder="Scenario name…"
                            className="flex-1 text-xs border border-gray-200 rounded-md px-2 py-1 focus:outline-none focus:ring-1 focus:ring-violet-300 focus:border-violet-300"
                          />
                          <button
                            onClick={() => { if (scenarioNewName.trim()) { saveScenario(scenarioNewName.trim()); setScenarioNewName(''); } }}
                            disabled={!scenarioNewName.trim()}
                            className="text-[10px] font-medium text-white bg-violet-500 hover:bg-violet-600 disabled:bg-gray-300 rounded-md px-2.5 py-1 transition-colors"
                          >Save</button>
                        </div>
                        {activeScenario && hasAnyAdjustments && (
                          <button
                            onClick={() => { updateScenario(activeScenario.id); setScenarioMenuOpen(false); }}
                            className="mt-1.5 w-full text-left text-[10px] text-violet-600 hover:text-violet-800 hover:bg-violet-50 rounded px-1 py-0.5"
                          >↻ Update "{activeScenario.name}" with current values</button>
                        )}
                      </div>
                      {/* Baseline option */}
                      <div className="px-1 py-1 border-b border-gray-100">
                        <button
                          onClick={() => { applyScenarioData({ salaryAdjPctByMonth: {}, collPctByMonth: {}, salaryDeptAdj: {}, vendorCatAdj: {}, vendorDetailAdj: {}, leverOverrides: {}, pipelineMinProb: 100 }); setActiveScenarioId(null); setScenarioMenuOpen(false); }}
                          className={`w-full flex items-center gap-2 px-2 py-1.5 rounded-lg transition-colors ${!activeScenarioId && !hasAnyAdjustments ? 'bg-gray-100' : 'hover:bg-gray-50'}`}
                        >
                          <span className="text-[10px] text-gray-400">{!activeScenarioId && !hasAnyAdjustments ? '●' : '○'}</span>
                          <div className="flex-1 text-left">
                            <span className="text-xs font-medium text-gray-700">Baseline</span>
                            <span className="block text-[9px] text-gray-400">No adjustments — raw budget/actuals</span>
                          </div>
                        </button>
                      </div>
                      {/* Scenario list */}
                      {companyScenarios.length > 0 && (
                        <div className="px-1 py-1">
                          <p className="text-[10px] text-gray-400 uppercase font-medium px-2 py-1">Saved Scenarios ({companyScenarios.length})</p>
                          {companyScenarios.map((s, si) => {
                            const prevScenario = si > 0 ? companyScenarios[si - 1] : null;
                            const isExpanded = expandedScenarioId === s.id;
                            return (
                            <div key={s.id} className={`rounded-lg transition-colors ${s.id === activeScenarioId ? 'bg-violet-50' : isExpanded ? 'bg-gray-50' : ''}`}>
                              <div className="relative group flex items-center gap-1 px-2 py-1.5 cursor-pointer hover:bg-gray-50/70"
                                   onClick={() => setExpandedScenarioId(prev => prev === s.id ? null : s.id)}>
                              {scenarioNameEdit === s.id ? (
                                <input
                                  type="text"
                                  defaultValue={s.name}
                                  autoFocus
                                  onClick={e => e.stopPropagation()}
                                  onBlur={e => renameScenario(s.id, e.target.value)}
                                  onKeyDown={e => { if (e.key === 'Enter') renameScenario(s.id, (e.target as HTMLInputElement).value); if (e.key === 'Escape') setScenarioNameEdit(null); }}
                                  className="flex-1 text-xs border border-violet-200 rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-violet-300"
                                />
                              ) : (
                                <>
                                  <span className="text-[9px] text-gray-400 mr-0.5">{isExpanded ? '▼' : '▶'}</span>
                                  <div className="flex-1 text-left">
                                    <span className={`text-xs font-medium ${s.id === activeScenarioId ? 'text-violet-700' : 'text-gray-700'}`}>
                                      {s.id === activeScenarioId && <span className="text-violet-500 mr-1">●</span>}
                                      {s.name}
                                    </span>
                                    <span className="block text-[9px] text-gray-400">
                                      {new Date(s.updatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })} {new Date(s.updatedAt).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })}
                                      {s.data.salaryAdjPctByMonth && Object.values(s.data.salaryAdjPctByMonth).some(v => v !== 0) && <span className="ml-1 text-amber-500">S%</span>}
                                      {s.data.salaryDeptAdj && Object.keys(s.data.salaryDeptAdj).length > 0 && <span className="ml-1 text-orange-500">D%</span>}
                                      {s.data.vendorCatAdj && Object.keys(s.data.vendorCatAdj).length > 0 && <span className="ml-1 text-teal-500">V%</span>}
                                      {s.data.collPctByMonth && Object.keys(s.data.collPctByMonth).length > 0 && <span className="ml-1 text-green-500">C%</span>}
                                    </span>
                                  </div>
                                  <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                                    {s.id === activeScenarioId ? (
                                      <button
                                        onClick={e => { e.stopPropagation(); updateScenario(s.id); }}
                                        title="Save current adjustments to this scenario"
                                        className="text-[9px] px-1.5 py-0.5 rounded bg-green-50 hover:bg-green-100 text-green-600 hover:text-green-700 font-medium border border-green-200"
                                      >Save</button>
                                    ) : (
                                      <button
                                        onClick={e => { e.stopPropagation(); loadScenario(s.id); }}
                                        title="Load this scenario"
                                        className="text-[9px] px-1.5 py-0.5 rounded hover:bg-violet-100 text-violet-500 hover:text-violet-700 font-medium"
                                      >Load</button>
                                    )}
                                    <button
                                      onClick={e => { e.stopPropagation(); setCompareScenarioId(prev => prev === s.id ? null : s.id); setScenarioMenuOpen(false); }}
                                      title="Compare in table"
                                      className={`text-[9px] px-1.5 py-0.5 rounded transition-colors ${compareScenarioId === s.id ? 'bg-blue-100 text-blue-700' : 'hover:bg-blue-50 text-gray-400 hover:text-blue-600'}`}
                                    >⇄</button>
                                    <button
                                      onClick={e => { e.stopPropagation(); setScenarioNameEdit(s.id); }}
                                      title="Rename"
                                      className="text-[9px] px-1.5 py-0.5 rounded hover:bg-gray-100 text-gray-400 hover:text-gray-600"
                                    >✎</button>
                                    <button
                                      onClick={e => { e.stopPropagation(); deleteScenario(s.id); }}
                                      title="Delete"
                                      className="text-[9px] px-1.5 py-0.5 rounded hover:bg-red-50 text-gray-400 hover:text-red-500"
                                    >✕</button>
                                    <button
                                      onClick={e => { e.stopPropagation(); if (_shareOpen === s.id) { _setShareOpen(null); } else { _setShareOpen(s.id); _loadShares(s.id); } }}
                                      title="Share"
                                      className={`text-[9px] px-1.5 py-0.5 rounded transition-colors ${_shareOpen === s.id ? 'bg-emerald-100 text-emerald-700' : 'hover:bg-emerald-50 text-gray-400 hover:text-emerald-600'}`}
                                    >👤</button>
                                  </div>
                                </>
                              )}
                              </div>
                              {/* Expanded detail — adjustments list */}
                              {isExpanded && (
                                <div className="px-3 pb-2 pt-0.5 border-t border-gray-100/50">
                                  {(() => {
                                    const vsOriginal = describeScenarioDiff(null, s.data, 'vs Baseline');
                                    const vsPrev = prevScenario ? describeScenarioDiff(prevScenario.data, s.data, `vs ${prevScenario.name}`) : null;
                                    return (
                                      <div className="space-y-1.5">
                                        <div>
                                          <p className="text-[9px] font-semibold text-violet-600 uppercase mb-0.5">{vsOriginal.label}</p>
                                          {vsOriginal.items.length === 0 ? (
                                            <p className="text-[9px] text-gray-400 italic">No adjustments (same as baseline)</p>
                                          ) : (
                                            <div className="space-y-0.5 max-h-[140px] overflow-y-auto">
                                              {vsOriginal.items.map(item => (
                                                <p key={item.key} className={`text-[9px] ${item.color}`}>• {item.desc}</p>
                                              ))}
                                            </div>
                                          )}
                                        </div>
                                        {vsPrev && (
                                          <div className="border-t border-gray-100 pt-1">
                                            <p className="text-[9px] font-semibold text-blue-600 uppercase mb-0.5">{vsPrev.label}</p>
                                            {vsPrev.items.length === 0 ? (
                                              <p className="text-[9px] text-gray-400 italic">No changes</p>
                                            ) : (
                                              <div className="space-y-0.5 max-h-[100px] overflow-y-auto">
                                                {vsPrev.items.map(item => (
                                                  <p key={item.key} className={`text-[9px] ${item.color}`}>• {item.desc}</p>
                                                ))}
                                              </div>
                                            )}
                                          </div>
                                        )}
                                        <div className="flex items-center gap-2 pt-1 border-t border-gray-100">
                                          <button
                                            onClick={e => { e.stopPropagation(); loadScenario(s.id); }}
                                            className="text-[10px] font-medium text-violet-600 hover:text-violet-800 hover:bg-violet-50 rounded px-2 py-0.5"
                                          >Load scenario</button>
                                          <button
                                            onClick={e => { e.stopPropagation(); setCompareScenarioId(prev => prev === s.id ? null : s.id); setScenarioMenuOpen(false); }}
                                            className="text-[10px] text-blue-500 hover:text-blue-700 hover:bg-blue-50 rounded px-2 py-0.5"
                                          >Compare in table</button>
                                        </div>
                                      </div>
                                    );
                                  })()}
                                </div>
                              )}
                              {_shareOpen === s.id && (
                                <div className="px-3 pb-2 pt-1 border-t border-emerald-100/50 bg-emerald-50/30 rounded-b-lg">
                                  <p className="text-[9px] font-semibold text-emerald-700 uppercase mb-1.5">Share with</p>
                                  {_bdUsers.length === 0 ? (
                                    <p className="text-[9px] text-gray-400 italic">No eligible users found</p>
                                  ) : (
                                    <div className="space-y-0.5 max-h-[140px] overflow-y-auto">
                                      {_bdUsers.map(u => {
                                        const _isSharedWith = (_shareMap[s.id] || []).some(x => x.email.toLowerCase() === u.email.toLowerCase());
                                        const _sharePendingKey = s.id + '::' + u.email.toLowerCase();
                                        const _isSharePending = !!_sharePending[_sharePendingKey];
                                        return (
                                          <button
                                            key={u.email}
                                            disabled={_isSharePending}
                                            onClick={e => { e.stopPropagation(); _toggleShare(s.id, u.email, _isSharedWith); }}
                                            className={`w-full flex items-center gap-1.5 px-2 py-1 rounded text-[10px] transition-colors ${_isSharePending ? 'opacity-60 cursor-wait' : ''} ${_isSharedWith ? 'bg-emerald-100 text-emerald-800 font-medium' : 'hover:bg-gray-100 text-gray-600'}`}
                                          >
                                            <span className="w-3 text-center">{_isSharePending ? '…' : _isSharedWith ? '✓' : '○'}</span>
                                            <span className="truncate">{u.displayName || u.email}</span>
                                          </button>
                                        );
                                      })}
                                    </div>
                                  )}
                                </div>
                              )}
                            </div>
                            );
                          })}
                        </div>
                      )}
                      {/* Shared with me */}
                      {_shared.length > 0 && (
                        <div className="px-1 py-1 border-t border-gray-100">
                          <p className="text-[10px] text-gray-400 uppercase font-medium px-2 py-1">Shared with me ({_shared.length})</p>
                          {_shared.map(s => (
                            <div key={s.ownerEmail + '-' + s.id}
                              className="rounded-lg hover:bg-violet-50/60 px-2 py-1.5 cursor-pointer group flex items-center gap-1"
                              onClick={() => { applyScenarioData(s.data); setActiveScenarioId(null); setScenarioMenuOpen(false); }}
                            >
                              <div className="flex-1 text-left">
                                <span className="text-xs font-medium text-gray-700">{s.name}</span>
                                <span className="block text-[9px] text-gray-400">
                                  by {s.ownerName} &middot; {new Date(s.updatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}
                                </span>
                              </div>
                              <span className="text-[9px] font-medium text-violet-500 opacity-0 group-hover:opacity-100 transition-opacity">Load</span>
                            </div>
                          ))}
                        </div>
                      )}
                      {/* Quick actions */}
                      {activeScenario && (
                        <div className="px-3 py-2 border-t border-gray-100">
                          <button
                            onClick={() => { setActiveScenarioId(null); setScenarioMenuOpen(false); }}
                            className="text-[10px] text-gray-500 hover:text-gray-700"
                          >Detach from scenario (keep current values)</button>
                        </div>
                      )}
                      {scenarios.length >= 1 && (
                        <div className="px-3 py-2 border-t border-gray-100">
                          <button
                            onClick={() => { setShowComparePanel(true); setScenarioMenuOpen(false); if (!compareLeftId) setCompareLeftId('__baseline__'); if (!compareRightId && scenarios.length >= 1) setCompareRightId(scenarios[0].id); }}
                            className="w-full text-left text-[10px] font-medium text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded px-2 py-1.5 flex items-center gap-1"
                          >⇄ Compare Scenarios Side by Side</button>
                        </div>
                      )}
                      {compareScenarioId && (
                        <div className="px-3 py-1.5 border-t border-gray-100 bg-blue-50/50">
                          <div className="flex items-center justify-between">
                            <span className="text-[10px] text-blue-600">⇄ Comparing: {compareScenario?.name}</span>
                            <button onClick={() => setCompareScenarioId(null)} className="text-[9px] text-blue-400 hover:text-blue-700">✕ Stop</button>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
                {/* Compare indicator — shown in header when active */}
                {compareScenario && !scenarioMenuOpen && (
                  <div className="flex items-center gap-1 text-[10px] text-blue-600 bg-blue-50 border border-blue-200 rounded-lg px-2 py-1">
                    <span>⇄ vs {compareScenario.name}</span>
                    <button onClick={() => setCompareScenarioId(null)} className="text-blue-400 hover:text-blue-700 ml-1">✕</button>
                  </div>
                )}
                {/* Download cashflow as Excel */}
                <button
                  onClick={() => {
                    const hdr = ['Month','Status','Opening Balance','Inflows (AR)','Pipeline','Churn','Total Inflows','Salary','Vendors','Total Outflow','Net','Reval Impact','Closing Balance'];
                    const data = [hdr, ...cashflowForecast.map(r => [
                      r.month, r.isPast ? 'ACTUAL' : r.isCurrent ? 'CURRENT' : 'PROJECTED',
                      r.openingBalance, r.collections, r.pipelineWeighted, r.churnDeduction, r.collections + r.pipelineWeighted - r.churnDeduction, r.salary, r.vendors, r.totalOutflow, r.net, r.revalImpact, r.closingBalance
                    ]), ['TOTAL', '', '', cashflowForecast.reduce((s,r)=>s+r.collections,0), cashflowForecast.reduce((s,r)=>s+r.pipelineWeighted,0), cashflowForecast.reduce((s,r)=>s+r.churnDeduction,0), cashflowForecast.reduce((s,r)=>s+r.collections+r.pipelineWeighted-r.churnDeduction,0), cashflowForecast.reduce((s,r)=>s+r.salary,0), cashflowForecast.reduce((s,r)=>s+r.vendors,0), cashflowForecast.reduce((s,r)=>s+r.totalOutflow,0), cashflowForecast.reduce((s,r)=>s+r.net,0), cashflowForecast.reduce((s,r)=>s+r.revalImpact,0), cashflowForecast[cashflowForecast.length-1]?.closingBalance||0]];
                    const ws = XLSX.utils.aoa_to_sheet(data);
                    ws['!cols'] = [{ wch: 18 }, { wch: 10 }, ...Array(11).fill({ wch: 16 })];
                    // EUR format for numeric columns
                    for (let r = 1; r <= data.length - 1; r++) { for (let c = 2; c <= 12; c++) { const cell = ws[XLSX.utils.encode_cell({ r, c })]; if (cell && typeof cell.v === 'number') cell.z = '#,##0'; } }
                    const wb = XLSX.utils.book_new();
                    XLSX.utils.book_append_sheet(wb, ws, 'Cashflow Forecast');
                    XLSX.writeFile(wb, `Cashflow_Forecast_${new Date().toISOString().slice(0,10)}.xlsx`);
                  }}
                  className="text-[10px] text-green-600 hover:text-green-800 bg-green-50 border border-green-200 rounded-lg px-2 py-1 transition-colors"
                  title="Download as Excel"
                >📥</button>
                <div className="flex bg-gray-100 rounded-lg p-0.5">
                  <button onClick={() => setCurrency('EUR')}
                          className={`px-2.5 py-1 text-[10px] font-medium rounded-md transition-colors ${currency === 'EUR' ? 'bg-white shadow-sm text-violet-700' : 'text-gray-400 hover:text-gray-600'}`}>EUR</button>
                  <button onClick={() => setCurrency('ILS')}
                          className={`px-2.5 py-1 text-[10px] font-medium rounded-md transition-colors ${currency === 'ILS' ? 'bg-white shadow-sm text-blue-700' : 'text-gray-400 hover:text-gray-600'}`}>ILS</button>
                </div>
              </div>
            </div>

            {/* Cashflow Table — Opening → Inflows → Outflows → Net → Closing */}
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-left text-gray-400 uppercase border-b-2 border-gray-200">
                    <th className="pb-2 pr-1 whitespace-nowrap">Month</th>
                    <th className="pb-2 pr-1 text-right whitespace-nowrap">Opening Bal.</th>
                    <th className="pb-2 pr-1 text-right text-green-600 whitespace-nowrap">Inflows (AR)</th>
                    <th className="pb-2 pr-1 text-right text-teal-600 whitespace-nowrap">Pipeline<div className="text-[8px] font-normal normal-case text-gray-400">{pipelineMinProb}% • {cashflowForecast[0]?.pipelineHistWinRate || 33}% wr • +{cashflowForecast[0]?.pipelineDelayMonths || 2}m</div></th>
                    <th className="pb-2 pr-1 text-right text-orange-600 whitespace-nowrap">Churn</th>
                    <th className="pb-2 pr-1 text-right text-emerald-700 whitespace-nowrap">Total Inflows</th>
                    <th className="pb-2 pr-1 text-right text-amber-600 whitespace-nowrap">Salary</th>
                    <th className="pb-2 pr-1 text-right text-violet-600 whitespace-nowrap">Vendors</th>
                    <th className="pb-2 pr-1 text-right text-red-600 whitespace-nowrap">Total Outflow</th>
                    <th className="pb-2 pr-1 text-right whitespace-nowrap">Net</th>
                    <th className="pb-2 pr-1 text-right text-amber-500 whitespace-nowrap">Reval</th>
                    <th className="pb-2 pr-1 text-right text-blue-700 whitespace-nowrap">Closing Balance</th>
                  </tr>
                </thead>
                <tbody>
                  {cashflowForecast.map((r, i) => (
                    <tr key={i} className={`border-b border-gray-100 h-[52px] ${r.isCurrent ? 'bg-blue-50/30' : r.isPast ? 'bg-gray-50/50' : ''}`}>
                      <td className="py-2.5 pr-3 whitespace-nowrap">
                        <span className="font-medium text-gray-700">{r.month}</span>
                        {r.isPast
                          ? <span className="ml-2 text-[10px] bg-green-100 text-green-700 px-1.5 py-0.5 rounded-full font-medium">ACTUAL</span>
                          : r.isCurrent
                          ? <span className="ml-2 text-[10px] bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded-full font-medium">CURRENT</span>
                          : <span className="ml-2 text-[10px] bg-violet-100 text-violet-700 px-1.5 py-0.5 rounded-full font-medium">PROJECTED</span>
                        }
                      </td>
                      <td className={`py-2.5 pr-1 text-right font-medium ${r.openingBalance >= 0 ? 'text-gray-700' : 'text-red-600'} ${(r.isCurrent || r.isPast) ? 'cursor-pointer hover:underline' : ''}`}
                          onClick={() => {
                            if (!(r.isCurrent || r.isPast)) return;
                            // Get end of previous month
                            const [yr, mn] = r.mKey.split('-').map(Number);
                            const prevMonthEnd = new Date(yr, mn - 1, 0); // day 0 of current month = last day of prev month
                            const asOfStr = `${prevMonthEnd.getFullYear()}-${String(prevMonthEnd.getMonth()+1).padStart(2,'0')}-${String(prevMonthEnd.getDate()).padStart(2,'0')}`;
                            const label = prevMonthEnd.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
                            setBankBreakdownAsOf({ date: asOfStr, label, accounts: 'loading' });
                            fetch(`/api/ns-bank-accounts-asof?date=${asOfStr}`).then(res => res.json()).then(j => {
                              setBankBreakdownAsOf(prev => prev ? { ...prev, accounts: j.data || [] } : null);
                            }).catch(() => setBankBreakdownAsOf(prev => prev ? { ...prev, accounts: [] } : null));
                          }}>
                        {fmtCFull(r.openingBalance, r.openingBalanceILS)}
                        {r.isCurrent && prevMonthEndBalance && <span className="text-[9px] text-amber-500 ml-0.5" title="Anchored to actual bank balance">*</span>}
                        {(r.isCurrent || r.isPast) && <span className="text-[10px] text-blue-400 ml-1">→</span>}
                      </td>
                      <td className="py-2.5 pr-1 text-right text-green-600 font-medium">
                        <div className="flex items-center justify-end gap-1">
                          {!r.isPast && (r.collectionsForecast > 0 || r.isCurrent) && (
                            <div className="flex items-center gap-0.5 mr-1">
                              <button onClick={() => setCollPctByMonth(prev => ({ ...prev, [i]: (prev[i] ?? 100) - 5 }))}
                                      className="w-4 h-4 rounded bg-gray-100 hover:bg-green-100 text-gray-500 text-[10px] flex items-center justify-center font-bold leading-none">−</button>
                              <input type="text" inputMode="numeric" value={collPctByMonth[i] ?? 100}
                                     onChange={e => { const v = e.target.value; const n = parseInt(v); if (!isNaN(n)) setCollPctByMonth(prev => ({ ...prev, [i]: n })); }}
                                     className={`w-8 text-center text-[10px] font-semibold border rounded px-0.5 py-0 ${(collPctByMonth[i] ?? 100) !== 100 ? 'text-green-700 border-green-200 bg-green-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                              <span className="text-[10px] text-gray-400">%</span>
                              <button onClick={() => setCollPctByMonth(prev => ({ ...prev, [i]: (prev[i] ?? 100) + 5 }))}
                                      className="w-4 h-4 rounded bg-gray-100 hover:bg-green-100 text-gray-500 text-[10px] flex items-center justify-center font-bold leading-none">+</button>
                              {(collPctByMonth[i] ?? 100) !== 100 && (
                                <button onClick={() => {
                                  const copyToNext = confirm('Also clear for remaining months?');
                                  setCollPctByMonth(prev => { const u = { ...prev }; delete u[i]; if (copyToNext) { for (let j = i + 1; j < 12; j++) delete u[j]; } return u; });
                                }} className="w-4 h-4 rounded bg-red-50 hover:bg-red-100 text-red-400 hover:text-red-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Clear adjustment">✕</button>
                              )}
                              {(collPctByMonth[i] ?? 100) !== 100 && i < 11 && (
                                <button onClick={() => setCollPctByMonth(prev => { const u = { ...prev }; for (let j = i + 1; j < 12; j++) u[j] = prev[i] ?? 100; return u; })}
                                        className="w-4 h-4 rounded bg-green-100 hover:bg-green-200 text-green-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Copy to remaining months">→</button>
                              )}
                            </div>
                          )}
                          <span className="cursor-pointer hover:underline" onClick={(e) => {
                            e.stopPropagation();
                            const revPd = sfRevenuePaid[r.mKey];
                            setForecastDrilldown({ type: 'inflows', month: r.month, mKey: r.mKey, data: {
                              actual: r.collectionsActual || actualCollections[r.mKey] || 0,
                              revenue: revPd?.revenue || 0,
                              paid: revPd?.paid || 0,
                              unpaid: revPd?.unpaid || 0,
                              unpaidCarry: r.collectionsUnpaidCarry || 0,
                              pipeline: r.collectionsPipeline || 0,
                              forecast: sfRevenue.budget?.[r.mKey]?.eur || 0,
                              target: sfRevenue.targets?.[r.mKey] || 0,
                              collections: r.collections,
                              customers: r.customers,
                              collPct: collPctByMonth[i] ?? 100,
                              isPast: r.isPast, isCurrent: r.isCurrent,
                            }});
                          }}>
                            {r.collections > 0 ? `+${fmtC(r.collections, r.collectionsILS)}` : '-'}
                          </span>
                          {r.customers > 0 && <span className="text-[10px] text-gray-400 ml-1">({r.customers})</span>}
                        </div>
                        {r.isCurrent && r.collectionsActual > 0 && (
                          <div className="text-[10px]">
                            <span className="text-green-700">Actual: {fmt(r.collectionsActual)}</span>
                            {r.collectionsRemaining > 0 && <span className="text-blue-500 ml-1">| Rem: {fmt(r.collectionsRemaining)}</span>}
                            {r.collectionsUnpaidCarry > 0 && <span className="text-amber-500 ml-1 cursor-pointer hover:underline" onClick={(e) => {
                              e.stopPropagation();
                              const carryMonth = r.collectionsUnpaidCarryMonth;
                              if (!carryMonth) return;
                              setForecastDrilldown({ type: 'inflows', month: `Unpaid Carry from ${carryMonth}`, mKey: carryMonth, data: 'loading' });
                              fetch(`/api/sf-revenue-breakdown?month=${carryMonth}&unpaidOnly=1`).then(res => res.json()).then(j => {
                                const clients = (j.data || []).filter((c: any) => c.unpaid > 0).sort((a: any, b: any) => b.unpaid - a.unpaid);
                                setForecastDrilldown(prev => prev ? { ...prev, data: { __carryClients: clients, __carryTotal: r.collectionsUnpaidCarry, __sourceMonth: carryMonth } } : null);
                              });
                            }}>| Carry: {fmt(r.collectionsUnpaidCarry)}</span>}
                          </div>
                        )}
                        {!r.isPast && (r.collectionsForecast > 0 || r.isCurrent) && (
                          <div className="text-[10px] text-gray-400">{companyConfig.hasSF ? 'Snowflake' : 'NS Budget'} forecast × {collPctByMonth[i] ?? 100}%{r.collectionsPipeline > 0 ? ` + pipeline ${fmt(r.collectionsPipeline)}` : ''}</div>
                        )}
                      </td>
                      <td className={`py-2.5 pr-1 text-right text-teal-700 font-medium ${r.pipelineWeighted > 0 ? 'cursor-pointer hover:underline' : ''}`}
                          onClick={() => r.pipelineWeighted > 0 && setForecastDrilldown({
                            type: 'pipeline', month: r.month, mKey: r.mKey,
                            data: { opps: r.pipelineOpps, weighted: r.pipelineWeighted, total: r.pipelineTotal, count: r.pipelineCount, winRate: r.pipelineHistWinRate, delayMonths: r.pipelineDelayMonths,
                              monthlyEffect: cashflowForecast.map(fr => ({ month: fr.month, mKey: fr.mKey, weighted: fr.pipelineWeighted, total: fr.pipelineTotal, count: fr.pipelineCount, isPast: fr.isPast, isCurrent: fr.isCurrent, opps: fr.pipelineOpps })) }
                          })}>
                        {r.pipelineWeighted > 0 ? (
                          <div>
                            <span>+{fmtC(r.pipelineWeighted, r.pipelineWeightedILS)}</span>
                            <div className="text-[10px] text-gray-400">
                              {fmt(r.pipelineTotal)} total • {r.pipelineCount} opp{r.pipelineCount !== 1 ? 's' : ''}
                            </div>
                          </div>
                        ) : '-'}
                      </td>
                      <td className={`py-2.5 pr-1 text-right font-medium ${r.churnDeduction > 0 ? 'text-orange-600 cursor-pointer hover:underline' : 'text-gray-300'}`}
                          onClick={() => {
                            if (r.churnDeduction <= 0) return;
                            // Show churn calculation breakdown + drilldown for most impactful recent year
                            const currentYear = new Date().getFullYear();
                            const recentYears = churnData.filter(c => c.monthlyImpact > 0).sort((a, b) => b.year - a.year);
                            const last6m = recentYears.slice(0, 3); // show last few years for context
                            setForecastDrilldown({ type: 'churn' as any, month: r.month, mKey: r.mKey, data: {
                              __churnCalc: true,
                              monthlyAvg: churnMonthlyAvg,
                              deduction: r.churnDeduction,
                              yearlyData: churnData.filter(c => c.year >= 2022),
                            }});
                            // Also load drilldown for most recent full year
                            const topYear = recentYears.length > 0 ? recentYears[0].year : currentYear - 1;
                            fetch(`/api/sf-churn-drilldown?year=${topYear}`).then(res => res.json()).then(j => {
                              setForecastDrilldown(prev => prev ? { ...prev, data: { ...(prev.data as any), drilldown: j.data || [], drilldownYear: topYear } } : null);
                            }).catch(() => {});
                          }}>
                        {r.churnDeduction > 0 ? (
                          <div>
                            <span>-{fmtC(r.churnDeduction, r.churnDeductionILS)}</span>
                            <div className="text-[10px] text-gray-400">6m avg</div>
                          </div>
                        ) : '-'}
                      </td>
                      <td className="py-2.5 pr-1 text-right text-emerald-700 font-bold whitespace-nowrap">
                        {fmtC(r.collections + r.pipelineWeighted - r.churnDeduction, r.collectionsILS + r.pipelineWeightedILS - r.churnDeductionILS)}
                      </td>
                      <td className="py-2.5 pr-1 text-right text-amber-700 font-medium whitespace-nowrap">
                        <div className="flex items-center justify-end gap-1">
                          {!r.isPast && (() => {
                            // Compute dept adj % for display
                            const effAdj2: Record<string, number> = {};
                            const adjKeys2 = Object.keys(salaryDeptAdj).filter(k => k <= r.mKey).sort();
                            for (const ak of adjKeys2) { for (const [dep, p] of Object.entries(salaryDeptAdj[ak])) { if (p !== 0) effAdj2[dep] = p; else delete effAdj2[dep]; } }
                            let deptPctStr = '';
                            if (Object.keys(effAdj2).length > 0 && salaryDeptBudgets[r.mKey] && sfSalaryBudget[r.mKey]?.eur) {
                              const delta2 = Object.entries(effAdj2).reduce((s, [dep, p]) => s + Math.round((salaryDeptBudgets[r.mKey][dep] || 0) * (p / 100)), 0);
                              if (delta2 !== 0) deptPctStr = (delta2 / sfSalaryBudget[r.mKey].eur * 100).toFixed(1);
                            }
                            const globalPct = salaryAdjPctByMonth[i] || 0;
                            const hasGlobal = globalPct !== 0;
                            const hasDept = deptPctStr !== '';
                            return (
                            <div className="flex items-center gap-0.5 mr-1">
                              <button onClick={() => setSalaryAdjPctByMonth(prev => ({ ...prev, [i]: (prev[i] || 0) - 1 }))}
                                      className="w-4 h-4 rounded bg-gray-100 hover:bg-amber-100 text-gray-500 text-[10px] flex items-center justify-center font-bold leading-none">−</button>
                              <input type="text" inputMode="numeric" value={globalPct}
                                     onChange={e => { const v = e.target.value; if (v === '' || v === '-') setSalaryAdjPctByMonth(prev => ({ ...prev, [i]: v as any })); else { const n = parseInt(v); if (!isNaN(n)) setSalaryAdjPctByMonth(prev => ({ ...prev, [i]: n })); } }}
                                     className={`w-9 text-center text-[10px] font-semibold border rounded px-0.5 py-0 ${hasGlobal ? 'text-amber-700 border-amber-200 bg-amber-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                              <span className="text-[10px] text-gray-400">%</span>
                              <button onClick={() => setSalaryAdjPctByMonth(prev => ({ ...prev, [i]: (prev[i] || 0) + 1 }))}
                                      className="w-4 h-4 rounded bg-gray-100 hover:bg-amber-100 text-gray-500 text-[10px] flex items-center justify-center font-bold leading-none">+</button>
                              {hasGlobal && (
                                <button onClick={() => {
                                  const copyToNext = confirm('Also clear for remaining months?');
                                  setSalaryAdjPctByMonth(prev => { const u = { ...prev }; delete u[i]; if (copyToNext) { for (let j = i + 1; j < 12; j++) delete u[j]; } return u; });
                                }} className="w-4 h-4 rounded bg-red-50 hover:bg-red-100 text-red-400 hover:text-red-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Clear adjustment">✕</button>
                              )}
                              {hasGlobal && i < 11 && (
                                <button onClick={() => setSalaryAdjPctByMonth(prev => { const u = { ...prev }; for (let j = i + 1; j < 12; j++) u[j] = prev[i] || 0; return u; })}
                                        className="w-4 h-4 rounded bg-amber-100 hover:bg-amber-200 text-amber-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Copy to remaining months">→</button>
                              )}
                              {hasDept && <span className={`text-[9px] font-semibold ml-0.5 px-1 py-0 rounded ${Number(deptPctStr) >= 0 ? 'text-red-600 bg-red-50' : 'text-green-700 bg-green-50'}`}>{Number(deptPctStr) >= 0 ? '+' : ''}{deptPctStr}%</span>}
                            </div>
                            );
                          })()}
                          <span className="cursor-pointer hover:underline" onClick={(e) => {
                            e.stopPropagation();
                            const adjPct = salaryAdjPctByMonth[i] || 0;
                            setForecastDrilldown({ type: 'salary', month: r.month, mKey: r.mKey, data: 'loading', adjPct });
                            if (companyConfig.hasSF) {
                              Promise.all([
                                fetch(`/api/sf-salary-breakdown?month=${r.mKey}`).then(res => res.json()),
                                fetch(`/api/sf-salary-budget-breakdown?month=${r.mKey}`).then(res => res.json()),
                                fetch(`/api/sf-headcount-events?month=${r.mKey}`).then(res => res.json()),
                              ]).then(([actRes, budRes, hcRes]) => {
                                setForecastDrilldown(prev => prev ? { ...prev, data: { actuals: actRes.data || [], budget: budRes.data || [], headcount: hcRes.data || { events: [], cumulative: [], baseline: {} } } } : null);
                                // Cache department budgets for per-dept adjustments
                                if (budRes.data && budRes.data.length > 0) {
                                  const byDept: Record<string, number> = {};
                                  for (const row of budRes.data) { byDept[row.department] = (byDept[row.department] || 0) + (row.amountEUR || 0); }
                                  setSalaryDeptBudgets(prev => ({ ...prev, [r.mKey]: byDept }));
                                }
                              });
                            } else {
                              // Non-SF subsidiary: fetch NS salary breakdown by account
                              fetch(`/api/ns-salary-breakdown?month=${r.mKey}&subsidiary=${companyConfig.subsidiary}`).then(res => res.json()).then(data => {
                                // Transform NS data to match the expected format: actuals[] and budget[] with department field
                                const actuals = (data.actuals || []).map((row: any) => ({ department: row.name || row.account, account: row.account, amountEUR: row.amountEUR || 0, amountILS: 0 }));
                                const budget = (data.budget || []).map((row: any) => ({ department: row.name || row.account, account: row.account, amountEUR: row.amountEUR || 0, amountILS: 0 }));
                                setForecastDrilldown(prev => prev ? { ...prev, data: { actuals, budget, headcount: null, __nsMode: true } } : null);
                              });
                            }
                          }}>{r.salary > 0 ? `-${fmtC(r.salary, r.salaryILS)}` : '-'}</span>
                          {!r.isPast && r.salaryBase > 0 && r.salary !== r.salaryBase && (() => {
                            const delta = r.salary - r.salaryBase;
                            return <span className={`text-[9px] font-semibold ml-1 px-1 py-0 rounded ${delta > 0 ? 'text-red-600 bg-red-50' : 'text-green-700 bg-green-50'}`}>{delta > 0 ? '+' : ''}{fmt(delta)}</span>;
                          })()}
                        </div>
                      </td>
                      <td className="py-2.5 pr-1 text-right text-violet-700 font-medium whitespace-nowrap cursor-pointer hover:underline"
                          onClick={() => {
                            // Compute historical avg from SF actuals or NS vendor history (12-month trailing)
                            const nsVendorByMonth: Record<string, number> = {};
                            vendorHistory.forEach(v => { const mk = v.paidDate.slice(0, 7); nsVendorByMonth[mk] = (nsVendorByMonth[mk] || 0) + v.amountEUR; });
                            const hasSfActuals = Object.keys(sfActualsSplit).some(k => sfActualsSplit[k]?.vendors > 0);
                            const actSrc = hasSfActuals ? sfActualsSplit : Object.fromEntries(Object.entries(nsVendorByMonth).map(([k, v]) => [k, { vendors: v, salary: salaryData.find(s => s.month === k)?.amountEUR || 0 }]));
                            const pastKeys = Object.keys(actSrc).filter(k => k < r.mKey && (actSrc[k] as any)?.vendors > 0).sort();
                            const trail = pastKeys.slice(-12);
                            const histAvg = trail.length > 0 ? Math.round(trail.reduce((s, k) => s + ((actSrc[k] as any).vendors || 0), 0) / trail.length) : 0;
                            const histMonths = trail.map(k => ({ month: k, vendors: (actSrc[k] as any).vendors || 0, salary: (actSrc[k] as any).salary || 0 }));
                            const budgetTotal = sfBudget.totalByMonth[r.mKey]?.eur || nsBudget.byMonth[r.mKey]?.vendors || 0;
                            const nsVendAct = nsVendorByMonth[r.mKey] || 0;
                            setForecastDrilldown({ type: 'vendors', month: r.month, mKey: r.mKey, data: {
                              ...(sfBudget.byMonth?.[r.mKey] || nsBudget.byMonth[r.mKey]?.categories || expenseCategories.byMonth?.[r.mKey] || {}),
                              __vendorMeta: { budgetTotal, histAvg, histMonths, actual: sfActualsSplit[r.mKey]?.vendors || nsVendAct, used: r.vendors }
                            }});
                          }}>
                        {r.vendors > 0 ? `-${fmtC(r.vendors, r.vendorsILS)}` : '-'}
                        {!r.isPast && r.vendorsBase > 0 && r.vendors !== r.vendorsBase && (() => {
                          const delta = r.vendors - r.vendorsBase;
                          const effPct = Math.round((delta / r.vendorsBase) * 100);
                          return <span className={`text-[9px] font-semibold ml-1 px-1 py-0 rounded ${delta > 0 ? 'text-red-600 bg-red-50' : 'text-green-700 bg-green-50'}`}>{effPct > 0 ? '+' : ''}{effPct}%</span>;
                        })()}
                        {r.vendors > 0 && r.vendors === r.vendorsBase && <span className="text-[10px] text-violet-400 ml-1">→</span>}
                      </td>
                      <td className="py-2.5 pr-1 text-right text-red-600 font-bold">-{fmtC(r.totalOutflow, r.totalOutflowILS)}</td>
                      <td className={`py-2.5 pr-1 text-right font-bold ${r.net >= 0 ? 'text-green-700' : 'text-red-600'}`}>{fmtC(r.net, r.netILS)}</td>
                      <td className={`py-2.5 pr-1 text-right font-medium ${r.revalImpact === 0 ? 'text-gray-300' : r.revalImpact > 0 ? 'text-amber-600' : 'text-amber-700'}`}>
                        {r.revalImpact !== 0 ? fmtC(r.revalImpact, r.revalImpactILS) : '-'}
                      </td>
                      <td className={`py-2.5 pr-1 text-right font-bold whitespace-nowrap ${r.closingBalance >= 0 ? 'text-blue-700' : 'text-red-600'}`}>
                        {fmtCFull(r.closingBalance, r.closingBalanceILS)}
                        {compareCashflow && compareCashflow[i] && (() => {
                          const delta = r.closingBalance - compareCashflow[i].closingBalance;
                          if (Math.abs(delta) < 100) return null;
                          return <span className={`text-[9px] font-medium ml-1 ${delta > 0 ? 'text-green-500' : 'text-red-400'}`}>{delta > 0 ? '▲' : '▼'}{fmt(Math.abs(delta))}</span>;
                        })()}
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr className="border-t-2 border-gray-300 font-bold bg-gray-50 whitespace-nowrap">
                    <td className="py-2.5 pr-1 text-gray-800">TOTAL</td>
                    <td className="py-2.5 pr-1"></td>
                    <td className="py-2.5 pr-1 text-right text-green-700">
                      {fmtC(cashflowForecast.reduce((s, r) => s + r.collections, 0), cashflowForecast.reduce((s, r) => s + r.collectionsILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-teal-700">
                      +{fmtC(cashflowForecast.reduce((s, r) => s + r.pipelineWeighted, 0), cashflowForecast.reduce((s, r) => s + r.pipelineWeightedILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-orange-600">
                      {cashflowForecast.reduce((s, r) => s + r.churnDeduction, 0) > 0 ? `-${fmtC(cashflowForecast.reduce((s, r) => s + r.churnDeduction, 0), cashflowForecast.reduce((s, r) => s + r.churnDeductionILS, 0))}` : '-'}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-emerald-700">
                      {fmtC(cashflowForecast.reduce((s, r) => s + r.collections + r.pipelineWeighted - r.churnDeduction, 0), cashflowForecast.reduce((s, r) => s + r.collectionsILS + r.pipelineWeightedILS - r.churnDeductionILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-amber-700">
                      -{fmtC(cashflowForecast.reduce((s, r) => s + r.salary, 0), cashflowForecast.reduce((s, r) => s + r.salaryILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-violet-700">
                      -{fmtC(cashflowForecast.reduce((s, r) => s + r.vendors, 0), cashflowForecast.reduce((s, r) => s + r.vendorsILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-red-700">
                      -{fmtC(cashflowForecast.reduce((s, r) => s + r.totalOutflow, 0), cashflowForecast.reduce((s, r) => s + r.totalOutflowILS, 0))}
                    </td>
                    <td className={`py-2.5 pr-1 text-right ${cashflowForecast.reduce((s, r) => s + r.net, 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                      {fmtC(cashflowForecast.reduce((s, r) => s + r.net, 0), cashflowForecast.reduce((s, r) => s + r.netILS, 0))}
                    </td>
                    <td className="py-2.5 pr-1 text-right text-amber-600">
                      {fmtC(cashflowForecast.reduce((s, r) => s + r.revalImpact, 0), cashflowForecast.reduce((s, r) => s + r.revalImpactILS, 0))}
                    </td>
                    <td className={`py-2.5 pr-1 text-right font-bold whitespace-nowrap ${(cashflowForecast[cashflowForecast.length - 1]?.closingBalance || 0) >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                      {fmtC(cashflowForecast[cashflowForecast.length - 1]?.closingBalance || 0, cashflowForecast[cashflowForecast.length - 1]?.closingBalanceILS || 0)}
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </div>
        )}

        {/* ── Inflows Breakdown ── */}
        {activeCompany !== 'consolidated' && cashflowForecast.length > 0 && (() => {
          const totalInflows = cashflowForecast.reduce((s, r) => s + r.collections, 0);
          // SF Revenue total — the "reference" number
          const sfRevenueTotal = Object.values(sfRevenuePaid).reduce((s, r) => s + (r.revenue || 0), 0);
          // Bridge: decompose totalInflows - sfRevenueTotal into understandable components
          const pastMonths = cashflowForecast.filter(r => r.isPast);
          // Totals for reconciliation
          const totalPipeline = cashflowForecast.reduce((s, r) => s + (r.pipelineWeighted || 0), 0);
          const totalCarry = cashflowForecast.reduce((s, r) => s + (r.collectionsUnpaidCarry || 0), 0);
          const totalCollectionAdj = totalInflows - sfRevenueTotal - totalPipeline - totalCarry;
          // Monthly breakdown data for each category
          const reconMonths = cashflowForecast.map((r, i) => {
            const revPd = sfRevenuePaid[r.mKey];
            return {
              month: r.month, mKey: r.mKey, isPast: r.isPast, isCurrent: r.isCurrent,
              revenue: revPd?.revenue || 0,
              collected: r.collections - (r.pipelineWeighted || 0) - (r.collectionsUnpaidCarry || 0),
              inflows: r.collections,
              collAdj: r.collections - (revPd?.revenue || 0) - (r.pipelineWeighted || 0) - (r.collectionsUnpaidCarry || 0),
              carry: r.collectionsUnpaidCarry || 0,
              pipeline: r.pipelineWeighted || 0,
              paid: revPd?.paid || 0,
              unpaid: revPd?.unpaid || 0,
              actualColl: r.collectionsActual || 0,
              collPct: collPctByMonth[i] ?? 100,
              collPctAdj: (collPctByMonth[i] ?? 100) !== 100 ? Math.round((revPd?.revenue || 0) * ((collPctByMonth[i] ?? 100) - 100) / 100) : 0,
              remaining: r.collectionsRemaining || 0,
            };
          });
          return (
          <ReconTable
            sfRevenueTotal={sfRevenueTotal} totalInflows={totalInflows}
            totalCollectionAdj={totalCollectionAdj} totalCarry={totalCarry} totalPipeline={totalPipeline}
            winRate={cashflowForecast[0]?.pipelineHistWinRate || 0}
            reconMonths={reconMonths} fmt={fmtFull} nsAccountId={nsAccountId} hasSF={companyConfig.hasSF}
          />
          );
        })()}

        {/* Bank Balance Breakdown Modal */}
        {showBankBreakdown && (
          <div className="fixed inset-0 bg-black/40 z-50 flex items-start justify-center pt-12 px-4" onClick={() => setShowBankBreakdown(false)}>
            <div className="bg-white rounded-xl shadow-2xl border w-full max-w-3xl max-h-[70vh] overflow-hidden" onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between px-5 py-4 border-b bg-gray-50">
                <div className="flex items-center gap-3">
                  <button onClick={() => setShowBankBreakdown(false)} className="text-gray-400 hover:text-gray-600">
                    <ChevronRight className="w-5 h-5 rotate-180" />
                  </button>
                  <div>
                    <h3 className="font-semibold text-gray-800">Bank Balance Breakdown</h3>
                    <p className="text-xs text-gray-400">As of {new Date().toLocaleDateString('en-GB', { day: '2-digit', month: 'long', year: 'numeric' })} — including FX revaluation</p>
                  </div>
                </div>
                <div className="text-right">
                  <div className="font-bold text-emerald-700">{fmt(revaluedTotalEUR)}</div>
                  <div className="text-xs text-blue-500">{fmtILS(revaluedTotalILS)}</div>
                </div>
              </div>
              <div className="overflow-auto max-h-[55vh] p-5">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left text-gray-400 uppercase border-b">
                      <th className="pb-1 pr-3">Account</th>
                      <th className="pb-1 pr-3">Account #</th>
                      <th className="pb-1 pr-3 text-right">EUR</th>
                      <th className="pb-1 pr-3 text-right">ILS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {bankAccounts
                      .filter(a => Math.abs(a.primaryBalance) > 0 || Math.abs(a.localBalance) > 0)
                      .sort((a, b) => Math.abs(b.primaryBalance) - Math.abs(a.primaryBalance))
                      .map((a, i) => {
                        const n = a.name.toUpperCase();
                        const isEurAcct = n.includes('EUR') || n.includes('EURO');
                        const rv = accountReval(a, isEurAcct);
                        return (
                        <tr key={i} className="border-b border-gray-50">
                          <td className="py-1.5 pr-3 text-gray-700">{a.name}</td>
                          <td className="py-1.5 pr-3 text-gray-400">{a.number}</td>
                          <td className={`py-1.5 pr-3 text-right font-medium ${rv.revaluedEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                            {fmt(rv.revaluedEUR)}
                            {rv.revalEUR !== 0 && <div className="text-[9px] text-amber-500">book {fmt(a.primaryBalance)}</div>}
                          </td>
                          <td className={`py-1.5 pr-3 text-right font-medium ${rv.revaluedILS >= 0 ? 'text-blue-700' : 'text-red-600'}`}>
                            {fmtILS(rv.revaluedILS)}
                            {rv.revalILS !== 0 && <div className="text-[9px] text-amber-500">book {fmtILS(a.localBalance)}</div>}
                          </td>
                        </tr>
                        );
                    })}
                  </tbody>
                  <tfoot>
                    <tr className="border-t-2 font-bold">
                      <td className="py-1.5" colSpan={2}>Total ({bankAccounts.filter(a => Math.abs(a.primaryBalance) > 0 || Math.abs(a.localBalance) > 0).length} accounts)</td>
                      <td className="py-1.5 pr-3 text-right text-emerald-700">{fmt(revaluedTotalEUR)}</td>
                      <td className="py-1.5 pr-3 text-right text-blue-700">{fmtILS(revaluedTotalILS)}</td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Historical Bank Balance Breakdown Modal (Opening Balance drilldown) */}
        {bankBreakdownAsOf && (
          <div className="fixed inset-0 bg-black/40 z-50 flex items-start justify-center pt-12 px-4" onClick={() => setBankBreakdownAsOf(null)}>
            <div className="bg-white rounded-xl shadow-2xl border w-full max-w-3xl max-h-[70vh] overflow-hidden" onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between px-5 py-4 border-b bg-gray-50">
                <div className="flex items-center gap-3">
                  <button onClick={() => setBankBreakdownAsOf(null)} className="text-gray-400 hover:text-gray-600">
                    <ChevronRight className="w-5 h-5 rotate-180" />
                  </button>
                  <div>
                    <h3 className="font-semibold text-gray-800">Bank Balance Breakdown</h3>
                    <p className="text-xs text-gray-400">As of {bankBreakdownAsOf.label} — NetSuite book balances</p>
                  </div>
                </div>
                {bankBreakdownAsOf.accounts !== 'loading' && (() => {
                  const ccKw = ['AMEX', 'MasterCard', 'Isracard', 'Visa'];
                  const all = bankBreakdownAsOf.accounts as BankAccount[];
                  const bOnly = all.filter(a => !ccKw.some(k => a.name.includes(k)));
                  const rv = (list: BankAccount[]) => list.reduce((s, a) => { const n = a.name.toUpperCase(); return s + accountReval(a, n.includes('EUR') || n.includes('EURO')).revaluedEUR; }, 0);
                  const bankEur = rv(bOnly);
                  const totalEur = rv(all);
                  const bankIls = bOnly.reduce((s, a) => s + a.localBalance, 0);
                  const totalIls = all.reduce((s, a) => s + a.localBalance, 0);
                  return (
                  <div className="text-right">
                    <div className="font-bold text-emerald-700">{fmt(bankEur)} <span className="text-[10px] font-normal text-gray-400">banks</span></div>
                    <div className="text-xs text-gray-500">{fmt(totalEur)} <span className="text-[10px] text-gray-400">incl. CC</span></div>
                    <div className="text-xs text-blue-500">{fmtILS(bankIls)} / {fmtILS(totalIls)}</div>
                  </div>
                  );
                })()}
              </div>
              <div className="overflow-auto max-h-[55vh] p-5">
                {bankBreakdownAsOf.accounts === 'loading' ? (
                  <div className="flex items-center gap-2 py-8 justify-center text-gray-400"><Loader2 className="w-5 h-5 animate-spin" /> Loading from NetSuite...</div>
                ) : (() => {
                  const ccKw = ['AMEX', 'MasterCard', 'Isracard', 'Visa'];
                  const allAccts = bankBreakdownAsOf.accounts as BankAccount[];
                  const bankOnly = allAccts.filter(a => !ccKw.some(k => a.name.includes(k)));
                  const ccOnly = allAccts.filter(a => ccKw.some(k => a.name.includes(k)));
                  const revalSum = (list: BankAccount[]) => list.reduce((s, a) => { const n = a.name.toUpperCase(); return s + accountReval(a, n.includes('EUR') || n.includes('EURO')).revaluedEUR; }, 0);
                  const ilsSum = (list: BankAccount[]) => list.reduce((s, a) => s + a.localBalance, 0);
                  const bankActiveCount = bankOnly.filter(a => Math.abs(a.primaryBalance) > 0 || Math.abs(a.localBalance) > 0).length;
                  const ccActive = ccOnly.filter(a => Math.abs(a.primaryBalance) > 0 || Math.abs(a.localBalance) > 0);
                  const renderRow = (a: BankAccount, i: number) => {
                    const n = a.name.toUpperCase();
                    const isEurAcct = n.includes('EUR') || n.includes('EURO');
                    const rv = accountReval(a, isEurAcct);
                    return (
                      <tr key={i} className="border-b border-gray-50">
                        <td className="py-1.5 pr-3 text-gray-700">{a.name}</td>
                        <td className="py-1.5 pr-3 text-gray-400">{a.number}</td>
                        <td className={`py-1.5 pr-3 text-right font-medium ${rv.revaluedEUR >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>
                          {fmt(rv.revaluedEUR)}
                          {rv.revalEUR !== 0 && <span className="text-[9px] text-gray-400 ml-1">(book {fmt(a.primaryBalance)})</span>}
                        </td>
                        <td className={`py-1.5 pr-3 text-right font-medium ${a.localBalance >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(a.localBalance)}</td>
                      </tr>
                    );
                  };
                  return (
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-gray-400 uppercase border-b">
                        <th className="pb-1 pr-3">Account</th>
                        <th className="pb-1 pr-3">Account #</th>
                        <th className="pb-1 pr-3 text-right">EUR (reval)</th>
                        <th className="pb-1 pr-3 text-right">ILS</th>
                      </tr>
                    </thead>
                    <tbody>
                      {/* ── Bank Accounts ── */}
                      <tr><td colSpan={4} className="pt-2 pb-1 font-semibold text-gray-600 text-[11px]">Bank Accounts</td></tr>
                      {bankOnly
                        .filter(a => Math.abs(a.primaryBalance) > 0 || Math.abs(a.localBalance) > 0)
                        .sort((a, b) => Math.abs(b.primaryBalance) - Math.abs(a.primaryBalance))
                        .map((a, i) => renderRow(a, i))}
                      <tr className="border-t-2 font-bold bg-gray-50">
                        <td className="py-1.5" colSpan={2}>Bank Subtotal ({bankActiveCount})</td>
                        <td className={`py-1.5 pr-3 text-right ${revalSum(bankOnly) >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(revalSum(bankOnly))}</td>
                        <td className={`py-1.5 pr-3 text-right ${ilsSum(bankOnly) >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(ilsSum(bankOnly))}</td>
                      </tr>

                      {/* ── Credit Cards ── */}
                      {ccActive.length > 0 && (<>
                        <tr><td colSpan={4} className="pt-4 pb-1 font-semibold text-gray-600 text-[11px]">Credit Cards</td></tr>
                        {ccActive
                          .sort((a, b) => Math.abs(b.primaryBalance) - Math.abs(a.primaryBalance))
                          .map((a, i) => renderRow(a, 1000 + i))}
                        <tr className="border-t-2 font-bold bg-gray-50">
                          <td className="py-1.5" colSpan={2}>CC Subtotal ({ccActive.length})</td>
                          <td className={`py-1.5 pr-3 text-right ${revalSum(ccOnly) >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(revalSum(ccOnly))}</td>
                          <td className={`py-1.5 pr-3 text-right ${ilsSum(ccOnly) >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(ilsSum(ccOnly))}</td>
                        </tr>
                      </>)}
                    </tbody>
                    <tfoot>
                      <tr className="border-t-2 border-gray-800 font-bold text-sm">
                        <td className="py-2" colSpan={2}>Grand Total</td>
                        <td className={`py-2 pr-3 text-right ${revalSum(allAccts) >= 0 ? 'text-emerald-700' : 'text-red-600'}`}>{fmt(revalSum(allAccts))}</td>
                        <td className={`py-2 pr-3 text-right ${ilsSum(allAccts) >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmtILS(ilsSum(allAccts))}</td>
                      </tr>
                    </tfoot>
                  </table>
                  );
                })()}
              </div>
            </div>
          </div>
        )}

        {/* Forecast Cell Drilldown Modal */}
        {forecastDrilldown && (
          <div className="fixed inset-0 bg-black/40 z-50 flex items-start justify-center pt-12 px-4" onClick={() => setForecastDrilldown(null)}>
            <div className="bg-white rounded-xl shadow-2xl border w-full max-w-4xl max-h-[85vh] overflow-hidden" onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between px-5 py-4 border-b bg-gray-50">
                <div className="flex items-center gap-3">
                  <button onClick={() => {
                    if (Array.isArray(forecastDrilldown.data) && forecastDrilldown.categoryData) {
                      setForecastDrilldown(prev => prev ? { ...prev, data: prev.categoryData, categoryData: undefined } : null);
                    } else {
                      setForecastDrilldown(null);
                    }
                  }} className="text-gray-400 hover:text-gray-600">
                    <ChevronRight className="w-5 h-5 rotate-180" />
                  </button>
                  <h3 className="font-semibold text-gray-800">
                    {forecastDrilldown.type === 'churn' ? 'Churn Deduction Calculation' : forecastDrilldown.type === 'vendors' ? 'Vendor Expenses' : forecastDrilldown.type === 'pipeline' ? 'Pipeline Opportunities' : forecastDrilldown.type === 'inflows' ? (forecastDrilldown.data?.__carryClients ? 'Unpaid Revenue Carry' : 'Revenue Forecast') : 'Salary'} — {forecastDrilldown.month}{forecastDrilldown.type === 'salary' && forecastDrilldown.adjPct ? ` (${forecastDrilldown.adjPct > 0 ? '+' : ''}${forecastDrilldown.adjPct}%)` : ''}
                  </h3>
                </div>
              </div>
              <div className="overflow-auto max-h-[72vh] p-5">
                {forecastDrilldown.data === 'loading' && (
                  <div className="flex items-center gap-2 py-8 justify-center text-gray-400"><Loader2 className="w-5 h-5 animate-spin" /> Loading from Snowflake...</div>
                )}
                {/* ── Churn calculation breakdown ── */}
                {forecastDrilldown.type === 'churn' && forecastDrilldown.data && forecastDrilldown.data !== 'loading' && forecastDrilldown.data.__churnCalc && (() => {
                  const cd = forecastDrilldown.data as { monthlyAvg: number; deduction: number; yearlyData: any[]; drilldown?: any[]; drilldownYear?: number };
                  return (
                    <div className="space-y-4">
                      <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
                        <h4 className="font-semibold text-orange-800 mb-2">How is churn deduction calculated?</h4>
                        <div className="text-sm text-gray-700 space-y-1">
                          <p>1. Query all customers who churned in the <b>last 6 months</b> (from DIM_CUSTOMER.CHURN_DATE)</p>
                          <p>2. For each churned customer, get their <b>last month's revenue</b> before churn (actual run-rate being lost)</p>
                          <p>3. Sum all lost monthly revenue and <b>divide by 6</b> to get the monthly average</p>
                          <p className="mt-2 font-semibold text-orange-800">Result: <span className="text-lg">{fmt(cd.monthlyAvg)}/month</span> deducted from each projected month</p>
                        </div>
                      </div>
                      <div>
                        <h4 className="font-semibold text-gray-700 mb-2">Yearly Churn Context</h4>
                        <table className="w-full text-sm">
                          <thead><tr className="border-b text-gray-500 text-xs">
                            <th className="py-1 text-left">Year</th><th className="py-1 text-right">Churned</th><th className="py-1 text-right">Lost Revenue (12m)</th><th className="py-1 text-right">Monthly Impact</th><th className="py-1 text-right">Active</th><th className="py-1 text-right">Client %</th>
                          </tr></thead>
                          <tbody>{(cd.yearlyData || []).map((y: any) => (
                            <tr key={y.year} className="border-b border-gray-100">
                              <td className="py-1.5 font-medium">{y.year}{y.monthsCount < 12 ? ` (${y.monthsCount}m)` : ''}</td>
                              <td className="py-1.5 text-right text-red-600">{y.churnedClients}</td>
                              <td className="py-1.5 text-right">{fmt(y.lostRevenue)}</td>
                              <td className="py-1.5 text-right font-medium text-orange-600">{fmt(y.monthlyImpact)}/mo</td>
                              <td className="py-1.5 text-right">{y.totalCustomers}</td>
                              <td className="py-1.5 text-right">{y.clientChurnPct}%</td>
                            </tr>
                          ))}</tbody>
                        </table>
                      </div>
                      {cd.drilldown && cd.drilldown.length > 0 && (
                        <div>
                          <h4 className="font-semibold text-gray-700 mb-2">Top Churned Customers — {cd.drilldownYear}</h4>
                          <table className="w-full text-sm">
                            <thead><tr className="border-b text-gray-500 text-xs">
                              <th className="py-1 text-left">#</th><th className="py-1 text-left">Customer</th><th className="py-1 text-right">Last Mo Rev</th><th className="py-1 text-right">12m Rev</th><th className="py-1 text-right">Churn Date</th><th className="py-1 text-left">Region</th>
                            </tr></thead>
                            <tbody>{cd.drilldown.slice(0, 15).map((c: any, i: number) => (
                              <tr key={i} className="border-b border-gray-100">
                                <td className="py-1 text-gray-400">{i + 1}</td>
                                <td className="py-1 font-medium">{c.name}</td>
                                <td className="py-1 text-right text-orange-600">{fmt(c.lastMonthRev)}</td>
                                <td className="py-1 text-right">{fmt(c.total12mRev)}</td>
                                <td className="py-1 text-right text-gray-500">{c.churnDate}</td>
                                <td className="py-1 text-gray-500">{c.region}</td>
                              </tr>
                            ))}</tbody>
                          </table>
                          {cd.drilldown.length > 15 && <p className="text-xs text-gray-400 mt-1">+ {cd.drilldown.length - 15} more</p>}
                        </div>
                      )}
                    </div>
                  );
                })()}
                {/* ── Salary drilldown: actuals + budget breakdown ── */}
                {forecastDrilldown.type === 'salary' && forecastDrilldown.data && forecastDrilldown.data !== 'loading' && forecastDrilldown.data.budget !== undefined && (() => {
                  const d = forecastDrilldown.data as { actuals: any[]; budget: any[]; headcount?: { events: any[]; cumulative: any[]; baseline: { headcount: number; monthlyBase: number } } };
                  const hasActuals = d.actuals.length > 0;
                  const hasBudget = d.budget.length > 0;
                  const hc = d.headcount;
                  const adj = forecastDrilldown.adjPct || 0;
                  const multiplier = 1 + (adj / 100);
                  const budgetTotal = d.budget.reduce((s: number, r: any) => s + (r.amountEUR || 0), 0);
                  const budgetTotalILS = d.budget.reduce((s: number, r: any) => s + (r.amountILS || 0), 0);
                  const adjustedTotal = Math.round(budgetTotal * multiplier);
                  const adjustedTotalILS = Math.round(budgetTotalILS * multiplier);
                  // Compute per-department lever deltas from per-employee overrides
                  const deptLeverDelta: Record<string, number> = {}; // department → ILS delta
                  const cachedLeverDetails = (forecastDrilldown.data?.__leverDetails || {}) as Record<string, any[]>;
                  const mKeyPrefix = forecastDrilldown.mKey;
                  for (const [leverKey, rowOvr] of Object.entries(leverOverrides)) {
                    if (!Object.keys(rowOvr).length) continue;
                    const detail = cachedLeverDetails[leverKey];
                    if (!detail) continue;
                    const filtered = detail.filter((dd: any) => dd.month >= mKeyPrefix || dd.status === 'Open' || !dd.employeeId);
                    const leverType = leverKey.split('/')[0]; // 'increase' or 'decrease'
                    for (const [idxStr, newCost] of Object.entries(rowOvr)) {
                      const di = parseInt(idxStr);
                      const dd = filtered[di];
                      if (!dd) continue;
                      const dept = dd.department || 'Unknown';
                      const costDelta = (newCost as number) - dd.cost; // ILS delta
                      // For decreases (terminations), the budget impact is inverted
                      const signedDelta = leverType === 'increase' ? costDelta : -costDelta;
                      deptLeverDelta[dept] = (deptLeverDelta[dept] || 0) + signedDelta;
                    }
                  }
                  const hasLeverOverrides = Object.keys(deptLeverDelta).length > 0;
                  // Per-department % adjustments
                  const monthDeptAdj2 = salaryDeptAdj[forecastDrilldown.mKey] || {};
                  const deptAdjDeltaByDept: Record<string, number> = {};
                  const deptBudgetTotals: Record<string, number> = {};
                  for (const row of d.budget) { deptBudgetTotals[row.department] = (deptBudgetTotals[row.department] || 0) + (row.amountEUR || 0); }
                  for (const [dept, pct] of Object.entries(monthDeptAdj2)) {
                    if (pct !== 0) deptAdjDeltaByDept[dept] = Math.round((deptBudgetTotals[dept] || 0) * (pct / 100));
                  }
                  const totalDeptAdjDelta = Object.values(deptAdjDeltaByDept).reduce((s, d) => s + d, 0);
                  const hasDeptAdj2 = totalDeptAdjDelta !== 0;
                  // SF overrides from Google Sheets (OVERIDE_TEMP) for this month
                  const monthOverrides = sfSalaryOverrides.filter(o => o.mKey === forecastDrilldown.mKey);
                  const sfOverrideTotal = monthOverrides.reduce((s, o) => s + (o.mode === 'Override' ? (o.newVal - o.oldVal) : o.amountEUR), 0);
                  const hasSfOverrides = monthOverrides.length > 0;
                  const hasAnyAdjustment = adj !== 0 || hasLeverOverrides || hasSfOverrides || hasDeptAdj2;
                  const actualTotal = d.actuals.reduce((s: number, r: any) => s + (r.amountEUR || 0), 0);
                  const finalBudget = adjustedTotal; // already includes SF overrides (applied server-side) + manual adj
                  const ilsRate = adjustedCurrent > 0 ? adjustedCurrentLocal / adjustedCurrent : 3.7;
                  const toILS = (eur: number) => Math.round(eur * ilsRate);
                  return (
                    <div className="space-y-4">
                      {/* Summary box */}
                      <div className="bg-gray-50 rounded-lg p-3">
                        <p className="text-xs text-gray-400 mb-2 uppercase">Summary{hasAnyAdjustment ? ' — adjustments applied' : ''}</p>
                        <table className="w-full text-xs">
                          <tbody>
                            <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600">Budget (original)</td><td className="py-1.5 text-right"><span className="font-bold text-violet-700">{fmt(budgetTotal)}</span><br/><span className="text-[10px] text-gray-400">{fmtILS(toILS(budgetTotal))}</span></td></tr>
                            {hasSfOverrides ? (
                              <>
                                {monthOverrides.map((ov, oi) => (
                                  <tr key={oi} className="border-b border-orange-200 bg-orange-50">
                                    <td className="py-2 pl-3">
                                      <div className="flex items-center gap-2">
                                        <span className="inline-block w-2 h-2 rounded-full bg-orange-500"></span>
                                        <span className="text-orange-800 font-medium">Snowflake Override</span>
                                        <span className="text-orange-600">{ov.department || ov.account}</span>
                                        {ov.comments && <span className="text-[10px] text-orange-500 italic">— {ov.comments}</span>}
                                        <span className="text-[9px] bg-orange-200 text-orange-700 px-1.5 py-0.5 rounded font-medium">{ov.mode}</span>
                                        <span className="text-[9px] text-orange-400">via Google Sheets</span>
                                      </div>
                                    </td>
                                    <td className={`py-2 text-right ${ov.mode === 'Override' ? 'text-orange-700' : (ov.amountEUR >= 0 ? 'text-red-600' : 'text-green-700')}`}>
                                      <span className="font-bold text-sm">{ov.mode === 'Override' ? fmt(ov.newVal - ov.oldVal) : `${ov.amountEUR >= 0 ? '+' : ''}${fmt(ov.amountEUR)}`}</span>
                                      <br/><span className="text-[10px] opacity-60">{fmtILS(toILS(ov.mode === 'Override' ? ov.newVal - ov.oldVal : ov.amountEUR))}</span>
                                    </td>
                                  </tr>
                                ))}
                              </>
                            ) : (
                              <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600 pl-3 text-gray-400">{forecastDrilldown.data?.__nsMode ? 'NS Override' : 'Snowflake Override (Google Sheets)'}</td><td className="py-1.5 text-right text-gray-400">-</td></tr>
                            )}
                            <tr className="border-b border-gray-200">
                              <td className="py-1.5 text-gray-600 pl-3">{adj !== 0 ? <span className="text-blue-700">Manual adjustment ({adj > 0 ? '+' : ''}{adj}%)</span> : <span className="text-gray-400">Manual adjustment (0%)</span>}</td>
                              <td className={`py-1.5 text-right ${adj !== 0 ? (adjustedTotal - budgetTotal >= 0 ? 'text-red-600' : 'text-green-700') : 'text-gray-400'}`}>{adj !== 0 ? <><span className="font-bold">{adjustedTotal - budgetTotal >= 0 ? '+' : ''}{fmt(adjustedTotal - budgetTotal)}</span><br/><span className="text-[10px] opacity-60">{fmtILS(toILS(adjustedTotal - budgetTotal))}</span></> : '-'}</td>
                            </tr>
                            {hasDeptAdj2 ? (
                              <tr className="border-b border-amber-200 bg-amber-50/50">
                                <td className="py-1.5 pl-3">
                                  <span className="text-amber-700 font-medium">Department adjustments</span>
                                  <span className="text-[10px] text-amber-500 ml-1">({budgetTotal > 0 ? (totalDeptAdjDelta > 0 ? '+' : '') + (totalDeptAdjDelta / budgetTotal * 100).toFixed(1) + '% of total' : ''})</span>
                                </td>
                                <td className={`py-1.5 text-right ${totalDeptAdjDelta >= 0 ? 'text-red-600' : 'text-green-700'}`}><span className="font-bold">{totalDeptAdjDelta >= 0 ? '+' : ''}{fmt(totalDeptAdjDelta)}</span><br/><span className="text-[10px] opacity-60">{fmtILS(toILS(totalDeptAdjDelta))}</span></td>
                              </tr>
                            ) : (
                              <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600 pl-3 text-gray-400">Department adjustments</td><td className="py-1.5 text-right text-gray-400">-</td></tr>
                            )}
                            {(adj !== 0 || hasSfOverrides || hasDeptAdj2) && (
                              <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600 font-semibold">Budget (adjusted)</td><td className="py-1.5 text-right"><span className="font-bold text-green-700">{fmt(adjustedTotal + totalDeptAdjDelta)}</span><br/><span className="text-[10px] text-gray-400">{fmtILS(toILS(adjustedTotal + totalDeptAdjDelta))}</span></td></tr>
                            )}
                            {hasActuals && <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600">Actual ({forecastDrilldown.data?.__nsMode ? 'NetSuite' : 'Snowflake'})</td><td className="py-1.5 text-right"><span className="font-bold text-amber-700">{fmt(actualTotal)}</span><br/><span className="text-[10px] text-gray-400">{fmtILS(toILS(actualTotal))}</span></td></tr>}
                            {hasActuals && (() => { const variance = (hasAnyAdjustment ? adjustedTotal + totalDeptAdjDelta : budgetTotal) - actualTotal; return <tr><td className="py-1.5 text-gray-600">Variance (Budget − Actual)</td><td className={`py-1.5 text-right ${variance >= 0 ? 'text-green-700' : 'text-red-600'}`}><span className="font-bold">{variance >= 0 ? '+' : ''}{fmt(variance)}</span><br/><span className="text-[10px] opacity-60">{fmtILS(toILS(variance))}</span></td></tr>; })()}
                          </tbody>
                        </table>
                      </div>
                      {/* Per-department % adjustment (projected months only) */}
                      {hasBudget && !forecastDrilldown.data?.__nsMode && (() => {
                        const now2 = new Date();
                        const curMKey = `${now2.getFullYear()}-${String(now2.getMonth() + 1).padStart(2, '0')}`;
                        const isProjected = forecastDrilldown.mKey > curMKey;
                        if (!isProjected) return null;
                        // Group budget by department
                        const deptTotals: Record<string, number> = {};
                        for (const row of d.budget) { deptTotals[row.department] = (deptTotals[row.department] || 0) + (row.amountEUR || 0); }
                        const deptEntries = Object.entries(deptTotals).sort((a, b) => b[1] - a[1]);
                        const mKey = forecastDrilldown.mKey;
                        // Compute effective adjustments (cascade from earlier months)
                        const effectiveAdj: Record<string, { pct: number; inherited: boolean; fromMonth?: string }> = {};
                        const allAdjMKeys = Object.keys(salaryDeptAdj).filter(k => k <= mKey).sort();
                        for (const adjMKey of allAdjMKeys) {
                          for (const [dept, pct] of Object.entries(salaryDeptAdj[adjMKey])) {
                            if (pct !== 0) effectiveAdj[dept] = { pct, inherited: adjMKey !== mKey, fromMonth: adjMKey };
                            else delete effectiveAdj[dept];
                          }
                        }
                        const hasAnyDeptAdj = Object.keys(effectiveAdj).length > 0;
                        const totalDeptImpact = Object.entries(effectiveAdj).reduce((s, [dep, { pct }]) => s + Math.round((deptTotals[dep] || 0) * (pct / 100)), 0);
                        const totalSalaryPct = budgetTotal > 0 ? (totalDeptImpact / budgetTotal * 100) : 0;
                        return (
                          <div className="bg-amber-50/50 border border-amber-200 rounded-lg p-3 mb-3">
                            <div className="flex items-center justify-between mb-2">
                              <p className="text-xs text-amber-600 font-medium">Department Adjustments — {forecastDrilldown.month}</p>
                              {hasAnyDeptAdj && <span className="text-[10px] text-amber-500">≈ {totalSalaryPct >= 0 ? '+' : ''}{totalSalaryPct.toFixed(1)}% of total salary</span>}
                            </div>
                            <table className="w-full text-xs">
                              <thead><tr className="text-left text-[10px] text-amber-500 uppercase border-b border-amber-200">
                                <th className="pb-1 pr-2">Department</th>
                                <th className="pb-1 pr-2 text-right">Budget EUR</th>
                                <th className="pb-1 pr-2 text-center w-[120px]">Adjust %</th>
                                <th className="pb-1 text-right">Impact</th>
                              </tr></thead>
                              <tbody>
                                {deptEntries.map(([dept, total]) => {
                                  const eff = effectiveAdj[dept];
                                  const pct = eff?.pct || 0;
                                  const inherited = eff?.inherited || false;
                                  const impact = Math.round(total * (pct / 100));
                                  return (
                                    <tr key={dept} className={`border-b border-amber-100 ${inherited && pct !== 0 ? 'bg-amber-50/80' : ''}`}>
                                      <td className="py-1.5 pr-2 text-gray-700 font-medium">{dept}</td>
                                      <td className="py-1.5 pr-2 text-right text-violet-700">{fmt(total)}</td>
                                      <td className="py-1.5 pr-2">
                                        <div className="flex items-center justify-center gap-0.5">
                                          <button onClick={() => setSalaryDeptAdj(prev => ({ ...prev, [mKey]: { ...(prev[mKey] || {}), [dept]: pct - 1 } }))}
                                                  className="w-5 h-5 rounded bg-white hover:bg-amber-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-amber-200">−</button>
                                          <input type="text" inputMode="numeric" value={pct}
                                                 onChange={e => { const v = e.target.value; if (v === '' || v === '-') { setSalaryDeptAdj(prev => ({ ...prev, [mKey]: { ...(prev[mKey] || {}), [dept]: v as any } })); return; } const n = parseInt(v); if (!isNaN(n)) setSalaryDeptAdj(prev => ({ ...prev, [mKey]: { ...(prev[mKey] || {}), [dept]: n } })); }}
                                                 className={`w-10 text-center text-[11px] font-semibold border rounded px-0.5 py-0.5 ${pct !== 0 ? 'text-amber-700 border-amber-300 bg-amber-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                                          <span className="text-[10px] text-gray-400">%</span>
                                          <button onClick={() => setSalaryDeptAdj(prev => ({ ...prev, [mKey]: { ...(prev[mKey] || {}), [dept]: pct + 1 } }))}
                                                  className="w-5 h-5 rounded bg-white hover:bg-amber-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-amber-200">+</button>
                                        </div>
                                        {inherited && pct !== 0 && <div className="text-[9px] text-amber-400 text-center mt-0.5">from {new Date(eff.fromMonth + '-01').toLocaleDateString('en-GB', { month: 'short' })}</div>}
                                      </td>
                                      <td className={`py-1.5 text-right font-bold ${pct === 0 ? 'text-gray-300' : impact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                                        {pct === 0 ? '—' : `${impact >= 0 ? '+' : ''}${fmt(impact)}`}
                                      </td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                              {hasAnyDeptAdj && (
                                <tfoot><tr className="border-t-2 border-amber-300 font-bold">
                                  <td className="py-1.5" colSpan={2}>Total Department Adjustment</td>
                                  <td className="py-1.5 text-center">
                                    <button onClick={() => setSalaryDeptAdj({})}
                                            className="text-[9px] text-red-500 hover:text-red-700 underline">reset all</button>
                                  </td>
                                  <td className={`py-1.5 text-right font-bold ${totalDeptImpact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                                    {totalDeptImpact >= 0 ? '+' : ''}{fmt(totalDeptImpact)}
                                  </td>
                                </tr></tfoot>
                              )}
                            </table>
                          </div>
                        );
                      })()}
                      {hasBudget && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">{forecastDrilldown.data?.__nsMode ? 'Budget Breakdown (NetSuite budgetsmachine)' : `Budget Breakdown (Snowflake FCT_BUDGET — levers, new hires, etc.)${hasAnyAdjustment ? ` — showing original → adjusted` : ''}${adj !== 0 ? ` (${adj > 0 ? '+' : ''}${adj}%)` : ''}${hasLeverOverrides ? ' + lever overrides' : ''}`}</p>
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Department</th><th className="pb-1 pr-2">Account #</th><th className="pb-1 pr-2">Name</th>
                              {hasAnyAdjustment ? (<><th className="pb-1 pr-2 text-right">Budget EUR</th><th className="pb-1 pr-2 text-right">Budget ILS</th><th className="pb-1 pr-2 text-right">Adjusted EUR</th><th className="pb-1 pr-2 text-right">Adjusted ILS</th></>) : (<><th className="pb-1 pr-2 text-right">EUR</th><th className="pb-1 pr-2 text-right">ILS</th></>)}
                              {forecastDrilldown.mKey >= `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}` && <th className="pb-1 pr-2 text-right">%</th>}
                            </tr></thead>
                            <tbody>
                              {d.budget.map((r: any, idx: number) => {
                                const budgetBillKey = `budget__${r.account}__${r.department}`;
                                const isBudgetBillExpanded = forecastDrilldown.data?.__expandedBillRow === budgetBillKey;
                                return (
                                <Fragment key={idx}>
                                <tr className="border-b border-gray-50 cursor-pointer hover:bg-violet-50 transition-colors"
                                    onClick={() => {
                                      if (isBudgetBillExpanded) {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedBillRow: null, __rowBills: null } } : null);
                                      } else {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedBillRow: budgetBillKey, __rowBills: null } } : null);
                                        if (r.account) {
                                          fetch(`/api/ns-vendor-bills?accountId=${r.account}&month=${forecastDrilldown.mKey}`).then(res => res.json()).then(j => {
                                            setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __rowBills: j.data || [], __rowNsAcctId: j.nsAcctId } } : null);
                                          });
                                        }
                                      }
                                    }}>
                                  <td className="py-1.5 pr-2 text-gray-500"><span className="text-gray-400 mr-1">{isBudgetBillExpanded ? '▼' : '▶'}</span>{r.department}</td>
                                  <td className="py-1.5 pr-2 font-mono text-violet-600">{r.account}</td>
                                  <td className="py-1.5 pr-2 text-gray-700">{r.name}</td>
                                  {(() => {
                                    const deptDelta = deptLeverDelta[r.department] || 0; // ILS delta from lever overrides
                                    const eurIlsRatio = r.amountILS > 0 ? r.amountEUR / r.amountILS : (1/3.75);
                                    const deptDeltaEUR = Math.round(deptDelta * eurIlsRatio);
                                    // Per-department % adjustment
                                    const deptPctAdj = monthDeptAdj2[r.department] || 0;
                                    const deptPctShare = deptBudgetTotals[r.department] ? r.amountEUR / deptBudgetTotals[r.department] : 0;
                                    const deptPctDeltaEUR = Math.round((deptAdjDeltaByDept[r.department] || 0) * deptPctShare);
                                    const deptPctDeltaILS = Math.round(deptPctDeltaEUR / eurIlsRatio);
                                    const adjEUR = Math.round(r.amountEUR * multiplier) + deptDeltaEUR + deptPctDeltaEUR;
                                    const adjILS = Math.round(r.amountILS * multiplier) + deptDelta + deptPctDeltaILS;
                                    const isChanged = hasAnyAdjustment && (adj !== 0 || deptDelta !== 0 || deptPctAdj !== 0);
                                    const _isFuture = forecastDrilldown.mKey >= `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}`;
                                    const salaryTotalEUR = d.budget.reduce((s: number, x: any) => s + Math.abs(x.amountEUR || 0), 0);
                                    const rowPct = salaryTotalEUR > 0 ? (Math.abs(r.amountEUR || 0) / salaryTotalEUR * 100).toFixed(1) + '%' : '—';
                                    if (hasAnyAdjustment) {
                                      return (<>
                                        <td className={`py-1.5 pr-2 text-right font-medium ${isChanged ? 'text-gray-400 line-through text-[10px]' : 'text-violet-700'}`}>{fmt(r.amountEUR)}</td>
                                        <td className={`py-1.5 pr-2 text-right ${isChanged ? 'text-gray-400 line-through text-[10px]' : 'text-blue-500'}`}>{fmtILS(r.amountILS)}</td>
                                        <td className="py-1.5 pr-2 text-right font-medium text-green-700">{fmt(adjEUR)}</td>
                                        <td className="py-1.5 pr-2 text-right text-green-600">{fmtILS(adjILS)}</td>
                                        {_isFuture && <td className="py-1.5 pr-2 text-right text-gray-400">{rowPct}</td>}
                                      </>);
                                    }
                                    return (<>
                                      <td className="py-1.5 pr-2 text-right font-medium text-violet-700">{fmt(r.amountEUR)}</td>
                                      <td className="py-1.5 pr-2 text-right text-blue-500">{fmtILS(r.amountILS)}</td>
                                      {_isFuture && <td className="py-1.5 pr-2 text-right text-gray-400">{rowPct}</td>}
                                    </>);
                                  })()}
                                </tr>
                                {isBudgetBillExpanded && (
                                  <tr><td colSpan={hasAnyAdjustment ? 8 : 6} className="p-0">
                                    <div className="bg-violet-50 p-2 mb-1 rounded">
                                      {!forecastDrilldown.data.__rowBills && <p className="text-xs text-gray-400 italic">Loading from NetSuite...</p>}
                                      {forecastDrilldown.data.__rowNsAcctId && nsAccountId && (() => {
                                        const [y, m] = forecastDrilldown.mKey.split('-');
                                        const endD = new Date(parseInt(y), parseInt(m), 0).getDate();
                                        return <p className="text-xs mb-1"><a href={`https://${nsAccountId}.app.netsuite.com/app/reporting/reportrunner.nl?acctid=${forecastDrilldown.data.__rowNsAcctId}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${m}/1/${y}&enddate=${m}/${endD}/${y}`} target="_blank" rel="noreferrer" className="text-violet-600 hover:text-violet-800 underline font-medium" onClick={(e) => e.stopPropagation()}>📋 View full register in NetSuite</a></p>;
                                      })()}
                                      {forecastDrilldown.data.__rowBills && forecastDrilldown.data.__rowBills.length === 0 && <p className="text-xs text-gray-400 italic">No posting transactions found for this account/month</p>}
                                      {forecastDrilldown.data.__rowBills && forecastDrilldown.data.__rowBills.length > 0 && (
                                        <table className="w-full text-xs">
                                          <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Date</th><th className="pb-1 pr-2">Bill #</th><th className="pb-1 pr-2">Vendor</th><th className="pb-1 pr-2">Memo</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 w-6"></th></tr></thead>
                                          <tbody>
                                            {forecastDrilldown.data.__rowBills.map((bill: any, bi: number) => (
                                              <tr key={bi} className="border-b border-violet-100">
                                                <td className="py-1 pr-2 text-gray-500">{bill.date ? (() => { const p = String(bill.date).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (p) { const d = new Date(+p[3], +p[2]-1, +p[1]); return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); } const d2 = new Date(bill.date); return isNaN(d2.getTime()) ? String(bill.date).substring(0,10) : d2.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); })() : '-'}</td>
                                                <td className="py-1 pr-2 text-violet-700 font-medium">{bill.billNumber || '-'}{bill.tranType && bill.tranType !== 'VendBill' && <span className="ml-1 text-[9px] text-gray-400 font-normal">{bill.tranType}</span>}</td>
                                                <td className="py-1 pr-2 text-gray-700 truncate max-w-[160px]">{bill.vendor || '-'}</td>
                                                <td className="py-1 pr-2 text-gray-500 truncate max-w-[140px]">{bill.memo || '-'}</td>
                                                <td className="py-1 pr-2 text-right font-medium text-violet-700">{fmt(bill.amount)}</td>
                                                <td className="py-1 text-center">{nsAccountId && bill.billId ? <a href={`https://${nsAccountId}.app.netsuite.com/app/accounting/transactions/${bill.nsUrlType || 'vendbill'}.nl?id=${bill.billId}`} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 text-[10px] font-bold" onClick={(e) => e.stopPropagation()}>NetSuite</a> : null}</td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      )}
                                    </div>
                                  </td></tr>
                                )}
                                </Fragment>
                                );
                              })}
                            </tbody>
                            <tfoot><tr className="border-t-2 font-bold">
                              <td className="py-1.5" colSpan={3}>Total</td>
                              {(() => {
                                const totalLeverDeltaILS = Object.values(deptLeverDelta).reduce((s, d) => s + d, 0);
                                const avgRatio = budgetTotalILS > 0 ? budgetTotal / budgetTotalILS : (1/3.75);
                                const totalLeverDeltaEUR = Math.round(totalLeverDeltaILS * avgRatio);
                                const grandAdjEUR = adjustedTotal + totalLeverDeltaEUR;
                                const grandAdjILS = adjustedTotalILS + totalLeverDeltaILS;
                                if (hasAnyAdjustment) {
                                  return (<>
                                    <td className="py-1.5 pr-2 text-right text-gray-400 line-through">{fmt(budgetTotal)}</td>
                                    <td className="py-1.5 pr-2 text-right text-gray-400 line-through">{fmtILS(budgetTotalILS)}</td>
                                    <td className="py-1.5 pr-2 text-right text-green-800">{fmt(grandAdjEUR)}</td>
                                    <td className="py-1.5 pr-2 text-right text-green-700">{fmtILS(grandAdjILS)}</td>
                                    {forecastDrilldown.mKey >= `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}` && <td className="py-1.5 pr-2 text-right text-gray-500 font-bold">100%</td>}
                                  </>);
                                }
                                return (<>
                                  <td className="py-1.5 pr-2 text-right text-violet-800">{fmt(budgetTotal)}</td>
                                  <td className="py-1.5 pr-2 text-right text-blue-700">{fmtILS(budgetTotalILS)}</td>
                                  {forecastDrilldown.mKey >= `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}` && <td className="py-1.5 pr-2 text-right text-gray-500 font-bold">100%</td>}
                                </>);
                              })()}
                            </tr></tfoot>
                          </table>
                        </div>
                      )}
                      {/* Headcount events (HiBob levers) */}
                      {hc && (hc.events?.length > 0 || hc.cumulative?.length > 0) && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">Salary Projection Levers (HiBob → Snowflake)</p>
                          {/* Cumulative impact summary */}
                          {hc.cumulative?.length > 0 && (() => {
                            const mKeyPrefix = forecastDrilldown.mKey; // e.g. "2026-06"
                            const detailOverrides = leverOverrides;
                            const hasOverrides = Object.values(detailOverrides).some((m: any) => Object.keys(m).length > 0);
                            // Compute amended totals per lever from detail overrides
                            const leverAmended: Record<string, number> = {};
                            for (const c of hc.cumulative) {
                              const key = `${c.type}/${c.subType}`;
                              const rowOverrides = detailOverrides[key] || {};
                              if (Object.keys(rowOverrides).length > 0) {
                                // Detail loaded — amended = sum of (override || original) per row
                                // But we need the detail data for this; use cached __leverDetails
                                const cachedDetail = (forecastDrilldown.data?.__leverDetails || {})[key] as any[] | undefined;
                                if (cachedDetail) {
                                  const mKey = forecastDrilldown.mKey;
                                  const filtered = cachedDetail.filter((d: any) => d.month >= mKey || d.status === 'Open' || !d.employeeId);
                                  leverAmended[key] = filtered.reduce((s: number, d: any, i: number) => s + (rowOverrides[i] !== undefined ? rowOverrides[i] : d.cost), 0);
                                } else {
                                  leverAmended[key] = c.totalCost; // No detail yet, use original
                                }
                              }
                            }
                            const origNet = hc.cumulative.reduce((s: number, c: any) => s + (c.type === 'increase' ? c.totalCost : -c.totalCost), 0);
                            const amendedNet = hc.cumulative.reduce((s: number, c: any) => {
                              const key = `${c.type}/${c.subType}`;
                              const cost = leverAmended[key] !== undefined ? leverAmended[key] : c.totalCost;
                              return s + (c.type === 'increase' ? cost : -cost);
                            }, 0);
                            const monthIdx = parseInt(forecastDrilldown.mKey.split('-')[1]) - 1;
                            const monthBudgetILS = budgetTotalILS || (budgetTotal * 3.75);
                            const pctChange = monthBudgetILS > 0 ? Math.round(((amendedNet - origNet) / monthBudgetILS) * 100) : 0;
                            return (
                            <div className="bg-amber-50 rounded-lg p-3 mb-3">
                              <div className="flex items-center justify-between mb-2">
                                <p className="text-xs text-gray-500 font-medium">Headcount Impact ({['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][monthIdx]} → Dec 2026) {hasOverrides ? <span className="text-amber-600 ml-1">— amended</span> : ''}</p>
                                <div className="flex items-center gap-2">
                                  {hasOverrides && (
                                    <button className="text-[10px] bg-gray-100 text-gray-500 px-2 py-0.5 rounded hover:bg-gray-200" onClick={(e) => {
                                      e.stopPropagation();
                                      setLeverOverrides({});
                                      setSalaryAdjPctByMonth(prev => {
                                        const next = { ...prev };
                                        for (let mi = monthIdx; mi <= 11; mi++) delete next[mi];
                                        return next;
                                      });
                                    }}>Reset</button>
                                  )}
                                  {hasOverrides && pctChange !== 0 && (
                                    <span className="text-[10px] text-amber-600 font-medium">Auto-applied {pctChange >= 0 ? '+' : ''}{pctChange}% ({['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][monthIdx]}→Dec)</span>
                                  )}
                                </div>
                              </div>
                              <table className="w-full text-xs">
                                <thead><tr className="text-left text-gray-400 uppercase border-b border-amber-200">
                                  <th className="pb-1 pr-2">Lever</th><th className="pb-1 pr-2 text-right">Count</th><th className="pb-1 pr-2 text-right">Original</th><th className="pb-1 pr-2 text-right">Amended</th>
                                </tr></thead>
                                <tbody>
                                  {hc.cumulative.map((c: any, idx: number) => {
                                    const leverKey = `${c.type}/${c.subType}`;
                                    const isExpanded = forecastDrilldown.data?.__expandedLever === leverKey;
                                    const amendedCost = leverAmended[leverKey] !== undefined ? leverAmended[leverKey] : c.totalCost;
                                    const isAmended = leverAmended[leverKey] !== undefined && leverAmended[leverKey] !== c.totalCost;
                                    return (
                                    <Fragment key={idx}>
                                    <tr className="border-b border-amber-100 cursor-pointer hover:bg-amber-100 transition-colors" onClick={() => {
                                      if (isExpanded) {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedLever: null } } : null);
                                      } else {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedLever: leverKey, __leverDetail: null } } : null);
                                        fetch(`/api/sf-headcount-lever-detail?eventType=${encodeURIComponent(c.type)}&eventSubType=${encodeURIComponent(c.subType)}&fromMonth=${forecastDrilldown.mKey}`).then(r => r.json()).then(j => {
                                          setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __leverDetail: j.data || [], __leverDetails: { ...(prev.data.__leverDetails || {}), [leverKey]: j.data || [] } } } : null);
                                        });
                                      }
                                    }}>
                                      <td className="py-1.5 pr-2">
                                        <span className={`inline-block w-1.5 h-1.5 rounded-full mr-1.5 ${c.type === 'increase' ? 'bg-green-500' : 'bg-red-500'}`}></span>
                                        <span className={`${c.type === 'increase' ? 'text-green-700' : 'text-red-600'} underline decoration-dotted cursor-pointer`}>{c.subType}</span>
                                        <span className="text-gray-400 ml-1 text-[10px]">{isExpanded ? '▼' : '▶'}</span>
                                      </td>
                                      <td className="py-1.5 pr-2 text-right text-gray-600">{c.count}</td>
                                      <td className={`py-1.5 pr-2 text-right ${isAmended ? 'text-gray-400 line-through' : 'font-medium'} ${c.type === 'increase' ? 'text-red-600' : 'text-green-700'}`}>
                                        {c.type === 'increase' ? '+' : '-'}{fmtILS(c.totalCost)}
                                      </td>
                                      <td className={`py-1.5 pr-2 text-right font-medium ${isAmended ? 'text-amber-700' : 'text-gray-500'}`}>
                                        {c.type === 'increase' ? '+' : '-'}{fmtILS(amendedCost)}
                                        {!isAmended && <span className="text-[10px] italic text-gray-300 ml-1">▶</span>}
                                      </td>
                                    </tr>
                                    {isExpanded && (
                                      <tr><td colSpan={4} className="p-0">
                                        <div className="bg-amber-100/50 p-2 mb-1 rounded">
                                          {!forecastDrilldown.data.__leverDetail && <p className="text-xs text-gray-400 italic">Loading from HiBob...</p>}
                                          {forecastDrilldown.data.__leverDetail && forecastDrilldown.data.__leverDetail.length === 0 && <p className="text-xs text-gray-400 italic">No detail data available</p>}
                                          {forecastDrilldown.data.__leverDetail && forecastDrilldown.data.__leverDetail.length > 0 && (() => {
                                            const mKey = forecastDrilldown.mKey;
                                            const filtered = forecastDrilldown.data.__leverDetail.filter((d: any) => {
                                              if (d.month >= mKey) return true;
                                              if (d.status === 'Open' || !d.employeeId) return true;
                                              return false;
                                            });
                                            if (filtered.length === 0) return <p className="text-xs text-gray-400 italic">No future events from {mKey}</p>;
                                            const rowOverrides = detailOverrides[leverKey] || {};
                                            return (
                                            <table className="w-full text-xs">
                                              <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Month</th><th className="pb-1 pr-2">Dept</th><th className="pb-1 pr-2">Name</th><th className="pb-1 pr-2">Position</th><th className="pb-1 pr-2 text-right">Original</th><th className="pb-1 pr-2 text-right">Amended</th></tr></thead>
                                              <tbody>
                                                {filtered.map((d: any, di: number) => {
                                                  const rowAmended = rowOverrides[di] !== undefined ? rowOverrides[di] : d.cost;
                                                  const rowIsAmended = rowOverrides[di] !== undefined && rowOverrides[di] !== d.cost;
                                                  return (
                                                  <tr key={di} className={`border-b border-amber-100 ${d.month < mKey ? 'opacity-60' : ''}`}>
                                                    <td className="py-1 pr-1 text-gray-500">{d.month < mKey ? <span title="Open position from earlier month, carried forward">{d.month} →</span> : d.month}</td>
                                                    <td className="py-1 pr-1 text-gray-600">{d.department}</td>
                                                    <td className="py-1 pr-1 text-gray-700 truncate max-w-[140px]">{d.employeeName || (d.employeeId ? `#${d.employeeId}` : d.openingId || '-')}</td>
                                                    <td className="py-1 pr-1 text-gray-500 truncate max-w-[120px]">{d.position || '-'}</td>
                                                    <td className={`py-1 pr-1 text-right font-medium ${rowIsAmended ? 'text-gray-400 line-through text-[10px]' : ''} ${c.type === 'increase' ? 'text-red-600' : 'text-green-700'}`}>{fmtILS(d.cost)}</td>
                                                    <td className="py-1 pr-1 text-right">
                                                      <input type="text" inputMode="numeric"
                                                        value={rowAmended.toLocaleString()}
                                                        onClick={(e) => e.stopPropagation()}
                                                        onChange={(e) => {
                                                          const v = e.target.value.replace(/[^0-9]/g, '');
                                                          const n = v === '' ? 0 : parseInt(v);
                                                          // Update persistent lever overrides
                                                          setLeverOverrides(prev => {
                                                            const prevLever = prev[leverKey] || {};
                                                            const next = { ...prev, [leverKey]: { ...prevLever, [di]: n } };
                                                            // Auto-compute % impact on salary and apply forward
                                                            const allDetails = forecastDrilldown.data?.__leverDetails || {};
                                                            let totalOriginal = 0, totalAmended = 0;
                                                            for (const cc of hc.cumulative) {
                                                              const lk = `${cc.type}/${cc.subType}`;
                                                              const detail = allDetails[lk] as any[] | undefined;
                                                              const lOverrides = next[lk] || {};
                                                              if (detail && Object.keys(lOverrides).length > 0) {
                                                                const filt = detail.filter((dd: any) => dd.month >= mKeyPrefix || dd.status === 'Open' || !dd.employeeId);
                                                                const orig = filt.reduce((s: number, dd: any) => s + dd.cost, 0);
                                                                const amend = filt.reduce((s: number, dd: any, ii: number) => s + (lOverrides[ii] !== undefined ? lOverrides[ii] : dd.cost), 0);
                                                                totalOriginal += (cc.type === 'increase' ? orig : -orig);
                                                                totalAmended += (cc.type === 'increase' ? amend : -amend);
                                                              } else {
                                                                totalOriginal += (cc.type === 'increase' ? cc.totalCost : -cc.totalCost);
                                                                totalAmended += (cc.type === 'increase' ? cc.totalCost : -cc.totalCost);
                                                              }
                                                            }
                                                            const delta = totalAmended - totalOriginal;
                                                            const budgetILS = budgetTotalILS || (budgetTotal * 3.75);
                                                            const pct = budgetILS > 0 ? Math.round((delta / budgetILS) * 100) : 0;
                                                            // Apply % to salary from this month through December
                                                            setSalaryAdjPctByMonth(prevAdj => {
                                                              const nextAdj = { ...prevAdj };
                                                              for (let mi = monthIdx; mi <= 11; mi++) nextAdj[mi] = pct;
                                                              return nextAdj;
                                                            });
                                                            return next;
                                                          });
                                                        }}
                                                        className={`w-20 text-right text-xs border rounded px-1 py-0.5 ${rowIsAmended ? 'text-amber-700 border-amber-300 bg-amber-50 font-medium' : 'text-gray-500 border-gray-200 bg-white'}`}
                                                      />
                                                    </td>
                                                  </tr>
                                                  );
                                                })}
                                                <tr className="border-t border-amber-300 font-medium">
                                                  <td colSpan={4} className="py-1 pr-1 text-gray-500 text-right">Subtotal</td>
                                                  <td className={`py-1 pr-1 text-right ${c.type === 'increase' ? 'text-red-600' : 'text-green-700'}`}>{fmtILS(filtered.reduce((s: number, d: any) => s + d.cost, 0))}</td>
                                                  <td className={`py-1 pr-1 text-right font-bold ${c.type === 'increase' ? 'text-red-600' : 'text-green-700'}`}>{fmtILS(filtered.reduce((s: number, d: any, i: number) => s + (rowOverrides[i] !== undefined ? rowOverrides[i] : d.cost), 0))}</td>
                                                </tr>
                                              </tbody>
                                            </table>
                                            );
                                          })()}
                                        </div>
                                      </td></tr>
                                    )}
                                    </Fragment>
                                    );
                                  })}
                                </tbody>
                                <tfoot><tr className="border-t-2 border-amber-300 font-bold">
                                  <td className="py-1.5" colSpan={2}>Net Impact</td>
                                  <td className={`py-1.5 pr-2 text-right ${hasOverrides ? 'text-gray-400 line-through' : ''} ${origNet >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                                    {origNet >= 0 ? '+' : '-'}{fmtILS(Math.abs(origNet))}
                                  </td>
                                  <td className={`py-1.5 pr-2 text-right font-bold ${amendedNet >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                                    {amendedNet >= 0 ? '+' : '-'}{fmtILS(Math.abs(amendedNet))}
                                    {hasOverrides && <span className="text-amber-600 text-[10px] ml-1">({pctChange >= 0 ? '+' : ''}{pctChange}%)</span>}
                                  </td>
                                </tr></tfoot>
                              </table>
                            </div>
                          );
                          })()}
                          {/* Monthly timeline — reflects per-employee overrides */}
                          {hc.monthly?.length > 0 && (
                            <div className="mb-3">
                              <p className="text-xs text-gray-500 mb-1 font-medium cursor-pointer hover:text-gray-700" onClick={() => setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __showMonthlyTimeline: !prev.data.__showMonthlyTimeline } } : null)}>
                                Monthly Timeline {forecastDrilldown.data?.__showMonthlyTimeline === false ? '▶' : '▼'}
                              </p>
                              {forecastDrilldown.data?.__showMonthlyTimeline !== false && (() => {
                                const mKey = forecastDrilldown.mKey;
                                const detOverrides = leverOverrides;
                                const cachedDetails = (forecastDrilldown.data?.__leverDetails || {}) as Record<string, any[]>;
                                // Build amendment lookup: leverKey → month → cost delta
                                const amendByLeverMonth: Record<string, Record<string, number>> = {};
                                for (const [leverKey, rowOvr] of Object.entries(detOverrides)) {
                                  if (!Object.keys(rowOvr).length) continue;
                                  const detail = cachedDetails[leverKey];
                                  if (!detail) continue;
                                  const filtered = detail.filter((d: any) => d.month >= mKey || d.status === 'Open' || !d.employeeId);
                                  for (const [idxStr, newCost] of Object.entries(rowOvr)) {
                                    const di = parseInt(idxStr);
                                    const d = filtered[di];
                                    if (!d) continue;
                                    const effectiveMonth = d.month < mKey ? mKey : d.month;
                                    if (!amendByLeverMonth[leverKey]) amendByLeverMonth[leverKey] = {};
                                    amendByLeverMonth[leverKey][effectiveMonth] = (amendByLeverMonth[leverKey][effectiveMonth] || 0) + ((newCost as number) - d.cost);
                                  }
                                }
                                const byMonth: Record<string, { increases: number; decreases: number; net: number; count: number }> = {};
                                for (const m of hc.monthly) {
                                  if (m.month < mKey) continue;
                                  if (!byMonth[m.month]) byMonth[m.month] = { increases: 0, decreases: 0, net: 0, count: 0 };
                                  byMonth[m.month].count += m.count;
                                  const lk = `${m.type}/${m.subType}`;
                                  const delta = amendByLeverMonth[lk]?.[m.month] || 0;
                                  const adjustedCost = m.totalCost + delta;
                                  if (m.type === 'increase') { byMonth[m.month].increases += adjustedCost; byMonth[m.month].net += adjustedCost; }
                                  else { byMonth[m.month].decreases += adjustedCost; byMonth[m.month].net -= adjustedCost; }
                                }
                                const months = Object.keys(byMonth).sort();
                                let running = 0;
                                return (
                                  <table className="w-full text-xs mb-2">
                                    <thead><tr className="text-left text-gray-400 uppercase border-b">
                                      <th className="pb-1 pr-2">Month</th><th className="pb-1 pr-2 text-right">Events</th><th className="pb-1 pr-2 text-right text-green-600">Hires</th><th className="pb-1 pr-2 text-right text-red-500">Terms</th><th className="pb-1 pr-2 text-right">Net</th><th className="pb-1 pr-2 text-right">Running</th>
                                    </tr></thead>
                                    <tbody>
                                      {months.map(m => {
                                        const d = byMonth[m];
                                        running += d.net;
                                        return (
                                          <tr key={m} className="border-b border-gray-100">
                                            <td className="py-1 pr-1 text-gray-600">{m}</td>
                                            <td className="py-1 pr-1 text-right text-gray-500">{d.count}</td>
                                            <td className="py-1 pr-1 text-right text-red-600">{d.increases > 0 ? `+${fmtILS(d.increases)}` : '-'}</td>
                                            <td className="py-1 pr-1 text-right text-green-700">{d.decreases > 0 ? `-${fmtILS(d.decreases)}` : '-'}</td>
                                            <td className={`py-1 pr-1 text-right font-medium ${d.net >= 0 ? 'text-red-600' : 'text-green-700'}`}>{d.net >= 0 ? '+' : '-'}{fmtILS(Math.abs(d.net))}</td>
                                            <td className={`py-1 pr-1 text-right font-medium ${running >= 0 ? 'text-red-600' : 'text-green-700'}`}>{running >= 0 ? '+' : '-'}{fmtILS(Math.abs(running))}</td>
                                          </tr>
                                        );
                                      })}
                                    </tbody>
                                  </table>
                                );
                              })()}
                            </div>
                          )}
                          {/* This month's events — reflects per-employee overrides */}
                          {hc.events?.length > 0 && (() => {
                            const detOverrides = leverOverrides;
                            const cachedDetails = (forecastDrilldown.data?.__leverDetails || {}) as Record<string, any[]>;
                            const mKey = forecastDrilldown.mKey;
                            // Build lookup: for each event, find its amended cost from detail overrides
                            const getAmendedCost = (evt: any) => {
                              const leverKey = `${evt.type}/${evt.subType}`;
                              const rowOvr = detOverrides[leverKey];
                              const detail = cachedDetails[leverKey];
                              if (!rowOvr || !detail) return evt.cost;
                              const filtered = detail.filter((d: any) => d.month >= mKey || d.status === 'Open' || !d.employeeId);
                              // Match event to detail row by employeeId/openingId + month + cost
                              for (let i = 0; i < filtered.length; i++) {
                                const d = filtered[i];
                                if (d.month === mKey && d.cost === evt.cost && (d.employeeId === evt.employeeId || d.openingId === evt.openingId)) {
                                  return rowOvr[i] !== undefined ? rowOvr[i] : evt.cost;
                                }
                              }
                              return evt.cost;
                            };
                            return (
                            <div className="mb-3">
                              <p className="text-xs text-gray-500 mb-1 font-medium">Events This Month</p>
                              <table className="w-full text-xs">
                                <thead><tr className="text-left text-gray-400 uppercase border-b">
                                  <th className="pb-1 pr-2">Type</th><th className="pb-1 pr-2">Department</th><th className="pb-1 pr-2">Position</th><th className="pb-1 pr-2 text-right">Employer Cost</th>
                                </tr></thead>
                                <tbody>
                                  {hc.events.map((e: any, idx: number) => {
                                    const amended = getAmendedCost(e);
                                    const isAmended = amended !== e.cost;
                                    return (
                                    <tr key={idx} className="border-b border-gray-50">
                                      <td className="py-1.5 pr-2">
                                        <span className={`inline-block w-1.5 h-1.5 rounded-full mr-1.5 ${e.type === 'increase' ? 'bg-green-500' : 'bg-red-500'}`}></span>
                                        <span className={e.type === 'increase' ? 'text-green-700' : 'text-red-600'}>{e.subType}</span>
                                      </td>
                                      <td className="py-1.5 pr-2 text-gray-500">{e.department}</td>
                                      <td className="py-1.5 pr-2 text-gray-700 truncate max-w-[200px]">{e.position || (e.employeeId ? `Employee #${e.employeeId}` : '-')}</td>
                                      <td className={`py-1.5 pr-2 text-right font-medium ${e.type === 'increase' ? 'text-red-600' : 'text-green-700'}`}>
                                        {isAmended && <span className="text-gray-400 line-through text-[10px] mr-1">{fmtILS(e.cost)}</span>}
                                        {e.type === 'increase' ? '+' : '-'}{fmtILS(amended)}
                                      </td>
                                    </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
                            </div>
                            );
                          })()}
                          {(!hc.events || hc.events.length === 0) && <p className="text-xs text-gray-400 mb-3 italic">No headcount changes this month</p>}
                          {hc.baseline.headcount > 0 && (
                            <p className="text-xs text-gray-400">Baseline: {hc.baseline.headcount} active payroll employees, {fmtILS(hc.baseline.monthlyBase)}/month base salary</p>
                          )}
                        </div>
                      )}
                      {hasActuals && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">Actuals ({forecastDrilldown.data?.__nsMode ? 'NetSuite 76xx accounts' : 'Snowflake FCT_EXPENSE'}) — click row for bills</p>
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Department</th><th className="pb-1 pr-2">Account #</th><th className="pb-1 pr-2">Name</th><th className="pb-1 pr-2 text-right">EUR</th><th className="pb-1 pr-2 text-right">ILS</th>
                            </tr></thead>
                            <tbody>
                              {d.actuals.map((r: any, idx: number) => {
                                const billKey = `actual__${r.account}__${r.department}`;
                                const isBillExpanded = forecastDrilldown.data?.__expandedBillRow === billKey;
                                return (
                                <Fragment key={idx}>
                                <tr className={`border-b border-gray-50 cursor-pointer hover:bg-amber-50 transition-colors`}
                                    onClick={() => {
                                      if (isBillExpanded) {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedBillRow: null, __rowBills: null } } : null);
                                      } else {
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedBillRow: billKey, __rowBills: null } } : null);
                                        if (r.account) {
                                          fetch(`/api/ns-vendor-bills?accountId=${r.account}&month=${forecastDrilldown.mKey}`).then(res => res.json()).then(j => {
                                            setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __rowBills: j.data || [], __rowNsAcctId: j.nsAcctId } } : null);
                                          });
                                        }
                                      }
                                    }}>
                                  <td className="py-1.5 pr-2 text-gray-500"><span className="text-gray-400 mr-1">{isBillExpanded ? '▼' : '▶'}</span>{r.department}</td>
                                  <td className="py-1.5 pr-2 font-mono text-amber-600">{r.account}</td>
                                  <td className="py-1.5 pr-2 text-gray-700">{r.name}</td>
                                  <td className="py-1.5 pr-2 text-right font-medium text-amber-700">{fmt(r.amountEUR)}</td>
                                  <td className="py-1.5 pr-2 text-right text-blue-500">{fmtILS(r.amountILS)}</td>
                                </tr>
                                {isBillExpanded && (
                                  <tr><td colSpan={5} className="p-0">
                                    <div className="bg-amber-50 p-2 mb-1 rounded">
                                      {!forecastDrilldown.data.__rowBills && <p className="text-xs text-gray-400 italic">Loading from NetSuite...</p>}
                                      {forecastDrilldown.data.__rowNsAcctId && nsAccountId && (() => {
                                        const [y, m] = forecastDrilldown.mKey.split('-');
                                        const endD = new Date(parseInt(y), parseInt(m), 0).getDate();
                                        return <p className="text-xs mb-1"><a href={`https://${nsAccountId}.app.netsuite.com/app/reporting/reportrunner.nl?acctid=${forecastDrilldown.data.__rowNsAcctId}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${m}/1/${y}&enddate=${m}/${endD}/${y}`} target="_blank" rel="noreferrer" className="text-violet-600 hover:text-violet-800 underline font-medium" onClick={(e) => e.stopPropagation()}>📋 View full register in NetSuite</a></p>;
                                      })()}
                                      {forecastDrilldown.data.__rowBills && forecastDrilldown.data.__rowBills.length === 0 && <p className="text-xs text-gray-400 italic">No posting transactions found for this account/month</p>}
                                      {forecastDrilldown.data.__rowBills && forecastDrilldown.data.__rowBills.length > 0 && (
                                        <table className="w-full text-xs">
                                          <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Date</th><th className="pb-1 pr-2">Bill #</th><th className="pb-1 pr-2">Vendor</th><th className="pb-1 pr-2">Memo</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 w-6"></th></tr></thead>
                                          <tbody>
                                            {forecastDrilldown.data.__rowBills.map((bill: any, bi: number) => (
                                              <tr key={bi} className="border-b border-amber-100">
                                                <td className="py-1 pr-2 text-gray-500">{bill.date ? (() => { const p = String(bill.date).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (p) { const d = new Date(+p[3], +p[2]-1, +p[1]); return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); } const d2 = new Date(bill.date); return isNaN(d2.getTime()) ? String(bill.date).substring(0,10) : d2.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); })() : '-'}</td>
                                                <td className="py-1 pr-2 text-amber-700 font-medium">{bill.billNumber || '-'}{bill.tranType && bill.tranType !== 'VendBill' && <span className="ml-1 text-[9px] text-gray-400 font-normal">{bill.tranType}</span>}</td>
                                                <td className="py-1 pr-2 text-gray-700 truncate max-w-[160px]">{bill.vendor || '-'}</td>
                                                <td className="py-1 pr-2 text-gray-500 truncate max-w-[140px]">{bill.memo || '-'}</td>
                                                <td className="py-1 pr-2 text-right font-medium text-amber-700">{fmt(bill.amount)}</td>
                                                <td className="py-1 text-center">{nsAccountId && bill.billId ? <a href={`https://${nsAccountId}.app.netsuite.com/app/accounting/transactions/${bill.nsUrlType || 'vendbill'}.nl?id=${bill.billId}`} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 text-[10px] font-bold" onClick={(e) => e.stopPropagation()}>NetSuite</a> : null}</td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      )}
                                    </div>
                                  </td></tr>
                                )}
                                </Fragment>
                                );
                              })}
                            </tbody>
                            <tfoot><tr className="border-t-2 font-bold">
                              <td className="py-1.5" colSpan={3}>Total</td>
                              <td className="py-1.5 pr-2 text-right text-amber-800">{fmt(actualTotal)}</td>
                              <td className="py-1.5 pr-2 text-right text-blue-700">{fmtILS(d.actuals.reduce((s: number, r: any) => s + (r.amountILS || 0), 0))}</td>
                            </tr></tfoot>
                          </table>
                        </div>
                      )}
                      {!hasActuals && !hasBudget && <p className="text-gray-400 text-center py-8">No salary data available for this month</p>}
                    </div>
                  );
                })()}
                {/* ── Carry breakdown drilldown ── */}
                {forecastDrilldown.type === 'inflows' && forecastDrilldown.data && forecastDrilldown.data !== 'loading' && forecastDrilldown.data.__carryClients && (() => {
                  const clients = forecastDrilldown.data.__carryClients as any[];
                  const total = forecastDrilldown.data.__carryTotal as number;
                  const sourceMonth = forecastDrilldown.data.__sourceMonth as string;
                  return (
                    <div className="space-y-4">
                      <div className="bg-amber-50 rounded-lg p-3">
                        <p className="text-xs text-gray-400 mb-2 uppercase">Unpaid Revenue Carry from {sourceMonth}</p>
                        <table className="w-full text-xs">
                          <tbody>
                            <tr className="border-b border-amber-200"><td className="py-1.5 text-gray-600">Total Carry Amount</td><td className="py-1.5 text-right font-bold text-amber-700">{fmt(total)}</td></tr>
                            <tr><td className="py-1.5 text-gray-600">Clients with Unpaid</td><td className="py-1.5 text-right font-bold text-amber-700">{clients.length}</td></tr>
                          </tbody>
                        </table>
                      </div>
                      {clients.length > 0 && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">Client Breakdown — Revenue expected but not yet collected ({sourceMonth})</p>
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Client / Opportunity</th><th className="pb-1 pr-2 text-right">Revenue</th><th className="pb-1 pr-2 text-right">Paid</th><th className="pb-1 pr-2 text-right text-amber-600">Unpaid</th><th className="pb-1 text-center">NS</th>
                            </tr></thead>
                            <tbody>
                              {clients.map((c: any, idx: number) => {
                                const cName = c.customer || c.name || '';
                                const searchTerm = cName.split(/\s*[-–(]/)[0].trim().split(/\s+/).slice(0, 2).join(' ');
                                const nsUrl = nsAccountId && searchTerm ? `https://${nsAccountId}.app.netsuite.com/app/common/search/ubersearchresults.nl?quicksearch=T&searchtype=Uber&frame=be&Uber_NAMEtype=KEYWORDSTARTSWITH&Uber_NAME=${encodeURIComponent(searchTerm)}` : '';
                                return (
                                <tr key={idx} className="border-b border-gray-50">
                                  <td className="py-1.5 pr-2 text-gray-700 truncate max-w-[250px]">{cName || '-'}{c.opportunity && c.opportunity !== '-' ? <span className="text-gray-400 ml-1 text-[11px]">({c.opportunity})</span> : ''}</td>
                                  <td className="py-1.5 pr-2 text-right text-blue-600">{fmt(c.revenue)}</td>
                                  <td className="py-1.5 pr-2 text-right text-green-600">{fmt(c.paid)}</td>
                                  <td className="py-1.5 pr-2 text-right font-medium text-amber-700">{fmt(c.unpaid)}</td>
                                  <td className="py-1.5 text-center">{nsUrl ? <a href={nsUrl} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 font-bold text-[10px]" onClick={(e) => e.stopPropagation()}>NS</a> : null}</td>
                                </tr>
                                );
                              })}
                            </tbody>
                            <tfoot><tr className="border-t-2 font-bold">
                              <td className="py-1.5">Total</td>
                              <td className="py-1.5 pr-2 text-right text-blue-700">{fmt(clients.reduce((s: number, c: any) => s + c.revenue, 0))}</td>
                              <td className="py-1.5 pr-2 text-right text-green-700">{fmt(clients.reduce((s: number, c: any) => s + c.paid, 0))}</td>
                              <td className="py-1.5 pr-2 text-right text-amber-800">{fmt(clients.reduce((s: number, c: any) => s + c.unpaid, 0))}</td>
                              <td></td>
                            </tr></tfoot>
                          </table>
                        </div>
                      )}
                    </div>
                  );
                })()}
                {/* ── Inflows drilldown ── */}
                {forecastDrilldown.type === 'inflows' && forecastDrilldown.data && forecastDrilldown.data !== 'loading' && !Array.isArray(forecastDrilldown.data) && (forecastDrilldown.data.forecast !== undefined || forecastDrilldown.data.revenue !== undefined) && (() => {
                  const d = forecastDrilldown.data;
                  const revGap = (d.revenue || d.forecast || 0) - (d.target || 0);
                  return (
                    <div className="space-y-4">
                      <p className="text-xs text-gray-400 mb-1">Revenue from FCT_MONTHLY_REVENUE__SUBSET_PAID • {d.customers > 0 ? `${d.customers} customers` : ''}</p>
                      <table className="w-full text-xs">
                        <thead><tr className="text-left text-gray-400 uppercase border-b">
                          <th className="pb-1 pr-2">Metric</th><th className="pb-1 pr-2 text-right">EUR</th>
                        </tr></thead>
                        <tbody>
                          {(d.isPast || d.isCurrent) && d.actual > 0 && (
                            <tr className="border-b border-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">NetSuite Collections (actual cash received)</td>
                              <td className="py-2 pr-2 text-right font-bold text-green-700">{fmt(d.actual)}</td>
                            </tr>
                          )}
                          {d.revenue > 0 && (
                            <tr className="border-b border-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">Expected Revenue (REVENUE_AMOUNT_EUR)</td>
                              <td className="py-2 pr-2 text-right font-bold text-blue-700">{fmt(d.revenue)}</td>
                            </tr>
                          )}
                          {d.paid > 0 && (
                            <tr className="border-b border-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">  ↳ Paid</td>
                              <td className="py-2 pr-2 text-right font-medium text-green-600">{fmt(d.paid)}</td>
                            </tr>
                          )}
                          {d.unpaid > 0 && (
                            <tr className="border-b border-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">  ↳ Unpaid (→ rolls to next month)</td>
                              <td className="py-2 pr-2 text-right font-medium text-amber-600">{fmt(d.unpaid)}</td>
                            </tr>
                          )}
                          {d.unpaidCarry > 0 && (
                            <tr className="border-b border-gray-50 bg-blue-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">  ↳ Carried from prev month (unpaid)</td>
                              <td className="py-2 pr-2 text-right font-medium text-blue-600">+{fmt(d.unpaidCarry)}</td>
                            </tr>
                          )}
                          {d.pipeline > 0 && (
                            <tr className="border-b border-gray-50 bg-green-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">  ↳ Pipeline (≥{pipelineMinProb}% prob, closed by this month)</td>
                              <td className="py-2 pr-2 text-right font-medium text-green-600">+{fmt(d.pipeline)}</td>
                            </tr>
                          )}
                          {d.target > 0 && (
                            <tr className="border-b border-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">Budget Target</td>
                              <td className="py-2 pr-2 text-right font-bold text-violet-700">{fmt(d.target)}</td>
                            </tr>
                          )}
                          {d.target > 0 && (
                            <tr className="border-b border-gray-100 bg-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">Gap (Revenue − Target)</td>
                              <td className={`py-2 pr-2 text-right font-bold ${revGap >= 0 ? 'text-green-700' : 'text-red-600'}`}>{revGap >= 0 ? '+' : ''}{fmt(revGap)}</td>
                            </tr>
                          )}
                          {!d.isPast && !d.isCurrent && (
                            <tr className="border-b border-gray-100 bg-green-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">Used in cashflow (× {d.collPct}%{d.unpaidCarry > 0 ? ' + unpaid carry' : ''}{d.pipeline > 0 ? ' + pipeline' : ''})</td>
                              <td className="py-2 pr-2 text-right font-bold text-green-700">{fmt(d.collections)}</td>
                            </tr>
                          )}
                          {(d.isPast || d.isCurrent) && d.actual > 0 && d.target > 0 && (
                            <tr className="border-b border-gray-100 bg-gray-50">
                              <td className="py-2 pr-2 text-gray-700 font-medium">Actual vs Target</td>
                              <td className={`py-2 pr-2 text-right font-bold ${d.actual - d.target >= 0 ? 'text-green-700' : 'text-red-600'}`}>{d.actual - d.target >= 0 ? '+' : ''}{fmt(d.actual - d.target)} ({d.target > 0 ? ((d.actual / d.target * 100) - 100).toFixed(1) : 0}%)</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                      {/* Load breakdown button for future months */}
                      {!d.isPast && !d.breakdown && (
                        <button className="text-xs text-blue-600 hover:text-blue-800 underline" onClick={() => {
                          fetch(`/api/sf-revenue-breakdown?month=${forecastDrilldown.mKey}`).then(r => r.json()).then(r => {
                            setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, breakdown: r.data || [] } } : null);
                          });
                        }}>Load customer breakdown →</button>
                      )}
                      {d.breakdown && d.breakdown.length > 0 && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">Top customers by expected revenue</p>
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Opportunity</th><th className="pb-1 pr-2 text-right">Revenue</th><th className="pb-1 pr-2 text-right">Paid</th><th className="pb-1 pr-2 text-right">Unpaid</th>
                            </tr></thead>
                            <tbody>
                              {d.breakdown.map((r: any, idx: number) => (
                                <tr key={idx} className="border-b border-gray-50">
                                  <td className="py-1 pr-2 text-gray-700 truncate max-w-[250px]">{r.opportunity}</td>
                                  <td className="py-1 pr-2 text-right font-medium text-blue-700">{fmt(r.revenue)}</td>
                                  <td className="py-1 pr-2 text-right text-green-600">{r.paid > 0 ? fmt(r.paid) : '-'}</td>
                                  <td className="py-1 pr-2 text-right text-amber-600">{r.unpaid > 0 ? fmt(r.unpaid) : '-'}</td>
                                </tr>
                              ))}
                            </tbody>
                            <tfoot><tr className="border-t-2 font-bold">
                              <td className="py-1.5">Total (top {d.breakdown.length})</td>
                              <td className="py-1.5 pr-2 text-right text-blue-800">{fmt(d.breakdown.reduce((s: number, r: any) => s + r.revenue, 0))}</td>
                              <td className="py-1.5 pr-2 text-right text-green-700">{fmt(d.breakdown.reduce((s: number, r: any) => s + r.paid, 0))}</td>
                              <td className="py-1.5 pr-2 text-right text-amber-700">{fmt(d.breakdown.reduce((s: number, r: any) => s + r.unpaid, 0))}</td>
                            </tr></tfoot>
                          </table>
                        </div>
                      )}
                    </div>
                  );
                })()}
                {Array.isArray(forecastDrilldown.data) && forecastDrilldown.data.length > 0 && (() => {
                  const _now = new Date();
                  const _curMKey = `${_now.getFullYear()}-${String(_now.getMonth()+1).padStart(2,'0')}`;
                  const _isProjected = forecastDrilldown.mKey >= _curMKey;
                  // Category-level adjustment for this detail view
                  const _catName = forecastDrilldown.categoryName || '';
                  // Compute the "all departments" adjustment by checking if all detail rows share the same %
                  const _effCatAdj: { pct: number; inherited: boolean; fromMonth?: string; mixed: boolean } = { pct: 0, inherited: false, mixed: false };
                  if (_isProjected && _catName && Array.isArray(forecastDrilldown.data)) {
                    const detPcts: number[] = [];
                    for (const row of forecastDrilldown.data as any[]) {
                      const dk = `${_catName}||${row.department || ''}||${row.account || ''}`;
                      let dp = 0;
                      const adms = Object.keys(vendorDetailAdj).filter(k => k <= forecastDrilldown.mKey).sort();
                      for (const am of adms) { const v = vendorDetailAdj[am]?.[dk]; if (v && v.pct !== 0) dp = v.pct; else if (v && v.pct === 0) dp = 0; }
                      detPcts.push(dp);
                    }
                    if (detPcts.length > 0) {
                      const allSame = detPcts.every(p => p === detPcts[0]);
                      if (allSame) { _effCatAdj.pct = detPcts[0]; }
                      else { _effCatAdj.pct = detPcts[0]; _effCatAdj.mixed = true; }
                    }
                  }
                  return (
                  <div>
                  {forecastDrilldown.categoryName && (
                    <div className="flex items-center justify-between mb-2">
                      <p className="text-xs text-gray-400">{forecastDrilldown.categoryName}</p>
                      {_isProjected && _catName && (() => {
                        // Helper: increment/decrement ALL department detail rows by delta
                        const adjustAllDepts = (delta: number) => {
                          const mKey = forecastDrilldown.mKey;
                          const rows = forecastDrilldown.data as any[];
                          if (!Array.isArray(rows)) return;
                          setVendorDetailAdj(prev => {
                            const updated = { ...prev, [mKey]: { ...(prev[mKey] || {}) } };
                            for (const row of rows) {
                              const dk = `${_catName}||${row.department || ''}||${row.account || ''}`;
                              // Get current effective pct for this row
                              let curPct = 0;
                              const adms = Object.keys(prev).filter(k => k <= mKey).sort();
                              for (const am of adms) { const v = prev[am]?.[dk]; if (v && v.pct !== 0) curPct = v.pct; else if (v && v.pct === 0) curPct = 0; }
                              updated[mKey][dk] = { pct: curPct + delta, base: row.amountEUR || 0 };
                            }
                            return updated;
                          });
                        };
                        // Helper: set ALL department detail rows to an absolute %
                        const setAllDepts = (newPct: number) => {
                          const mKey = forecastDrilldown.mKey;
                          const rows = forecastDrilldown.data as any[];
                          if (!Array.isArray(rows)) return;
                          setVendorDetailAdj(prev => {
                            const updated = { ...prev, [mKey]: { ...(prev[mKey] || {}) } };
                            for (const row of rows) {
                              const dk = `${_catName}||${row.department || ''}||${row.account || ''}`;
                              updated[mKey][dk] = { pct: newPct, base: row.amountEUR || 0 };
                            }
                            return updated;
                          });
                        };
                        return (
                        <div className="flex items-center gap-1" onClick={e => e.stopPropagation()}>
                          <span className="text-[10px] text-teal-600 font-medium">Adjust all:</span>
                          <button onClick={() => adjustAllDepts(-1)}
                                  className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">−</button>
                          <input type="text" inputMode="numeric" value={_effCatAdj.mixed ? '~' : _effCatAdj.pct}
                                 onChange={e => { const v = e.target.value; if (v === '' || v === '-' || v === '~') return; const n = parseInt(v); if (!isNaN(n)) setAllDepts(n); }}
                                 className={`w-12 text-center text-[11px] font-semibold border rounded px-0.5 py-0.5 ${_effCatAdj.pct !== 0 ? 'text-teal-700 border-teal-300 bg-teal-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                          <span className="text-[10px] text-gray-400">%</span>
                          <button onClick={() => adjustAllDepts(1)}
                                  className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">+</button>
                          {_effCatAdj.pct !== 0 && !_effCatAdj.mixed && <span className={`text-[10px] font-bold ${_effCatAdj.pct > 0 ? 'text-red-500' : 'text-green-600'}`}>{_effCatAdj.pct > 0 ? '+' : ''}{_effCatAdj.pct}%</span>}
                          {_effCatAdj.mixed && <span className="text-[9px] text-amber-500 font-medium">mixed</span>}
                        </div>
                        );
                      })()}
                    </div>
                  )}
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-gray-400 uppercase border-b">
                        {forecastDrilldown.data[0]?.department && <th className="pb-1 pr-2">Department</th>}
                        {forecastDrilldown.data[0]?.category && !forecastDrilldown.data[0]?.department && <th className="pb-1 pr-2">Category</th>}
                        <th className="pb-1 pr-2">Account #</th>
                        <th className="pb-1 pr-2">Name</th>
                        <th className="pb-1 pr-2 text-right">EUR</th>
                        <th className="pb-1 pr-2 text-right">ILS</th>
                        {_isProjected && <th className="pb-1 pr-2 text-right">%</th>}
                        {_isProjected && <th className="pb-1 pr-2 text-center w-[110px]">Adjust %</th>}
                        {_isProjected && <th className="pb-1 text-right">Impact</th>}
                      </tr>
                    </thead>
                    <tbody>
                      {forecastDrilldown.data.map((r: any, idx: number) => {
                        const catBillKey = `cat__${r.account}__${r.department || r.category || idx}`;
                        const isCatBillExpanded = forecastDrilldown.data?.__expandedCatBill === catBillKey || (forecastDrilldown as any).__expandedCatBill === catBillKey;
                        const colCount = (r.department !== undefined || (r.category && !r.department) ? (_isProjected ? 8 : 5) : (_isProjected ? 7 : 4));
                        return (
                        <Fragment key={idx}>
                        <tr className="border-b border-gray-50 cursor-pointer hover:bg-violet-50 transition-colors"
                            onClick={() => {
                              if (isCatBillExpanded) {
                                setForecastDrilldown(prev => prev ? { ...prev, ...({ __expandedCatBill: null, __catBills: null } as any) } : null);
                              } else {
                                setForecastDrilldown(prev => prev ? { ...prev, ...({ __expandedCatBill: catBillKey, __catBills: null } as any) } : null);
                                const acctNum = r.account || r.accountId;
                                if (acctNum) {
                                  fetch(`/api/ns-vendor-bills?accountId=${acctNum}&month=${forecastDrilldown.mKey}`).then(res => res.json()).then(j => {
                                    setForecastDrilldown(prev => prev ? { ...prev, ...({ __catBills: j.data || [], __catNsAcctId: j.nsAcctId } as any) } : null);
                                  });
                                }
                              }
                            }}>
                          {r.department !== undefined && <td className="py-1.5 pr-2 text-gray-500"><span className="text-gray-400 mr-1">{isCatBillExpanded ? '▼' : '▶'}</span>{r.department}</td>}
                          {r.category && !r.department && <td className="py-1.5 pr-2 text-gray-500"><span className="text-gray-400 mr-1">{isCatBillExpanded ? '▼' : '▶'}</span>{r.category}</td>}
                          <td className="py-1.5 pr-2 font-mono text-violet-600">{r.account}</td>
                          <td className="py-1.5 pr-2 text-gray-700">{r.name}</td>
                          <td className={`py-1.5 pr-2 text-right font-medium ${r.amountEUR >= 0 ? 'text-violet-700' : 'text-green-600'}`}>{fmt(r.amountEUR)}</td>
                          <td className="py-1.5 pr-2 text-right text-blue-500">{fmtILS(r.amountILS)}</td>
                          {_isProjected && <td className="py-1.5 pr-2 text-right text-gray-400">{(() => { const tot = forecastDrilldown.data.reduce((s: number, x: any) => s + Math.abs(x.amountEUR || 0), 0); return tot > 0 ? (Math.abs(r.amountEUR || 0) / tot * 100).toFixed(1) + '%' : '—'; })()}</td>}
                          {_isProjected && (() => {
                            const detKey = `${_catName}||${r.department || ''}||${r.account || ''}`;
                            // Compute effective detail adj (cascading)
                            let _detPct = 0;
                            let _detInherited = false;
                            let _detFromMonth = '';
                            const _allDetMKeys = Object.keys(vendorDetailAdj).filter(k => k <= forecastDrilldown.mKey).sort();
                            for (const adjMK of _allDetMKeys) {
                              const v = vendorDetailAdj[adjMK]?.[detKey];
                              if (v && v.pct !== 0) { _detPct = v.pct; _detInherited = adjMK !== forecastDrilldown.mKey; _detFromMonth = adjMK; }
                              else if (v && v.pct === 0) { _detPct = 0; _detInherited = false; }
                            }
                            const _detImpact = Math.round((r.amountEUR || 0) * (_detPct / 100));
                            return (<>
                              <td className="py-1.5 pr-2" onClick={e => e.stopPropagation()}>
                                <div className="flex items-center justify-center gap-0.5">
                                  <button onClick={() => setVendorDetailAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [detKey]: { pct: _detPct - 1, base: r.amountEUR || 0 } } }))}
                                          className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">−</button>
                                  <input type="text" inputMode="numeric" value={_detPct}
                                         onChange={e => { const v = e.target.value; if (v === '' || v === '-') { setVendorDetailAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [detKey]: { pct: v as any, base: r.amountEUR || 0 } } })); return; } const n = parseInt(v); if (!isNaN(n)) setVendorDetailAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [detKey]: { pct: n, base: r.amountEUR || 0 } } })); }}
                                         onClick={e => e.stopPropagation()}
                                         className={`w-10 text-center text-[11px] font-semibold border rounded px-0.5 py-0.5 ${_detPct !== 0 ? 'text-teal-700 border-teal-300 bg-teal-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                                  <span className="text-[10px] text-gray-400">%</span>
                                  <button onClick={() => setVendorDetailAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [detKey]: { pct: _detPct + 1, base: r.amountEUR || 0 } } }))}
                                          className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">+</button>
                                  {_detPct !== 0 && (
                                    <button onClick={() => {
                                      const mKey = forecastDrilldown.mKey;
                                      const copyToNext = confirm('Also clear for remaining months?');
                                      setVendorDetailAdj(prev => {
                                        const u = { ...prev };
                                        if (u[mKey]) { const mc = { ...u[mKey] }; delete mc[detKey]; u[mKey] = mc; }
                                        if (copyToNext) {
                                          const yr = parseInt(mKey.split('-')[0]); const mo = parseInt(mKey.split('-')[1]);
                                          for (let m = mo + 1; m <= 12; m++) { const mk = `${yr}-${String(m).padStart(2, '0')}`; if (u[mk]) { const mc = { ...u[mk] }; delete mc[detKey]; u[mk] = mc; } }
                                        }
                                        return u;
                                      });
                                    }} className="w-4 h-4 rounded bg-red-50 hover:bg-red-100 text-red-400 hover:text-red-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Clear adjustment">✕</button>
                                  )}
                                  {_detPct !== 0 && (() => {
                                    const mo = parseInt(forecastDrilldown.mKey.split('-')[1]);
                                    return mo < 12 ? (
                                      <button onClick={() => {
                                        const mKey = forecastDrilldown.mKey;
                                        const yr = parseInt(mKey.split('-')[0]);
                                        const moN = parseInt(mKey.split('-')[1]);
                                        setVendorDetailAdj(prev => {
                                          const u = { ...prev };
                                          for (let m = moN + 1; m <= 12; m++) { const mk = `${yr}-${String(m).padStart(2, '0')}`; u[mk] = { ...(u[mk] || {}), [detKey]: { pct: _detPct, base: r.amountEUR || 0 } }; }
                                          return u;
                                        });
                                      }} className="w-4 h-4 rounded bg-teal-100 hover:bg-teal-200 text-teal-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Copy to remaining months">→</button>
                                    ) : null;
                                  })()}
                                </div>
                                {_detInherited && _detPct !== 0 && <div className="text-[9px] text-teal-400 text-center mt-0.5">from {new Date(_detFromMonth + '-01').toLocaleDateString('en-GB', { month: 'short' })}</div>}
                              </td>
                              <td className={`py-1.5 text-right font-bold ${_detPct === 0 ? 'text-gray-300' : _detImpact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                                {_detPct === 0 ? '—' : `${_detImpact >= 0 ? '+' : ''}${fmt(_detImpact)}`}
                              </td>
                            </>);
                          })()}
                        </tr>
                        {isCatBillExpanded && (
                          <tr><td colSpan={colCount} className="p-0">
                            <div className="bg-violet-50 p-2 mb-1 rounded">
                              {!(forecastDrilldown as any).__catBills && <p className="text-xs text-gray-400 italic">Loading from NetSuite...</p>}
                              {(forecastDrilldown as any).__catNsAcctId && nsAccountId && (() => {
                                const [y, m] = forecastDrilldown.mKey.split('-');
                                const endD = new Date(parseInt(y), parseInt(m), 0).getDate();
                                return <p className="text-xs mb-1"><a href={`https://${nsAccountId}.app.netsuite.com/app/reporting/reportrunner.nl?acctid=${(forecastDrilldown as any).__catNsAcctId}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${m}/1/${y}&enddate=${m}/${endD}/${y}`} target="_blank" rel="noreferrer" className="text-violet-600 hover:text-violet-800 underline font-medium" onClick={(e) => e.stopPropagation()}>📋 View full register in NetSuite</a></p>;
                              })()}
                              {(forecastDrilldown as any).__catBills && (forecastDrilldown as any).__catBills.length === 0 && <p className="text-xs text-gray-400 italic">No posting transactions found for this account/month</p>}
                              {(forecastDrilldown as any).__catBills && (forecastDrilldown as any).__catBills.length > 0 && (
                                <table className="w-full text-xs">
                                  <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Date</th><th className="pb-1 pr-2">Bill #</th><th className="pb-1 pr-2">Vendor</th><th className="pb-1 pr-2">Memo</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 w-6"></th></tr></thead>
                                  <tbody>
                                    {(forecastDrilldown as any).__catBills.map((bill: any, bi: number) => (
                                      <tr key={bi} className="border-b border-violet-100">
                                        <td className="py-1 pr-2 text-gray-500">{bill.date ? (() => { const p = String(bill.date).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (p) { const d = new Date(+p[3], +p[2]-1, +p[1]); return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); } const d2 = new Date(bill.date); return isNaN(d2.getTime()) ? String(bill.date).substring(0,10) : d2.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); })() : '-'}</td>
                                        <td className="py-1 pr-2 text-violet-700 font-medium">{bill.billNumber || '-'}{bill.tranType && bill.tranType !== 'VendBill' && <span className="ml-1 text-[9px] text-gray-400 font-normal">{bill.tranType}</span>}</td>
                                        <td className="py-1 pr-2 text-gray-700 truncate max-w-[160px]">{bill.vendor || '-'}</td>
                                        <td className="py-1 pr-2 text-gray-500 truncate max-w-[140px]">{bill.memo || '-'}</td>
                                        <td className="py-1 pr-2 text-right font-medium text-violet-700">{fmt(bill.amount)}</td>
                                        <td className="py-1 text-center">{nsAccountId && bill.billId ? <a href={`https://${nsAccountId}.app.netsuite.com/app/accounting/transactions/${bill.nsUrlType || 'vendbill'}.nl?id=${bill.billId}`} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 text-[10px] font-bold" onClick={(e) => e.stopPropagation()}>NetSuite</a> : null}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              )}
                            </div>
                          </td></tr>
                        )}
                        </Fragment>
                        );
                      })}
                    </tbody>
                    <tfoot>
                      <tr className="border-t-2 font-bold">
                        <td className="py-1.5" colSpan={forecastDrilldown.data[0]?.department || forecastDrilldown.data[0]?.category ? 3 : 2}>Total</td>
                        <td className="py-1.5 pr-2 text-right text-violet-800">{fmt(forecastDrilldown.data.reduce((s: number, r: any) => s + r.amountEUR, 0))}</td>
                        <td className="py-1.5 pr-2 text-right text-blue-700">{fmtILS(forecastDrilldown.data.reduce((s: number, r: any) => s + r.amountILS, 0))}</td>
                        {_isProjected && <td className="py-1.5 pr-2 text-right text-gray-500 font-bold">100%</td>}
                        {_isProjected && <td className="py-1.5 text-center"></td>}
                        {_isProjected && (() => {
                          const totalDetailImpact = forecastDrilldown.data.reduce((s: number, r: any) => {
                            const dk = `${_catName}||${r.department || ''}||${r.account || ''}`;
                            let dp = 0;
                            const adms = Object.keys(vendorDetailAdj).filter(k => k <= forecastDrilldown.mKey).sort();
                            for (const am of adms) { const v = vendorDetailAdj[am]?.[dk]; if (v && v.pct !== 0) dp = v.pct; else if (v && v.pct === 0) dp = 0; }
                            return s + Math.round((r.amountEUR || 0) * (dp / 100));
                          }, 0);
                          return <td className={`py-1.5 text-right font-bold ${totalDetailImpact === 0 ? 'text-gray-300' : totalDetailImpact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                            {totalDetailImpact === 0 ? '—' : `${totalDetailImpact >= 0 ? '+' : ''}${fmt(totalDetailImpact)}`}
                          </td>;
                        })()}
                      </tr>
                    </tfoot>
                  </table>
                  </div>
                  );
                })()}
                {/* ── Pipeline drilldown ── */}
                {forecastDrilldown.type === 'pipeline' && forecastDrilldown.data && forecastDrilldown.data !== 'loading' && (() => {
                  const pd = forecastDrilldown.data;
                  const opps = pd.opps || [];
                  return (
                    <div className="space-y-4">
                      <div className="bg-gray-50 rounded-lg p-3">
                        <p className="text-xs text-gray-400 mb-2 uppercase">Pipeline Waterfall</p>
                        <div className="max-w-md text-[12px] space-y-2">
                          <div className="flex justify-between items-center">
                            <span className="text-teal-700 font-semibold">Total pipeline ({pd.count} opps)</span>
                            <span className="text-teal-700 font-bold">{fmt(pd.total)}</span>
                          </div>
                          <div className="border-t border-dashed border-gray-200"></div>
                          <div className="flex justify-between items-center pl-3">
                            <span className="text-blue-600">× Historical close-won rate</span>
                            <span className="text-blue-700 font-semibold">{pd.winRate}%</span>
                          </div>
                          <div className="flex justify-between items-center pl-3">
                            <span className="text-gray-500">Delay applied (avg days to close)</span>
                            <span className="text-gray-600 font-medium">+{pd.delayMonths} months</span>
                          </div>
                          <div className="border-t border-dashed border-gray-200"></div>
                          <div className="flex justify-between items-center pl-3">
                            <span className="text-red-500">Pipeline excluded ({100 - pd.winRate}%)</span>
                            <span className="text-red-500 font-medium">-{fmt(pd.total - pd.weighted)}</span>
                          </div>
                          <div className="border-t-2 border-gray-300 pt-2 mt-1 flex justify-between items-center">
                            <span className="text-teal-800 font-bold">= Weighted pipeline</span>
                            <span className="text-teal-800 font-bold text-sm">{fmt(pd.weighted)}</span>
                          </div>
                          <div className="flex items-center gap-2 mt-2">
                            <div className="flex-1 bg-gray-200 rounded-full h-2.5 overflow-hidden">
                              <div className="bg-teal-500 h-full rounded-full" style={{ width: `${pd.winRate}%` }}></div>
                            </div>
                            <span className="text-[10px] text-gray-400">{pd.winRate}% kept</span>
                          </div>
                        </div>
                      </div>
                      {/* Monthly pipeline effect breakdown */}
                      {pd.monthlyEffect && (() => {
                        const months = pd.monthlyEffect as { month: string; mKey: string; weighted: number; total: number; count: number; isPast: boolean; isCurrent: boolean; opps: any[] }[];
                        const activeMonths = months.filter(m => m.weighted > 0);
                        if (activeMonths.length === 0) return null;
                        let prevWeighted = 0;
                        let prevOppNames = new Set<string>();
                        const withDelta = activeMonths.map(m => {
                          const delta = m.weighted - prevWeighted;
                          const currentOppNames = new Set((m.opps || []).map((o: any) => o.name));
                          const incrementalOpps = (m.opps || []).filter((o: any) => !prevOppNames.has(o.name));
                          prevWeighted = m.weighted;
                          prevOppNames = currentOppNames;
                          return { ...m, delta, incrementalOpps, incrementalCount: incrementalOpps.length };
                        });
                        const maxWeighted = Math.max(...activeMonths.map(m => m.weighted));
                        const selectedMonth = pd.__selectedMonth as string | undefined;
                        const selectedMonthData = selectedMonth ? withDelta.find(m => m.mKey === selectedMonth) : null;
                        return (
                          <div className="bg-blue-50 rounded-lg p-3">
                            <p className="text-xs text-gray-400 mb-2 uppercase">Monthly Pipeline Effect {selectedMonth ? <span className="text-blue-600">— showing {selectedMonthData?.month} incremental</span> : <span className="text-gray-300">click a row to see incremental opps</span>}</p>
                            <table className="w-full text-[11px]">
                              <thead><tr className="text-left text-gray-400 uppercase border-b border-blue-200">
                                <th className="pb-1 pr-2">Month</th>
                                <th className="pb-1 pr-2 text-right">Cumulative</th>
                                <th className="pb-1 pr-2 text-right">Incremental</th>
                                <th className="pb-1 pr-2 text-right">New Opps</th>
                                <th className="pb-1 w-24"></th>
                              </tr></thead>
                              <tbody>
                                {withDelta.map((m, mi) => (
                                  <tr key={mi}
                                      className={`border-b border-blue-100 cursor-pointer transition-colors ${selectedMonth === m.mKey ? 'bg-blue-200 font-semibold' : m.mKey === forecastDrilldown.mKey ? 'bg-teal-100 font-semibold' : 'hover:bg-blue-100'}`}
                                      onClick={() => {
                                        const newMonth = selectedMonth === m.mKey ? undefined : m.mKey;
                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __selectedMonth: newMonth } } : null);
                                      }}>
                                    <td className={`py-1.5 pr-2 ${m.isPast ? 'text-gray-400' : m.isCurrent ? 'text-blue-700 font-semibold' : 'text-gray-700'}`}>{m.month}</td>
                                    <td className="py-1.5 pr-2 text-right text-teal-700 font-medium">{fmt(m.weighted)}</td>
                                    <td className={`py-1.5 pr-2 text-right ${m.delta > 0 ? 'text-green-700 font-medium' : 'text-gray-400'}`}>{m.delta > 0 ? `+${fmt(m.delta)}` : '-'}</td>
                                    <td className="py-1.5 pr-2 text-right text-gray-500">{m.incrementalCount > 0 ? `+${m.incrementalCount}` : '-'}</td>
                                    <td className="py-1.5 pr-2">
                                      <div className="w-full bg-blue-100 rounded-full h-2 overflow-hidden">
                                        <div className={`h-full rounded-full ${selectedMonth === m.mKey ? 'bg-blue-600' : m.mKey === forecastDrilldown.mKey ? 'bg-teal-500' : 'bg-blue-400'}`} style={{ width: `${maxWeighted > 0 ? (m.weighted / maxWeighted * 100) : 0}%` }}></div>
                                      </div>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {/* Incremental opps for selected month */}
                            {selectedMonthData && selectedMonthData.incrementalOpps.length > 0 && (
                              <div className="mt-3 pt-3 border-t border-blue-200">
                                <p className="text-xs text-blue-600 font-medium mb-2">{selectedMonthData.incrementalOpps.length} new opportunities added in {selectedMonthData.month} (+{fmt(selectedMonthData.delta)} weighted)</p>
                                <table className="w-full text-[11px]">
                                  <thead><tr className="text-left text-gray-400 uppercase border-b border-blue-200">
                                    <th className="pb-1 pr-2">Opportunity</th><th className="pb-1 pr-2">Stage</th><th className="pb-1 pr-2 text-right">Prob</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 pr-2 text-right">Close Date</th><th className="pb-1 pr-2">Owner</th>
                                  </tr></thead>
                                  <tbody>
                                    {selectedMonthData.incrementalOpps.sort((a: any, b: any) => b.amount - a.amount).map((o: any, oi: number) => (
                                      <tr key={oi} className="border-b border-blue-100">
                                        <td className="py-1.5 pr-2 text-gray-700 font-medium max-w-[200px] truncate">{o.name}</td>
                                        <td className="py-1.5 pr-2 text-gray-500">{o.stage}</td>
                                        <td className="py-1.5 pr-2 text-right text-blue-600">{o.probability}%</td>
                                        <td className="py-1.5 pr-2 text-right font-medium text-teal-700">{fmt(o.amount)}</td>
                                        <td className="py-1.5 pr-2 text-right text-gray-500">{o.closeDate}</td>
                                        <td className="py-1.5 pr-2 text-gray-500 max-w-[100px] truncate">{o.owner}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                  <tfoot><tr className="border-t border-blue-300 font-bold">
                                    <td className="py-1 pr-2" colSpan={3}>Total</td>
                                    <td className="py-1 pr-2 text-right text-teal-700">{fmt(selectedMonthData.incrementalOpps.reduce((s: number, o: any) => s + o.amount, 0))}</td>
                                    <td colSpan={2}></td>
                                  </tr></tfoot>
                                </table>
                              </div>
                            )}
                          </div>
                        );
                      })()}
                      {opps.length > 0 && !pd.__selectedMonth && (
                        <div>
                          <p className="text-xs text-gray-400 mb-2">{opps.length} opportunities (prob &lt;{pipelineMinProb}%, closed by {forecastDrilldown.mKey} minus {pd.delayMonths}m delay)</p>
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">Opportunity</th><th className="pb-1 pr-2">Stage</th><th className="pb-1 pr-2 text-right">Prob</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 pr-2 text-right">Close Date</th><th className="pb-1 pr-2">Owner</th>
                            </tr></thead>
                            <tbody>
                              {opps.sort((a: any, b: any) => b.amount - a.amount).map((o: any, oi: number) => (
                                <tr key={oi} className="border-b border-gray-100">
                                  <td className="py-1.5 pr-2 text-gray-700 font-medium max-w-[200px] truncate">{o.name}</td>
                                  <td className="py-1.5 pr-2 text-gray-500">{o.stage}</td>
                                  <td className="py-1.5 pr-2 text-right text-blue-600">{o.probability}%</td>
                                  <td className="py-1.5 pr-2 text-right font-medium text-teal-700">{fmt(o.amount)}</td>
                                  <td className="py-1.5 pr-2 text-right text-gray-500">{o.closeDate}</td>
                                  <td className="py-1.5 pr-2 text-gray-500 max-w-[120px] truncate">{o.owner}</td>
                                </tr>
                              ))}
                            </tbody>
                            <tfoot>
                              <tr className="border-t-2 border-gray-300 font-bold">
                                <td className="py-1.5 pr-2" colSpan={3}>Total ({opps.length} opps)</td>
                                <td className="py-1.5 pr-2 text-right text-teal-700">{fmt(pd.total)}</td>
                                <td colSpan={2}></td>
                              </tr>
                            </tfoot>
                          </table>
                        </div>
                      )}
                    </div>
                  );
                })()}
                {Array.isArray(forecastDrilldown.data) && forecastDrilldown.data.length === 0 && (
                  <p className="text-gray-400 text-center py-8">No data available for this month</p>
                )}
                {forecastDrilldown.data && !Array.isArray(forecastDrilldown.data) && forecastDrilldown.data !== 'loading' && forecastDrilldown.data.budget === undefined && forecastDrilldown.data.forecast === undefined && (
                  <div>
                    {/* Vendor budget vs historical comparison */}
                    {forecastDrilldown.type === 'vendors' && forecastDrilldown.data.__vendorMeta && (() => {
                      const meta = forecastDrilldown.data.__vendorMeta;
                      const gap = meta.budgetTotal - meta.histAvg;
                      const vendorOverrides = (sfBudget.overrides || []).filter(o => o.mKey === forecastDrilldown.mKey);
                      const hasVendorOverrides = vendorOverrides.length > 0;
                      const overrideTotal = vendorOverrides.reduce((s, o) => s + (o.mode === 'Override' ? (o.newVal - o.oldVal) : o.amountEUR), 0);
                      return (
                        <div className="bg-gray-50 rounded-lg p-3 mb-3">
                          <p className="text-xs text-gray-400 mb-2 uppercase">Budget vs Historical{hasVendorOverrides ? ' — overrides applied' : ''}</p>
                          <table className="w-full text-xs">
                            <tbody>
                              <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600">Snowflake Budget (original)</td><td className="py-1.5 text-right font-bold text-violet-700">{fmt(meta.budgetTotal - overrideTotal)}</td></tr>
                              {hasVendorOverrides && vendorOverrides.map((ov, oi) => (
                                <tr key={oi} className="border-b border-orange-200 bg-orange-50">
                                  <td className="py-2 pl-3">
                                    <span className="inline-block w-2 h-2 rounded-full bg-orange-500 mr-1.5 align-middle"></span>
                                    <span className="text-orange-700 font-medium">{ov.category || ov.account}</span>
                                    <span className="ml-2 text-[9px] bg-orange-200 text-orange-700 px-1.5 py-0.5 rounded font-medium align-middle">{ov.mode}</span>
                                    {ov.comments && <div className="text-[10px] text-orange-500 italic mt-0.5 pl-4 truncate">{ov.comments}</div>}
                                    <div className="text-[9px] text-orange-400 pl-4">via Google Sheets</div>
                                  </td>
                                  <td className={`py-2 text-right font-bold text-sm align-top whitespace-nowrap ${ov.mode === 'Override' ? 'text-orange-700' : (ov.amountEUR >= 0 ? 'text-red-600' : 'text-green-700')}`}>
                                    {ov.mode === 'Override' ? fmt(ov.newVal - ov.oldVal) : `${ov.amountEUR >= 0 ? '+' : ''}${fmt(ov.amountEUR)}`}
                                  </td>
                                </tr>
                              ))}
                              {hasVendorOverrides && (
                                <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600 font-semibold">Budget (after overrides)</td><td className="py-1.5 text-right font-bold text-green-700">{fmt(meta.budgetTotal)}</td></tr>
                              )}
                              {meta.histAvg > 0 && (
                                <tr className="border-b border-gray-200 cursor-pointer hover:bg-blue-50" onClick={() => {
                                  setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __showHistDetail: !prev.data.__showHistDetail } } : null);
                                }}>
                                  <td className="py-1.5 text-blue-600 underline">Historical Avg (12m trailing) <span className="text-[10px] text-gray-400">click to expand</span></td>
                                  <td className="py-1.5 text-right font-bold text-blue-700">{fmt(meta.histAvg)}</td>
                                </tr>
                              )}
                              {meta.actual > 0 && <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600">Snowflake Actual (this month)</td><td className="py-1.5 text-right font-bold text-amber-700">{fmt(meta.actual)}</td></tr>}
                              <tr className="border-b border-gray-200"><td className="py-1.5 text-gray-600">Used in Forecast</td><td className="py-1.5 text-right font-bold text-green-700">{fmt(meta.used)}</td></tr>
                              {meta.histAvg > 0 && <tr><td className="py-1.5 text-gray-600">Budget − Historical Gap</td><td className={`py-1.5 text-right font-bold ${gap >= 0 ? 'text-red-600' : 'text-green-700'}`}>{gap >= 0 ? '+' : ''}{fmt(gap)}</td></tr>}
                            </tbody>
                          </table>
                          {/* Historical months breakdown */}
                          {forecastDrilldown.data.__showHistDetail && meta.histMonths && meta.histMonths.length > 0 && (
                            <div className="mt-3 pt-3 border-t border-gray-200">
                              <p className="text-xs text-gray-500 mb-2 font-medium">12-Month Trailing Composition (Snowflake Actuals — FCT_EXPENSE)</p>
                              <table className="w-full text-xs">
                                <thead><tr className="text-left text-gray-400 uppercase border-b">
                                  <th className="pb-1 pr-2">Month</th><th className="pb-1 pr-2 text-right">Vendors</th><th className="pb-1 pr-2 text-right">vs Avg</th>
                                </tr></thead>
                                <tbody>
                                  {meta.histMonths.map((h: any) => {
                                    const diff = h.vendors - meta.histAvg;
                                    const monthLabel = new Date(h.month + '-01').toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                                    const isExpanded = forecastDrilldown.data.__expandedHistMonth === h.month;
                                    return (
                                      <Fragment key={h.month}>
                                      <tr className="border-b border-gray-100 cursor-pointer hover:bg-blue-50 transition-colors" onClick={() => {
                                        if (isExpanded) {
                                          setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedHistMonth: null, __histMonthVendors: null } } : null);
                                        } else {
                                          setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedHistMonth: h.month, __histMonthVendors: null } } : null);
                                          fetch(`/api/sf-vendor-breakdown?month=${h.month}`).then(r => r.json()).then(j => {
                                            setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __histMonthVendors: j.data || [] } } : null);
                                          });
                                        }
                                      }}>
                                        <td className="py-1.5 pr-2 text-blue-600 underline decoration-dotted">{monthLabel} {isExpanded ? '▼' : '▶'}</td>
                                        <td className="py-1.5 pr-2 text-right font-medium text-blue-700">{fmt(h.vendors)}</td>
                                        <td className={`py-1.5 pr-2 text-right text-xs ${diff >= 0 ? 'text-red-500' : 'text-green-600'}`}>{diff >= 0 ? '+' : ''}{fmt(diff)}</td>
                                      </tr>
                                      {isExpanded && (
                                        <tr><td colSpan={3} className="p-0">
                                          <div className="bg-blue-50 p-2 mb-1 rounded">
                                            {!forecastDrilldown.data.__histMonthVendors && <p className="text-xs text-gray-400 italic">Loading vendor breakdown...</p>}
                                            {forecastDrilldown.data.__histMonthVendors && forecastDrilldown.data.__histMonthVendors.length === 0 && <p className="text-xs text-gray-400 italic">No vendor data for this month</p>}
                                            {forecastDrilldown.data.__histMonthVendors && forecastDrilldown.data.__histMonthVendors.length > 0 && (() => {
                                              // Aggregate by category, keep raw items for drill-down
                                              const byCat: Record<string, { total: number; items: any[] }> = {};
                                              for (const v of forecastDrilldown.data.__histMonthVendors) {
                                                const cat = v.category || v.name || 'Other';
                                                if (!byCat[cat]) byCat[cat] = { total: 0, items: [] };
                                                byCat[cat].total += (v.amountEUR || v.amount || 0);
                                                byCat[cat].items.push(v);
                                              }
                                              const sorted = Object.entries(byCat).sort((a, b) => b[1].total - a[1].total);
                                              const expandedCat = forecastDrilldown.data.__expandedHistCat;
                                              const histMonth = forecastDrilldown.data.__expandedHistMonth;
                                              return (
                                              <table className="w-full text-xs">
                                                <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Category</th><th className="pb-1 pr-2 text-right">EUR</th></tr></thead>
                                                <tbody>
                                                  {sorted.slice(0, 20).map(([cat, data], vi) => (
                                                    <Fragment key={vi}>
                                                    <tr className="border-b border-blue-100 cursor-pointer hover:bg-blue-100 transition-colors" onClick={() => {
                                                      setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedHistCat: expandedCat === cat ? null : cat } } : null);
                                                    }}>
                                                      <td className="py-1 pr-2 text-blue-700 underline decoration-dotted">{expandedCat === cat ? '▼' : '▶'} {cat}</td>
                                                      <td className="py-1 pr-2 text-right font-medium text-blue-700">{fmt(data.total)}</td>
                                                    </tr>
                                                    {expandedCat === cat && (
                                                      <tr><td colSpan={2} className="p-0">
                                                        <div className="bg-blue-100/50 p-2 mb-1 rounded">
                                                          <table className="w-full text-xs">
                                                            <thead><tr className="text-left text-gray-400 uppercase"><th className="pb-1 pr-2">Account</th><th className="pb-1 pr-2">Dept</th><th className="pb-1 pr-2 text-right">EUR</th><th className="pb-1 w-8"></th></tr></thead>
                                                            <tbody>
                                                              {data.items.sort((a: any, b: any) => (b.amountEUR || 0) - (a.amountEUR || 0)).map((item: any, ii: number) => {
                                                                const acctKey = `${histMonth}__${item.accountId}__${item.department}`;
                                                                const isAcctExpanded = forecastDrilldown.data?.__expandedAcct === acctKey;
                                                                return (
                                                                <Fragment key={ii}>
                                                                <tr className="border-b border-blue-100/50 cursor-pointer hover:bg-blue-200/50 transition-colors" onClick={(e) => {
                                                                  e.stopPropagation();
                                                                  if (isAcctExpanded) {
                                                                    setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedAcct: null, __acctBills: null } } : null);
                                                                  } else {
                                                                    setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __expandedAcct: acctKey, __acctBills: null } } : null);
                                                                    if ((item.account || item.accountId) && histMonth) {
                                                                      fetch(`/api/ns-vendor-bills?accountId=${item.account || item.accountId}&month=${histMonth}`).then(r => r.json()).then(j => {
                                                                        setForecastDrilldown(prev => prev ? { ...prev, data: { ...prev.data, __acctBills: j.data || [], __acctNsAcctId: j.nsAcctId } } : null);
                                                                      });
                                                                    }
                                                                  }
                                                                }}>
                                                                  <td className="py-1 pr-2 text-gray-700 truncate max-w-[250px]">
                                                                    <span className="text-gray-400 mr-1">{isAcctExpanded ? '▼' : '▶'}</span>
                                                                    {item.name || item.account || '-'} {item.account ? <span className="text-gray-400">({item.account})</span> : ''}
                                                                  </td>
                                                                  <td className="py-1 pr-2 text-gray-500 truncate max-w-[120px]">{item.department || '-'}</td>
                                                                  <td className="py-1 pr-2 text-right font-medium text-blue-700">{fmt(item.amountEUR || 0)}</td>
                                                                  <td className="py-1 w-8"></td>
                                                                </tr>
                                                                {isAcctExpanded && (
                                                                  <tr><td colSpan={4} className="p-0">
                                                                    <div className="bg-white/80 p-2 mb-1 rounded border border-blue-100">
                                                                      {!forecastDrilldown.data.__acctBills && <p className="text-xs text-gray-400 italic">Loading bills from NetSuite...</p>}
                                                                      {forecastDrilldown.data.__acctBills && forecastDrilldown.data.__acctBills.length === 0 && <p className="text-xs text-gray-400 italic">No posting transactions found for this account/month</p>}
                                                                      {forecastDrilldown.data.__acctBills && forecastDrilldown.data.__acctBills.length > 0 && (
                                                                        <table className="w-full text-xs">
                                                                          <thead><tr className="text-left text-gray-400 uppercase border-b"><th className="pb-1 pr-2">Date</th><th className="pb-1 pr-2">Bill #</th><th className="pb-1 pr-2">Vendor</th><th className="pb-1 pr-2">Memo</th><th className="pb-1 pr-2 text-right">Amount</th><th className="pb-1 w-6"></th></tr></thead>
                                                                          <tbody>
                                                                            {forecastDrilldown.data.__acctBills.map((bill: any, bi: number) => {
                                                                              const billUrl = nsAccountId && bill.billId ? `https://${nsAccountId}.app.netsuite.com/app/accounting/transactions/${bill.nsUrlType || 'vendbill'}.nl?id=${bill.billId}` : null;
                                                                              return (
                                                                                <tr key={bi} className="border-b border-gray-100 hover:bg-blue-50">
                                                                                  <td className="py-1 pr-2 text-gray-500 whitespace-nowrap">{bill.date ? bill.date.substring(0, 10) : '-'}</td>
                                                                                  <td className="py-1 pr-2 font-mono text-blue-600">{billUrl ? <a href={billUrl} target="_blank" rel="noreferrer" className="underline hover:text-blue-800" onClick={(e) => e.stopPropagation()}>{bill.billNumber || bill.billId}</a> : (bill.billNumber || '-')}</td>
                                                                                  <td className="py-1 pr-2 text-gray-700 truncate max-w-[180px]">{bill.vendor || '-'}</td>
                                                                                  <td className="py-1 pr-2 text-gray-500 truncate max-w-[180px]">{bill.memo || '-'}</td>
                                                                                  <td className="py-1 pr-2 text-right font-medium text-blue-700">{fmt(bill.amount)}</td>
                                                                                  <td className="py-1 text-center">{billUrl ? <a href={billUrl} target="_blank" rel="noreferrer" className="text-orange-500 hover:text-orange-700 font-medium" onClick={(e) => e.stopPropagation()}>↗</a> : ''}</td>
                                                                                </tr>
                                                                              );
                                                                            })}
                                                                          </tbody>
                                                                        </table>
                                                                      )}
                                                                      {forecastDrilldown.data.__acctNsAcctId && nsAccountId && (() => {
                                                                        const [y, m] = histMonth.split('-');
                                                                        const endD = new Date(Number(y), Number(m), 0).getDate();
                                                                        return <a href={`https://${nsAccountId}.app.netsuite.com/app/reporting/reportrunner.nl?acctid=${forecastDrilldown.data.__acctNsAcctId}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${m}/1/${y}&enddate=${m}/${endD}/${y}`} target="_blank" rel="noreferrer" className="inline-block mt-1 text-[10px] text-orange-600 hover:text-orange-800 underline" onClick={(e) => e.stopPropagation()}>📋 View full register in NetSuite</a>;
                                                                      })()}
                                                                    </div>
                                                                  </td></tr>
                                                                )}
                                                                </Fragment>
                                                                );
                                                              })}
                                                            </tbody>
                                                          </table>
                                                        </div>
                                                      </td></tr>
                                                    )}
                                                    </Fragment>
                                                  ))}
                                                  {sorted.length > 20 && (
                                                    <tr><td colSpan={2} className="py-1 text-gray-400 italic">+ {sorted.length - 20} more categories</td></tr>
                                                  )}
                                                </tbody>
                                              </table>
                                              );
                                            })()}
                                          </div>
                                        </td></tr>
                                      )}
                                      </Fragment>
                                    );
                                  })}
                                </tbody>
                                <tfoot>
                                  <tr className="border-t-2 font-bold">
                                    <td className="py-1.5">Average ({meta.histMonths.length}m)</td>
                                    <td className="py-1.5 pr-2 text-right text-blue-800">{fmt(meta.histAvg)}</td>
                                    <td className="py-1.5 pr-2 text-right text-gray-400">—</td>
                                  </tr>
                                  <tr className="font-medium text-xs">
                                    <td className="py-1 text-gray-500">Min / Max</td>
                                    <td className="py-1 pr-2 text-right text-gray-500">{fmt(Math.min(...meta.histMonths.map((h: any) => h.vendors)))} / {fmt(Math.max(...meta.histMonths.map((h: any) => h.vendors)))}</td>
                                    <td className="py-1"></td>
                                  </tr>
                                </tfoot>
                              </table>
                            </div>
                          )}
                        </div>
                      );
                    })()}
                    {forecastDrilldown.type !== 'churn' && (() => {
                      const _now3 = new Date();
                      const _curMKey3 = `${_now3.getFullYear()}-${String(_now3.getMonth()+1).padStart(2,'0')}`;
                      const _isFutureCat = forecastDrilldown.mKey >= _curMKey3;
                      const _catEntries = Object.entries(forecastDrilldown.data as Record<string, number>).filter(([k]) => !k.startsWith('__'));
                      const _catTotal = _catEntries.reduce((s, [, v]) => s + Math.abs(typeof v === 'number' ? v : 0), 0);
                      // Compute effective vendor category adjustments (cascading)
                      const _effVendorAdj: Record<string, { pct: number; inherited: boolean; fromMonth?: string }> = {};
                      if (_isFutureCat) {
                        const _allVcMKeys = Object.keys(vendorCatAdj).filter(k => k <= forecastDrilldown.mKey).sort();
                        for (const adjMK of _allVcMKeys) {
                          for (const [cat2, pct2] of Object.entries(vendorCatAdj[adjMK])) {
                            if (pct2 !== 0) _effVendorAdj[cat2] = { pct: pct2, inherited: adjMK !== forecastDrilldown.mKey, fromMonth: adjMK };
                            else delete _effVendorAdj[cat2];
                          }
                        }
                      }
                      const _hasAnyVcAdj = Object.keys(_effVendorAdj).length > 0;
                      const _totalVcImpact = _catEntries.reduce((s, [cat, amt]) => {
                        const adj = _effVendorAdj[cat]?.pct || 0;
                        return s + Math.round((typeof amt === 'number' ? amt : 0) * (adj / 100));
                      }, 0);
                      return (<><p className="text-xs text-gray-500 mb-2">
                      {forecastDrilldown.type === 'vendors' ? 'Snowflake Budget Breakdown — click category for details' : 'Budget Breakdown'}
                    </p>
                    <table className="w-full text-xs">
                      <thead><tr className="text-left text-gray-400 uppercase border-b">
                        <th className="pb-1 pr-2">Category</th><th className="pb-1 pr-2 text-right">EUR</th><th className="pb-1 pr-2 text-right">%</th>
                        {_isFutureCat && <th className="pb-1 pr-2 text-center w-[120px]">Adjust %</th>}
                        {_isFutureCat && <th className="pb-1 text-right">Impact</th>}
                      </tr></thead>
                      <tbody>
                        {_catEntries.sort(([,a]: any,[,b]: any) => b - a).map(([cat, amt]: any) => {
                          const vcAdj = _effVendorAdj[cat];
                          const vcPct = vcAdj?.pct || 0;
                          const vcInherited = vcAdj?.inherited || false;
                          const vcImpact = Math.round((typeof amt === 'number' ? amt : 0) * (vcPct / 100));
                          return (
                          <tr key={cat} className={`border-b border-gray-50 cursor-pointer hover:bg-violet-50 ${vcInherited && vcPct !== 0 ? 'bg-teal-50/50' : ''}`}
                              onClick={() => {
                                const savedCategories = forecastDrilldown.data as Record<string, number>;
                                setForecastDrilldown(prev => prev ? { ...prev, data: 'loading', categoryData: savedCategories, categoryName: cat } : null);
                                if (forecastDrilldown.mKey >= _curMKey3) {
                                  fetch(`/api/sf-budget-detail?month=${forecastDrilldown.mKey}&category=${encodeURIComponent(cat)}`)
                                    .then(r => r.json())
                                    .then(r => setForecastDrilldown(prev => prev ? { ...prev, data: r.data || [] } : null));
                                } else {
                                  fetch(`/api/sf-vendor-breakdown?month=${forecastDrilldown.mKey}`)
                                    .then(r => r.json())
                                    .then(r => {
                                      const filtered = (r.data || []).filter((d: any) => d.category === cat);
                                      setForecastDrilldown(prev => prev ? { ...prev, data: filtered.length > 0 ? filtered : r.data || [] } : null);
                                    });
                                }
                              }}>
                            <td className="py-1.5 pr-2 text-violet-600 hover:text-violet-800 underline cursor-pointer">{cat}{(sfBudget.overrides || []).some(o => o.mKey === forecastDrilldown.mKey && o.category === cat) && <span className="ml-1 inline-block w-2 h-2 rounded-full bg-orange-500" title="Has Google Sheets override"></span>}</td>
                            <td className="py-1.5 pr-2 text-right font-medium text-violet-700">{fmt(amt)}</td>
                            <td className="py-1.5 pr-2 text-right text-gray-400">{_catTotal > 0 ? (Math.abs(typeof amt === 'number' ? amt : 0) / _catTotal * 100).toFixed(1) + '%' : '—'}</td>
                            {_isFutureCat && <td className="py-1.5 pr-2" onClick={e => e.stopPropagation()}>
                              <div className="flex items-center justify-center gap-0.5">
                                <button onClick={() => setVendorCatAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [cat]: vcPct - 1 } }))}
                                        className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">−</button>
                                <input type="text" inputMode="numeric" value={vcPct}
                                       onChange={e => { const v = e.target.value; if (v === '' || v === '-') { setVendorCatAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [cat]: v as any } })); return; } const n = parseInt(v); if (!isNaN(n)) setVendorCatAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [cat]: n } })); }}
                                       onClick={e => e.stopPropagation()}
                                       className={`w-10 text-center text-[11px] font-semibold border rounded px-0.5 py-0.5 ${vcPct !== 0 ? 'text-teal-700 border-teal-300 bg-teal-50' : 'text-gray-400 border-gray-200 bg-white'}`} />
                                <span className="text-[10px] text-gray-400">%</span>
                                <button onClick={() => setVendorCatAdj(prev => ({ ...prev, [forecastDrilldown.mKey]: { ...(prev[forecastDrilldown.mKey] || {}), [cat]: vcPct + 1 } }))}
                                        className="w-5 h-5 rounded bg-white hover:bg-teal-100 text-gray-500 text-xs flex items-center justify-center font-bold border border-teal-200">+</button>
                                {vcPct !== 0 && (
                                  <button onClick={() => {
                                    const mKey = forecastDrilldown.mKey;
                                    const copyToNext = confirm('Also clear for remaining months?');
                                    setVendorCatAdj(prev => {
                                      const u = { ...prev };
                                      if (u[mKey]) { const mc = { ...u[mKey] }; delete mc[cat]; u[mKey] = mc; }
                                      if (copyToNext) {
                                        const yr = parseInt(mKey.split('-')[0]); const mo = parseInt(mKey.split('-')[1]);
                                        for (let m = mo + 1; m <= 12; m++) { const mk = `${yr}-${String(m).padStart(2, '0')}`; if (u[mk]) { const mc = { ...u[mk] }; delete mc[cat]; u[mk] = mc; } }
                                      }
                                      return u;
                                    });
                                  }} className="w-4 h-4 rounded bg-red-50 hover:bg-red-100 text-red-400 hover:text-red-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Clear adjustment">✕</button>
                                )}
                                {vcPct !== 0 && (() => {
                                  const mo = parseInt(forecastDrilldown.mKey.split('-')[1]);
                                  return mo < 12 ? (
                                    <button onClick={() => {
                                      const mKey = forecastDrilldown.mKey;
                                      const yr = parseInt(mKey.split('-')[0]);
                                      const moN = parseInt(mKey.split('-')[1]);
                                      setVendorCatAdj(prev => {
                                        const u = { ...prev };
                                        for (let m = moN + 1; m <= 12; m++) { const mk = `${yr}-${String(m).padStart(2, '0')}`; u[mk] = { ...(u[mk] || {}), [cat]: vcPct }; }
                                        return u;
                                      });
                                    }} className="w-4 h-4 rounded bg-teal-100 hover:bg-teal-200 text-teal-600 text-[10px] flex items-center justify-center font-bold leading-none" title="Copy to remaining months">→</button>
                                  ) : null;
                                })()}
                              </div>
                              {vcInherited && vcPct !== 0 && <div className="text-[9px] text-teal-400 text-center mt-0.5">from {new Date(vcAdj.fromMonth + '-01').toLocaleDateString('en-GB', { month: 'short' })}</div>}
                            </td>}
                            {_isFutureCat && <td className={`py-1.5 text-right font-bold ${vcPct === 0 ? 'text-gray-300' : vcImpact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                              {vcPct === 0 ? '—' : `${vcImpact >= 0 ? '+' : ''}${fmt(vcImpact)}`}
                            </td>}
                          </tr>
                          );
                        })}
                      </tbody>
                      <tfoot><tr className="border-t-2 font-bold">
                        <td className="py-1.5">Total</td>
                        <td className="py-1.5 pr-2 text-right text-violet-800">{fmt(Object.entries(forecastDrilldown.data as Record<string, any>).filter(([k]) => !k.startsWith('__')).reduce((s, [, v]) => s + (typeof v === 'number' ? v : 0), 0))}</td>
                        <td className="py-1.5 pr-2 text-right text-gray-500 font-bold">100%</td>
                        {_isFutureCat && <td className="py-1.5 text-center">
                          {_hasAnyVcAdj && <button onClick={() => setVendorCatAdj({})} className="text-[9px] text-red-500 hover:text-red-700 underline">reset all</button>}
                        </td>}
                        {_isFutureCat && <td className={`py-1.5 text-right font-bold ${_totalVcImpact === 0 ? 'text-gray-300' : _totalVcImpact >= 0 ? 'text-red-600' : 'text-green-700'}`}>
                          {_totalVcImpact === 0 ? '—' : `${_totalVcImpact >= 0 ? '+' : ''}${fmt(_totalVcImpact)}`}
                        </td>}
                      </tr></tfoot>
                    </table>
                    </>);
                    })()}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ── Open Pipeline (not closed-won) ── */}
        {activeCompany !== 'consolidated' && companyConfig.hasSF && sfPipeline.length > 0 && (() => {
          const filtered = pipelineMinProb > 0 ? sfPipeline.filter(o => o.probability >= pipelineMinProb) : sfPipeline;
          // Monthly revenue impact: each opp adds its amount to months from close date onward (recurring MRR)
          const revenueImpact: Record<string, { base: number; pipeline: number; total: number; opps: number }> = {};
          const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
          for (let mi = 0; mi < 12; mi++) {
            const mKey = `2026-${String(mi + 1).padStart(2, '0')}`;
            const baseRev = sfRevenuePaid[mKey]?.revenue || 0;
            // Opps that close on or before this month → recurring revenue from close month onward
            const pipelineAdd = filtered.filter(o => o.closeDate.substring(0, 7) <= mKey).reduce((s, o) => s + o.amount, 0);
            revenueImpact[mKey] = { base: baseRev, pipeline: pipelineAdd, total: baseRev + pipelineAdd, opps: filtered.filter(o => o.closeDate.substring(0, 7) <= mKey).length };
          }
          return (
          <div className="bg-white rounded-xl shadow-sm border p-5 mt-4">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="text-base font-bold text-gray-800 flex items-center gap-2">
                  <TrendingUp className="w-4 h-4 text-blue-500" />
                  Sales Pipeline (Not Closed-Won)
                </h2>
                <p className="text-xs text-gray-400 mt-0.5">Salesforce DIM_OPPORTUNITY • {filtered.length} of {sfPipeline.length} opportunities{pipelineMinProb > 0 ? ` (≥${pipelineMinProb}% probability)` : ''}</p>
              </div>
              <div className="flex items-center gap-4">
                {/* Probability filter */}
                <div className="flex items-center gap-1.5 bg-blue-50 rounded-lg px-3 py-2 border border-blue-200">
                  <span className="text-xs text-blue-600 font-medium whitespace-nowrap">Min Prob:</span>
                  <button onClick={() => setPipelineMinProb(p => Math.max(0, p - 10))} className="w-5 h-5 rounded bg-white border border-blue-200 hover:bg-blue-100 text-blue-600 text-xs flex items-center justify-center font-bold">−</button>
                  <input type="number" value={pipelineMinProb} onChange={e => setPipelineMinProb(Math.max(0, Math.min(100, parseInt(e.target.value) || 0)))}
                         className="w-14 text-center text-xs font-semibold border rounded px-1 py-0.5 text-blue-700 border-blue-200 bg-white" />
                  <span className="text-xs text-blue-400">%</span>
                  <button onClick={() => setPipelineMinProb(p => Math.min(100, p + 10))} className="w-5 h-5 rounded bg-white border border-blue-200 hover:bg-blue-100 text-blue-600 text-xs flex items-center justify-center font-bold">+</button>
                </div>
                <div className="text-right">
                  <div className="text-xs text-gray-400">Weighted Pipeline</div>
                  <div className="text-lg font-bold text-blue-700">{fmt(filtered.reduce((s, o) => s + o.weighted, 0))}</div>
                  <div className="text-[10px] text-gray-400">Total: {fmt(filtered.reduce((s, o) => s + o.amount, 0))}</div>
                </div>
              </div>
            </div>

            {/* Monthly Revenue Impact */}
            <div className="bg-blue-50 rounded-lg border border-blue-200 p-3 mb-4">
              <p className="text-xs text-blue-600 font-semibold mb-2">Monthly Revenue Impact — if filtered pipeline closes as expected (recurring from close month onward)</p>
              <table className="w-full text-xs">
                <thead><tr className="text-left text-blue-400 uppercase border-b border-blue-200">
                  <th className="pb-1 pr-2">Month</th>
                  <th className="pb-1 pr-2 text-right">Current Revenue</th>
                  <th className="pb-1 pr-2 text-right">+ Pipeline</th>
                  <th className="pb-1 pr-2 text-right">= Projected</th>
                  <th className="pb-1 pr-2 text-right">Opps</th>
                </tr></thead>
                <tbody>
                  {Object.keys(revenueImpact).sort().map(mKey => {
                    const r = revenueImpact[mKey];
                    const monthLabel = monthNames[parseInt(mKey.split('-')[1]) - 1]?.substring(0, 3) + ' ' + mKey.split('-')[0];
                    return (
                      <tr key={mKey} className="border-b border-blue-100">
                        <td className="py-1.5 pr-2 text-gray-600 font-medium">{monthLabel}</td>
                        <td className="py-1.5 pr-2 text-right text-gray-500">{r.base > 0 ? fmt(r.base) : '-'}</td>
                        <td className="py-1.5 pr-2 text-right font-medium text-green-600">{r.pipeline > 0 ? `+${fmt(r.pipeline)}` : '-'}</td>
                        <td className="py-1.5 pr-2 text-right font-bold text-blue-700">{r.base > 0 || r.pipeline > 0 ? fmt(r.total) : '-'}</td>
                        <td className="py-1.5 pr-2 text-right text-gray-400">{r.opps > 0 ? r.opps : '-'}</td>
                      </tr>
                    );
                  })}
                </tbody>
                <tfoot><tr className="border-t-2 border-blue-300 font-bold">
                  <td className="py-1.5">Annual</td>
                  <td className="py-1.5 pr-2 text-right text-gray-600">{fmt(Object.values(revenueImpact).reduce((s, r) => s + r.base, 0))}</td>
                  <td className="py-1.5 pr-2 text-right text-green-700">+{fmt(Object.values(revenueImpact).reduce((s, r) => s + r.pipeline, 0))}</td>
                  <td className="py-1.5 pr-2 text-right text-blue-800">{fmt(Object.values(revenueImpact).reduce((s, r) => s + r.total, 0))}</td>
                  <td className="py-1.5 pr-2 text-right text-gray-400">{filtered.length}</td>
                </tr></tfoot>
              </table>
            </div>

            {/* Summary by stage */}
            <div className="grid grid-cols-5 gap-2 mb-4">
              {(() => {
                const stageMap: Record<string, { count: number; total: number; weighted: number; prob: number }> = {};
                filtered.forEach(o => {
                  if (!stageMap[o.stage]) stageMap[o.stage] = { count: 0, total: 0, weighted: 0, prob: 0 };
                  stageMap[o.stage].count++;
                  stageMap[o.stage].total += o.amount;
                  stageMap[o.stage].weighted += o.weighted;
                  stageMap[o.stage].prob = o.probability;
                });
                const stageOrder = ['Best Case', 'Contract Sent', 'Negotiation', 'Test', 'New / Qualified'];
                const stageColors: Record<string, string> = { 'Best Case': 'bg-green-50 border-green-200 text-green-700', 'Contract Sent': 'bg-blue-50 border-blue-200 text-blue-700', 'Negotiation': 'bg-amber-50 border-amber-200 text-amber-700', 'Test': 'bg-purple-50 border-purple-200 text-purple-700', 'New / Qualified': 'bg-gray-50 border-gray-200 text-gray-600' };
                return stageOrder.filter(s => stageMap[s]).map(stage => (
                  <div key={stage} className={`rounded-lg border p-2 ${stageColors[stage] || 'bg-gray-50 border-gray-200'}`}>
                    <div className="text-[10px] font-medium truncate">{stage}</div>
                    <div className="text-sm font-bold">{fmt(stageMap[stage].weighted)}</div>
                    <div className="text-[10px] opacity-70">{stageMap[stage].count} opps • {stageMap[stage].prob}%</div>
                  </div>
                ));
              })()}
            </div>
            {/* Pipeline by expected close month */}
            {(() => {
              const byMonth: Record<string, typeof sfPipeline> = {};
              filtered.forEach(o => {
                const m = o.closeDate.substring(0, 7);
                if (!byMonth[m]) byMonth[m] = [];
                byMonth[m].push(o);
              });
              return Object.keys(byMonth).sort().map(month => {
                const opps = byMonth[month];
                const monthLabel = new Date(month + '-01').toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
                const monthTotal = opps.reduce((s, o) => s + o.amount, 0);
                const monthWeighted = opps.reduce((s, o) => s + o.weighted, 0);
                return (
                  <div key={month} className="mb-3">
                    <div className="flex items-center justify-between mb-1">
                      <p className="text-xs font-semibold text-gray-600">{monthLabel} <span className="text-gray-400 font-normal">({opps.length} opps)</span></p>
                      <p className="text-xs"><span className="text-gray-400">Total:</span> <span className="font-medium">{fmt(monthTotal)}</span> <span className="text-gray-400 mx-1">|</span> <span className="text-gray-400">Weighted:</span> <span className="font-bold text-blue-700">{fmt(monthWeighted)}</span></p>
                    </div>
                    <table className="w-full text-xs">
                      <thead><tr className="text-left text-gray-400 uppercase border-b">
                        <th className="pb-1 pr-2">Opportunity</th>
                        <th className="pb-1 pr-2">Stage</th>
                        <th className="pb-1 pr-2">Owner</th>
                        <th className="pb-1 pr-2 text-right">Amount</th>
                        <th className="pb-1 pr-2 text-right">Prob</th>
                        <th className="pb-1 pr-2 text-right">Weighted</th>
                      </tr></thead>
                      <tbody>
                        {opps.map((o, idx) => (
                          <tr key={idx} className="border-b border-gray-50 hover:bg-gray-50">
                            <td className="py-1.5 pr-2 text-gray-700 truncate max-w-[250px]">{o.name}</td>
                            <td className="py-1.5 pr-2">
                              <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                o.stage === 'Best Case' ? 'bg-green-100 text-green-700' :
                                o.stage === 'Contract Sent' ? 'bg-blue-100 text-blue-700' :
                                o.stage === 'Negotiation' ? 'bg-amber-100 text-amber-700' :
                                o.stage === 'Test' ? 'bg-purple-100 text-purple-700' :
                                'bg-gray-100 text-gray-600'
                              }`}>{o.stage}</span>
                            </td>
                            <td className="py-1.5 pr-2 text-gray-500 truncate max-w-[120px]">{o.owner}</td>
                            <td className="py-1.5 pr-2 text-right font-medium">{fmt(o.amount)}</td>
                            <td className="py-1.5 pr-2 text-right text-gray-500">{o.probability}%</td>
                            <td className="py-1.5 pr-2 text-right font-bold text-blue-700">{fmt(o.weighted)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                );
              });
            })()}
          </div>
          );
        })()}

        {/* ── Conversion Rate Analysis ── */}
        {activeCompany !== 'consolidated' && sfConversion.yearly.length > 0 && (
          <div className="bg-white rounded-xl shadow-sm border p-5 mt-4">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-base font-bold text-gray-800 flex items-center gap-2">
                <TrendingUp className="w-4 h-4 text-indigo-500" />
                Conversion Rate Analysis
                <span className="text-xs font-normal text-gray-400 ml-2">Snowflake DIM_OPPORTUNITY — historical close-won performance</span>
              </h2>
              <button onClick={() => {
                const headers = ['Year', 'New Clients', 'Upgrades', 'Lost', 'Win Rate %', 'Avg Days', 'New Client Rev (EUR)', 'Upgrade Rev (EUR)'];
                const rows = sfConversion.yearly.map((y: any) => [y.year, y.won, y.wonUpgrades || 0, y.lost, y.winRate, y.avgWonDays || 0, y.wonNewAmt || 0, y.wonUpgradeAmt || 0]);
                const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, ws, 'Conversion Analysis');
                XLSX.writeFile(wb, `Conversion_Rate_Analysis_${new Date().toISOString().slice(0,10)}.xlsx`);
              }} className="text-xs text-emerald-600 hover:text-emerald-800 bg-emerald-50 rounded px-2 py-1 font-medium" title="Download as Excel">
                📥 Excel
              </button>
            </div>

            {/* Yearly Win Rate */}
            <div className="mb-5">
              <p className="text-xs text-gray-400 uppercase mb-2">Historical Win Rate by Year</p>
              <table className="w-full text-xs">
                <thead><tr className="text-left text-gray-400 uppercase border-b">
                  <th className="pb-1 pr-3">Year</th>
                  <th className="pb-1 pr-3 text-right text-green-600">New Clients</th>
                  <th className="pb-1 pr-3 text-right text-teal-600">Upgrades</th>
                  <th className="pb-1 pr-3 text-right text-red-500">Lost</th>
                  <th className="pb-1 pr-3 text-right">Win Rate</th>
                  <th className="pb-1 pr-3 text-right">Avg Days</th>
                  <th className="pb-1 pr-3 text-right text-green-700">New € Won</th>
                  <th className="pb-1 pr-3 text-right text-teal-700">Upgrade € Won</th>
                  <th className="pb-1">Visual</th>
                </tr></thead>
                <tbody>
                  {sfConversion.yearly.map((y: any, i: number) => (
                    <tr key={i} className="border-b border-gray-100">
                      <td className="py-1.5 pr-3 font-medium text-gray-700">{y.year}</td>
                      <td className="py-1.5 pr-3 text-right text-green-600 font-medium cursor-pointer hover:underline hover:bg-green-50 transition-colors" onClick={() => {
                        setWonOppsDrilldown({ year: y.year, type: 'new', data: 'loading' });
                        fetch(`/api/sf-won-opps?year=${y.year}&type=new`).then(r => r.json()).then(j => setWonOppsDrilldown(prev => prev ? { ...prev, data: j.data || [] } : null));
                      }}>{y.won}</td>
                      <td className="py-1.5 pr-3 text-right text-teal-600 font-medium cursor-pointer hover:underline hover:bg-teal-50 transition-colors" onClick={() => {
                        setWonOppsDrilldown({ year: y.year, type: 'upgrades', data: 'loading' });
                        fetch(`/api/sf-won-opps?year=${y.year}&type=upgrades`).then(r => r.json()).then(j => setWonOppsDrilldown(prev => prev ? { ...prev, data: j.data || [] } : null));
                      }}>{y.wonUpgrades || 0}</td>
                      <td className="py-1.5 pr-3 text-right text-red-500 font-medium">{y.lost}</td>
                      <td className="py-1.5 pr-3 text-right font-bold text-blue-700">{y.winRate}%</td>
                      <td className="py-1.5 pr-3 text-right text-gray-600">{y.avgWonDays || '-'} days</td>
                      <td className="py-1.5 pr-3 text-right text-green-700 font-medium">{y.wonNewAmt ? fmt(y.wonNewAmt) : '-'}</td>
                      <td className="py-1.5 pr-3 text-right text-teal-700 font-medium">{y.wonUpgradeAmt ? fmt(y.wonUpgradeAmt) : '-'}</td>
                      <td className="py-1.5">
                        <div className="flex items-center gap-0.5">
                          <div className="w-24 bg-gray-200 rounded-full h-2 overflow-hidden flex">
                            <div className="bg-green-500 h-full" style={{ width: `${y.won && y.lost ? Math.min(y.won * 100 / (y.won + y.lost), 100) : 0}%` }}></div>
                            <div className="bg-teal-400 h-full" style={{ width: `${y.wonUpgrades && y.lost ? Math.min(y.wonUpgrades * 100 / (y.won + y.wonUpgrades + y.lost), 100) : 0}%` }}></div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot><tr className="border-t-2 border-gray-300 font-bold">
                  <td className="py-1.5 pr-3">Average (recent 3yr)</td>
                  <td className="py-1.5 pr-3 text-right text-green-600">{Math.round(sfConversion.yearly.filter((y: any) => y.year >= 2023).reduce((s: number, y: any) => s + y.won, 0) / Math.max(sfConversion.yearly.filter((y: any) => y.year >= 2023).length, 1))}</td>
                  <td className="py-1.5 pr-3 text-right text-teal-600">{Math.round(sfConversion.yearly.filter((y: any) => y.year >= 2023).reduce((s: number, y: any) => s + (y.wonUpgrades || 0), 0) / Math.max(sfConversion.yearly.filter((y: any) => y.year >= 2023).length, 1))}</td>
                  <td className="py-1.5 pr-3 text-right text-red-500">{Math.round(sfConversion.yearly.filter((y: any) => y.year >= 2023).reduce((s: number, y: any) => s + y.lost, 0) / Math.max(sfConversion.yearly.filter((y: any) => y.year >= 2023).length, 1))}</td>
                  <td className="py-1.5 pr-3 text-right text-blue-700">{(() => { const recent = sfConversion.yearly.filter((y: any) => y.year >= 2023); return recent.length > 0 ? Math.round(recent.reduce((s: number, y: any) => s + y.winRate, 0) / recent.length) : '-'; })()}%</td>
                  <td className="py-1.5 pr-3 text-right text-gray-600">{(() => { const recent = sfConversion.yearly.filter((y: any) => y.year >= 2023 && y.avgWonDays); return recent.length > 0 ? Math.round(recent.reduce((s: number, y: any) => s + (y.avgWonDays || 0), 0) / recent.length) : '-'; })()} days</td>
                  <td className="py-1.5 pr-3 text-right text-green-700">{(() => { const recent = sfConversion.yearly.filter((y: any) => y.year >= 2023 && y.wonNewAmt); return recent.length > 0 ? fmt(Math.round(recent.reduce((s: number, y: any) => s + (y.wonNewAmt || 0), 0) / recent.length)) : '-'; })()}</td>
                  <td className="py-1.5 pr-3 text-right text-teal-700">{(() => { const recent = sfConversion.yearly.filter((y: any) => y.year >= 2023 && y.wonUpgradeAmt); return recent.length > 0 ? fmt(Math.round(recent.reduce((s: number, y: any) => s + (y.wonUpgradeAmt || 0), 0) / recent.length)) : '-'; })()}</td>
                  <td></td>
                </tr></tfoot>
              </table>
              <p className="text-[10px] text-gray-400 mt-2">Used in Pipeline column: {cashflowForecast[0]?.pipelineHistWinRate || '-'}% win rate, +{cashflowForecast[0]?.pipelineDelayMonths || '-'}m delay (based on avg {(() => { const recent = sfConversion.yearly.filter(y => y.year >= 2023 && y.avgWonDays); return recent.length > 0 ? Math.round(recent.reduce((s, y) => s + (y.avgWonDays || 0), 0) / recent.length) : '-'; })()} days to close)</p>

              {/* Won Opportunities Drilldown */}
              {wonOppsDrilldown && (
                <div className="mt-3 bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
                  <div className="flex items-center justify-between px-4 py-2.5 border-b bg-white">
                    <h4 className="font-semibold text-sm text-gray-800">
                      {wonOppsDrilldown.type === 'new' ? '🆕 New Clients' : '⬆️ Upgrades'} — {wonOppsDrilldown.year}
                    </h4>
                    <div className="flex items-center gap-2">
                      {wonOppsDrilldown.data !== 'loading' && Array.isArray(wonOppsDrilldown.data) && wonOppsDrilldown.data.length > 0 && (
                        <button onClick={() => {
                          const data = wonOppsDrilldown.data as any[];
                          const isUpg = wonOppsDrilldown.type === 'upgrades';
                          const headers = ['#', 'Client / Opportunity', 'Customer ID', ...(isUpg ? ['Before (EUR)', 'After (EUR)', 'Change'] : ['Amount (EUR)']), 'Created', 'Closed Won', 'Days', 'Owner'];
                          const rows = data.map((o: any, i: number) => [
                            i + 1, o.name, o.customer,
                            ...(isUpg ? [o.prevAmount || 0, o.amount || 0, (o.amount || 0) - (o.prevAmount || 0)] : [o.amount || 0]),
                            o.createdDate, o.closedDate, o.daysToClose || 0, o.owner
                          ]);
                          const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
                          const wb = XLSX.utils.book_new();
                          XLSX.utils.book_append_sheet(wb, ws, wonOppsDrilldown.type === 'new' ? 'New Clients' : 'Upgrades');
                          XLSX.writeFile(wb, `${wonOppsDrilldown.type === 'new' ? 'New_Clients' : 'Upgrades'}_${wonOppsDrilldown.year}.xlsx`);
                        }} className="text-xs text-emerald-600 hover:text-emerald-800 bg-emerald-50 rounded px-2 py-1 font-medium" title="Download as Excel">
                          📥 Excel
                        </button>
                      )}
                      <button onClick={() => setWonOppsDrilldown(null)} className="text-gray-400 hover:text-gray-600 text-lg leading-none">&times;</button>
                    </div>
                  </div>
                  <div className="p-4 max-h-[400px] overflow-auto">
                    {wonOppsDrilldown.data === 'loading' ? (
                      <div className="flex items-center gap-2 py-6 justify-center text-gray-400"><Loader2 className="w-4 h-4 animate-spin" /> Loading from Snowflake...</div>
                    ) : wonOppsDrilldown.data.length === 0 ? (
                      <p className="text-gray-400 text-center py-4">No opportunities found</p>
                    ) : (() => {
                        const isUpgradeView = wonOppsDrilldown.type === 'upgrades';
                        const data = wonOppsDrilldown.data as any[];
                        return (
                          <table className="w-full text-xs">
                            <thead><tr className="text-left text-gray-400 uppercase border-b">
                              <th className="pb-1 pr-2">#</th>
                              <th className="pb-1 pr-2">Client / Opportunity</th>
                              {isUpgradeView && <th className="pb-1 pr-2 text-right text-gray-500">Before (€)</th>}
                              <th className="pb-1 pr-2 text-right">{isUpgradeView ? 'After (€)' : 'Amount (€)'}</th>
                              {isUpgradeView && <th className="pb-1 pr-2 text-right text-green-600">Change</th>}
                              <th className="pb-1 pr-2 text-right">Created</th>
                              <th className="pb-1 pr-2 text-right">Closed Won</th>
                              <th className="pb-1 pr-2 text-right">Days</th>
                              <th className="pb-1 pr-2">Owner</th>
                            </tr></thead>
                            <tbody>
                              {data.map((o: any, oi: number) => {
                                const change = isUpgradeView && o.prevAmount != null ? o.amount - o.prevAmount : null;
                                return (
                                  <tr key={oi} className="border-b border-gray-100 hover:bg-white transition-colors">
                                    <td className="py-1.5 pr-2 text-gray-400">{oi + 1}</td>
                                    <td className="py-1.5 pr-2">
                                      <div className="text-gray-800 font-medium max-w-[250px] truncate">{o.name}</div>
                                      <div className="text-[10px] text-gray-400">{o.customer}</div>
                                    </td>
                                    {isUpgradeView && (
                                      <td className="py-1.5 pr-2 text-right text-gray-500">{o.prevAmount != null ? fmt(o.prevAmount) : <span className="text-gray-300">-</span>}</td>
                                    )}
                                    <td className={`py-1.5 pr-2 text-right font-medium ${isUpgradeView ? 'text-teal-700' : 'text-green-700'}`}>{fmt(o.amount)}</td>
                                    {isUpgradeView && (
                                      <td className={`py-1.5 pr-2 text-right font-medium ${change != null ? (change >= 0 ? 'text-green-600' : 'text-red-500') : 'text-gray-300'}`}>
                                        {change != null ? `${change >= 0 ? '+' : ''}${fmt(change)}` : '-'}
                                      </td>
                                    )}
                                    <td className="py-1.5 pr-2 text-right text-gray-500">{o.createdDate}</td>
                                    <td className="py-1.5 pr-2 text-right text-gray-700 font-medium">{o.closedDate}</td>
                                    <td className="py-1.5 pr-2 text-right text-gray-500">{o.daysToClose > 0 ? `${o.daysToClose}d` : '-'}</td>
                                    <td className="py-1.5 pr-2 text-gray-500 max-w-[100px] truncate">{o.owner}</td>
                                  </tr>
                                );
                              })}
                            </tbody>
                            <tfoot><tr className="border-t-2 border-gray-300 font-bold">
                              <td className="py-1.5 pr-2" colSpan={2}>Total ({data.length} {wonOppsDrilldown.type === 'new' ? 'new clients' : 'upgrades'})</td>
                              {isUpgradeView && <td className="py-1.5 pr-2 text-right text-gray-500">{fmt(data.filter(o => o.prevAmount != null).reduce((s: number, o: any) => s + (o.prevAmount || 0), 0))}</td>}
                              <td className={`py-1.5 pr-2 text-right ${isUpgradeView ? 'text-teal-700' : 'text-green-700'}`}>{fmt(data.reduce((s: number, o: any) => s + (o.amount || 0), 0))}</td>
                              {isUpgradeView && <td className="py-1.5 pr-2 text-right text-green-600">{fmt(data.reduce((s: number, o: any) => s + (o.amount || 0), 0) - data.filter(o => o.prevAmount != null).reduce((s: number, o: any) => s + (o.prevAmount || 0), 0))}</td>}
                              <td colSpan={4}></td>
                            </tr></tfoot>
                          </table>
                        );
                      })()}
                  </div>
                </div>
              )}
            </div>

            {/* Stage Summary */}
            {sfConversion.stages.length > 0 && (
              <div className="mb-5">
                <p className="text-xs text-gray-400 uppercase mb-2">Open Pipeline by Stage</p>
                <table className="w-full text-xs">
                  <thead><tr className="text-left text-gray-400 uppercase border-b">
                    <th className="pb-1 pr-3">Stage</th><th className="pb-1 pr-3 text-right">Count</th><th className="pb-1 pr-3 text-right">Total Amount</th><th className="pb-1 pr-3 text-right">Avg Prob</th><th className="pb-1 pr-3 text-right">Avg Age (days)</th>
                  </tr></thead>
                  <tbody>
                    {sfConversion.stages.map((s: any, i: number) => (
                      <tr key={i} className="border-b border-gray-100">
                        <td className="py-1.5 pr-3 font-medium text-gray-700">{s.stage}</td>
                        <td className="py-1.5 pr-3 text-right text-gray-600">{s.count}</td>
                        <td className="py-1.5 pr-3 text-right font-medium text-violet-700">{fmt(s.totalAmt)}</td>
                        <td className="py-1.5 pr-3 text-right text-blue-600">{s.avgProb}%</td>
                        <td className="py-1.5 pr-3 text-right text-gray-500">{s.avgAgeDays || '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Weighted Projection by Month */}
            {sfConversion.projection.length > 0 && (
              <div>
                <p className="text-xs text-gray-400 uppercase mb-2">Weighted Pipeline Projection by Close Month</p>
                <table className="w-full text-xs">
                  <thead><tr className="text-left text-gray-400 uppercase border-b">
                    <th className="pb-1 pr-3">Month</th><th className="pb-1 pr-3 text-right">Opps</th><th className="pb-1 pr-3 text-right">Total Amount</th><th className="pb-1 pr-3 text-right">Avg Prob</th><th className="pb-1 pr-3 text-right">Weighted Amount</th>
                  </tr></thead>
                  <tbody>
                    {sfConversion.projection.map((p: any, i: number) => (
                      <tr key={i} className="border-b border-gray-100">
                        <td className="py-1.5 pr-3 font-medium text-gray-700">{new Date(p.month + '-01').toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })}</td>
                        <td className="py-1.5 pr-3 text-right text-gray-600">{p.count}</td>
                        <td className="py-1.5 pr-3 text-right font-medium text-gray-700">{fmt(p.totalAmt)}</td>
                        <td className="py-1.5 pr-3 text-right text-blue-600">{p.avgProb}%</td>
                        <td className="py-1.5 pr-3 text-right font-bold text-teal-700">{fmt(p.weightedAmt)}</td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot><tr className="border-t-2 border-gray-300 font-bold">
                    <td className="py-1.5 pr-3">Total</td>
                    <td className="py-1.5 pr-3 text-right">{sfConversion.projection.reduce((s: number, p: any) => s + p.count, 0)}</td>
                    <td className="py-1.5 pr-3 text-right">{fmt(sfConversion.projection.reduce((s: number, p: any) => s + p.totalAmt, 0))}</td>
                    <td></td>
                    <td className="py-1.5 pr-3 text-right text-teal-700">{fmt(sfConversion.projection.reduce((s: number, p: any) => s + p.weightedAmt, 0))}</td>
                  </tr></tfoot>
                </table>
              </div>
            )}
          </div>
        )}

        {/* ── Churn Rate Analysis ── */}
        {activeCompany !== 'consolidated' && companyConfig.hasSF && churnData.length > 0 && (
          <div className="bg-white rounded-xl shadow-sm border p-5 mt-4">
            <h2 className="text-base font-bold text-gray-800 flex items-center gap-2 mb-4">
              <TrendingUp className="w-4 h-4 text-red-500" />
              Churn Rate Analysis
              <span className="text-xs font-normal text-gray-400 ml-2">Snowflake FCT_CUSTOMER__MONTHLY — revenue lost from churned customers</span>
            </h2>
            <table className="w-full text-xs">
              <thead><tr className="text-left text-gray-400 uppercase border-b">
                <th className="pb-1 pr-3">Year</th>
                <th className="pb-1 pr-3 text-right">Active Clients</th>
                <th className="pb-1 pr-3 text-right text-red-500">Churned</th>
                <th className="pb-1 pr-3 text-right">Client Churn %</th>
                <th className="pb-1 pr-3 text-right text-red-600">Lost Revenue</th>
                <th className="pb-1 pr-3 text-right">Rev Churn %</th>
                <th className="pb-1 pr-3 text-right text-orange-600">Monthly Impact</th>
                <th className="pb-1">Visual</th>
              </tr></thead>
              <tbody>
                {churnData.map((c, i) => {
                  const maxLost = Math.max(...churnData.map(d => d.lostRevenue));
                  return (
                    <tr key={i} className="border-b border-gray-100">
                      <td className="py-1.5 pr-3 font-medium text-gray-700">{c.year}{c.monthsCount < 12 ? <span className="text-gray-400 text-[10px] ml-1">({c.monthsCount}m)</span> : ''}</td>
                      <td className="py-1.5 pr-3 text-right text-gray-600">{c.totalCustomers}</td>
                      <td className="py-1.5 pr-3 text-right text-red-500 font-medium cursor-pointer hover:underline hover:bg-red-50 transition-colors" onClick={() => {
                        if (churnDrilldown?.year === c.year && churnDrilldown.data !== 'loading') { setChurnDrilldown(null); return; }
                        setChurnDrilldown({ year: c.year, data: 'loading' });
                        fetch(`/api/sf-churn-drilldown?year=${c.year}`).then(r => r.json()).then(j => setChurnDrilldown({ year: c.year, data: j.data || [] })).catch(() => setChurnDrilldown({ year: c.year, data: [] }));
                      }}>{c.churnedClients}</td>
                      <td className="py-1.5 pr-3 text-right font-bold text-red-700">{c.clientChurnPct}%</td>
                      <td className="py-1.5 pr-3 text-right text-red-600 font-medium">{fmt(c.lostRevenue)}</td>
                      <td className="py-1.5 pr-3 text-right font-bold text-orange-700">{c.churnPct}%</td>
                      <td className="py-1.5 pr-3 text-right text-orange-600 font-medium">{fmt(c.monthlyImpact)}<span className="text-gray-400">/mo</span></td>
                      <td className="py-1.5">
                        <div className="w-24 bg-gray-200 rounded-full h-2 overflow-hidden">
                          <div className="bg-red-400 h-full rounded-full" style={{ width: `${maxLost > 0 ? Math.min(c.lostRevenue / maxLost * 100, 100) : 0}%` }}></div>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
              <tfoot><tr className="border-t-2 border-gray-300 font-bold">
                <td className="py-1.5 pr-3">Average (recent 3yr)</td>
                <td className="py-1.5 pr-3 text-right text-gray-600">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? Math.round(r.reduce((s, c) => s + c.totalCustomers, 0) / r.length) : '-'; })()}</td>
                <td className="py-1.5 pr-3 text-right text-red-500">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? Math.round(r.reduce((s, c) => s + c.churnedClients, 0) / r.length) : '-'; })()}</td>
                <td className="py-1.5 pr-3 text-right text-red-700">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? (Math.round(r.reduce((s, c) => s + c.clientChurnPct, 0) / r.length * 10) / 10) : '-'; })()}%</td>
                <td className="py-1.5 pr-3 text-right text-red-600">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? fmt(Math.round(r.reduce((s, c) => s + c.lostRevenue, 0) / r.length)) : '-'; })()}</td>
                <td className="py-1.5 pr-3 text-right text-orange-700">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? (Math.round(r.reduce((s, c) => s + c.churnPct, 0) / r.length * 10) / 10) : '-'; })()}%</td>
                <td className="py-1.5 pr-3 text-right text-orange-600">{(() => { const r = churnData.filter(c => c.year >= 2023 && c.year <= 2025); return r.length > 0 ? fmt(Math.round(r.reduce((s, c) => s + c.monthlyImpact, 0) / r.length)) : '-'; })()}<span className="text-gray-400">/mo</span></td>
                <td></td>
              </tr></tfoot>
            </table>
            {/* Churn Drilldown */}
            {churnDrilldown && (
              <div className="mt-3 bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between px-4 py-2.5 border-b bg-white">
                  <h4 className="font-semibold text-sm text-gray-800">
                    📉 Churned Customers — {churnDrilldown.year}
                  </h4>
                  <button onClick={() => setChurnDrilldown(null)} className="text-gray-400 hover:text-gray-600 text-lg leading-none">&times;</button>
                </div>
                <div className="p-4 max-h-[500px] overflow-auto">
                  {churnDrilldown.data === 'loading' ? (
                    <div className="flex items-center gap-2 py-6 justify-center text-gray-400"><Loader2 className="w-4 h-4 animate-spin" /> Loading churned customers...</div>
                  ) : churnDrilldown.data.length === 0 ? (
                    <p className="text-gray-400 text-center py-4">No churned customers found</p>
                  ) : (() => {
                    const data = churnDrilldown.data as any[];
                    return (
                      <table className="w-full text-xs">
                        <thead><tr className="text-left text-gray-400 uppercase border-b">
                          <th className="pb-1 pr-2">#</th>
                          <th className="pb-1 pr-2">Customer</th>
                          <th className="pb-1 pr-2 text-right">Last Mo Rev</th>
                          <th className="pb-1 pr-2 text-right">12m Rev</th>
                          <th className="pb-1 pr-2 text-right">Avg/Mo</th>
                          <th className="pb-1 pr-2">Churn Date</th>
                          <th className="pb-1 pr-2">Tier</th>
                          <th className="pb-1 pr-2">Region</th>
                          <th className="pb-1 pr-2">Owner</th>
                          <th className="pb-1 text-center">NS</th>
                        </tr></thead>
                        <tbody>
                          {data.map((c: any, ci: number) => {
                            const searchTerm = (c.name || '').split(/\s*[-–(]/)[0].trim().split(/\s+/).slice(0, 2).join(' ');
                            const nsUrl = nsAccountId && searchTerm ? `https://${nsAccountId}.app.netsuite.com/app/common/search/ubersearchresults.nl?quicksearch=T&searchtype=Uber&frame=be&Uber_NAMEtype=KEYWORDSTARTSWITH&Uber_NAME=${encodeURIComponent(searchTerm)}` : '';
                            return (
                              <tr key={ci} className="border-b border-gray-100 hover:bg-white transition-colors">
                                <td className="py-1.5 pr-2 text-gray-400">{ci + 1}</td>
                                <td className="py-1.5 pr-2 font-medium text-gray-800 max-w-[200px] truncate">{c.name}</td>
                                <td className="py-1.5 pr-2 text-right text-red-600 font-medium">{fmt(c.lastMonthRev)}</td>
                                <td className="py-1.5 pr-2 text-right text-red-500">{fmt(c.total12mRev)}</td>
                                <td className="py-1.5 pr-2 text-right text-gray-600">{fmt(c.avgMonthlyRev)}</td>
                                <td className="py-1.5 pr-2 text-gray-500">{c.churnDate}</td>
                                <td className="py-1.5 pr-2 text-gray-500">{c.tier !== '-' ? c.tier : ''}</td>
                                <td className="py-1.5 pr-2 text-gray-500">{c.region !== '-' ? c.region : ''}</td>
                                <td className="py-1.5 pr-2 text-gray-500 max-w-[80px] truncate">{c.owner !== '-' ? c.owner : ''}</td>
                                <td className="py-1.5 text-center">{nsUrl ? <a href={nsUrl} target="_blank" rel="noreferrer" className="text-violet-500 hover:text-violet-700 font-bold text-[10px]" onClick={(e) => e.stopPropagation()}>NS</a> : null}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                        <tfoot><tr className="border-t-2 border-gray-300 font-bold">
                          <td className="py-1.5 pr-2" colSpan={2}>Total ({data.length} customers)</td>
                          <td className="py-1.5 pr-2 text-right text-red-700">{fmt(data.reduce((s: number, c: any) => s + (c.lastMonthRev || 0), 0))}</td>
                          <td className="py-1.5 pr-2 text-right text-red-600">{fmt(data.reduce((s: number, c: any) => s + (c.total12mRev || 0), 0))}</td>
                          <td className="py-1.5 pr-2 text-right text-gray-600">{fmt(Math.round(data.reduce((s: number, c: any) => s + (c.avgMonthlyRev || 0), 0)))}</td>
                          <td colSpan={5}></td>
                        </tr></tfoot>
                      </table>
                    );
                  })()}
                </div>
              </div>
            )}

            <p className="text-[10px] text-gray-400 mt-2">Lost Revenue = sum of last 12 months' revenue before churn. Monthly Impact = actual last-month revenue run-rate of churned customers (most accurate for forecasting). Click churned count to see individual customers.</p>
          </div>
        )}

        {/* ── Scenario Comparison Panel ── */}
        {activeCompany !== 'consolidated' && showComparePanel && scenarios.length >= 1 && (() => {
          const baselineData: ScenarioData = { salaryAdjPctByMonth: {}, collPctByMonth: {}, salaryDeptAdj: {}, leverOverrides: {}, pipelineMinProb: 100 };
          const baselineScenario: Scenario = { id: '__baseline__', name: 'Baseline', createdAt: '', updatedAt: '', data: baselineData };
          const leftScenario = compareLeftId === '__baseline__' ? baselineScenario : scenarios.find(s => s.id === compareLeftId);
          const rightScenario = compareRightId === '__baseline__' ? baselineScenario : scenarios.find(s => s.id === compareRightId);
          const leftCF = leftScenario ? computeScenarioCashflow(leftScenario.data) : null;
          const rightCF = rightScenario ? computeScenarioCashflow(rightScenario.data) : null;
          const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
          const now = new Date();
          return (
            <div className="fixed inset-0 bg-black/40 z-[100] flex items-center justify-center p-4" onClick={() => setShowComparePanel(false)}>
              <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-y-auto" onClick={e => e.stopPropagation()}>
                <div className="sticky top-0 bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between rounded-t-2xl z-10">
                  <h2 className="text-sm font-bold text-gray-800 flex items-center gap-2">⇄ Scenario Comparison</h2>
                  <div className="flex items-center gap-2">
                    {leftCF && rightCF && leftScenario && rightScenario && (
                      <button
                        onClick={() => {
                          const mn = ['January','February','March','April','May','June','July','August','September','October','November','December'];
                          const yr = now.getFullYear();
                          const data = [
                            ['Month', `Salary (${leftScenario.name})`, `Salary (${rightScenario.name})`, `Inflows (${leftScenario.name})`, `Inflows (${rightScenario.name})`, `Total Outflow (${leftScenario.name})`, `Total Outflow (${rightScenario.name})`, `Net (${leftScenario.name})`, `Net (${rightScenario.name})`, `Closing (${leftScenario.name})`, `Closing (${rightScenario.name})`, 'Delta'],
                            ...leftCF.map((l, i) => {
                              const r = rightCF[i];
                              return [`${mn[i]} ${yr}`, l.salary, r.salary, l.collections, r.collections, l.totalOutflow, r.totalOutflow, l.net, r.net, l.closingBalance, r.closingBalance, r.closingBalance - l.closingBalance];
                            }),
                            ['Year Total', leftCF.reduce((s,r)=>s+r.salary,0), rightCF.reduce((s,r)=>s+r.salary,0), leftCF.reduce((s,r)=>s+r.collections,0), rightCF.reduce((s,r)=>s+r.collections,0), leftCF.reduce((s,r)=>s+r.totalOutflow,0), rightCF.reduce((s,r)=>s+r.totalOutflow,0), leftCF.reduce((s,r)=>s+r.net,0), rightCF.reduce((s,r)=>s+r.net,0), leftCF[11]?.closingBalance||0, rightCF[11]?.closingBalance||0, (rightCF[11]?.closingBalance||0)-(leftCF[11]?.closingBalance||0)],
                          ];
                          const ws = XLSX.utils.aoa_to_sheet(data);
                          ws['!cols'] = [{ wch: 18 }, ...Array(11).fill({ wch: 18 })];
                          for (let r = 1; r < data.length; r++) { for (let c = 1; c <= 11; c++) { const cell = ws[XLSX.utils.encode_cell({ r, c })]; if (cell && typeof cell.v === 'number') cell.z = '#,##0'; } }
                          const wb = XLSX.utils.book_new();
                          XLSX.utils.book_append_sheet(wb, ws, 'Comparison');
                          XLSX.writeFile(wb, `Scenario_Comparison_${leftScenario.name}_vs_${rightScenario.name}_${new Date().toISOString().slice(0,10)}.xlsx`);
                        }}
                        className="text-xs text-green-700 bg-green-50 border border-green-200 rounded-lg px-3 py-1.5 hover:bg-green-100 transition-colors flex items-center gap-1"
                      >
                        <span className="text-sm">📥</span> Download Excel
                      </button>
                    )}
                    <button onClick={() => setShowComparePanel(false)} className="text-gray-400 hover:text-gray-700 text-lg">✕</button>
                  </div>
                </div>
                <div className="px-6 py-4">
                  {/* Scenario selectors */}
                  <div className="flex items-center gap-4 mb-4">
                    <div className="flex-1">
                      <label className="text-[10px] text-gray-400 uppercase font-medium mb-1 block">Scenario A</label>
                      <select value={compareLeftId || ''} onChange={e => setCompareLeftId(e.target.value || null)}
                              className="w-full text-xs border border-violet-200 rounded-lg px-3 py-2 bg-violet-50 text-violet-700 font-medium focus:outline-none focus:ring-2 focus:ring-violet-300">
                        <option value="__baseline__">Baseline (no adjustments)</option>
                        {companyScenarios.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
                      </select>
                    </div>
                    <span className="text-gray-400 text-lg mt-4">⇄</span>
                    <div className="flex-1">
                      <label className="text-[10px] text-gray-400 uppercase font-medium mb-1 block">Scenario B</label>
                      <select value={compareRightId || ''} onChange={e => setCompareRightId(e.target.value || null)}
                              className="w-full text-xs border border-blue-200 rounded-lg px-3 py-2 bg-blue-50 text-blue-700 font-medium focus:outline-none focus:ring-2 focus:ring-blue-300">
                        <option value="__baseline__">Baseline (no adjustments)</option>
                        {companyScenarios.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
                      </select>
                    </div>
                  </div>
                  {/* Adjustment differences with EUR impact */}
                  {leftScenario && rightScenario && leftCF && rightCF && (() => {
                    const diff = describeScenarioDiff(leftScenario.data, rightScenario.data, `${leftScenario.name} → ${rightScenario.name}`);
                    // Compute per-month deltas for salary, collections, net, closing
                    const monthDeltas = leftCF.map((l, i) => ({
                      salary: rightCF[i].salary - l.salary,
                      collections: rightCF[i].collections - l.collections,
                      net: rightCF[i].net - l.net,
                      closing: rightCF[i].closingBalance - l.closingBalance,
                    }));
                    const totalSalaryDelta = monthDeltas.reduce((s, d) => s + d.salary, 0);
                    const totalCollDelta = monthDeltas.reduce((s, d) => s + d.collections, 0);
                    const totalNetDelta = monthDeltas.reduce((s, d) => s + d.net, 0);
                    const endClosingDelta = monthDeltas[11]?.closing || 0;
                    return diff.items.length > 0 ? (
                      <div className="bg-gray-50 rounded-lg p-3 mb-4">
                        <p className="text-[10px] text-gray-400 uppercase font-medium mb-2">Adjustment Differences</p>
                        <div className="flex flex-wrap gap-x-4 gap-y-1">
                          {diff.items.map(item => <span key={item.key} className={`text-[10px] ${item.color}`}>• {item.desc}</span>)}
                        </div>
                        {/* EUR impact summary */}
                        <div className="flex flex-wrap gap-x-6 gap-y-1 mt-3 pt-2 border-t border-gray-200">
                          <div className="text-[10px]">
                            <span className="text-gray-400">Salary impact: </span>
                            <span className={`font-bold ${totalSalaryDelta > 0 ? 'text-red-600' : totalSalaryDelta < 0 ? 'text-green-600' : 'text-gray-400'}`}>
                              {totalSalaryDelta === 0 ? '-' : `${totalSalaryDelta > 0 ? '+' : ''}${fmt(totalSalaryDelta)}`}
                            </span>
                          </div>
                          <div className="text-[10px]">
                            <span className="text-gray-400">Inflows impact: </span>
                            <span className={`font-bold ${totalCollDelta > 0 ? 'text-green-600' : totalCollDelta < 0 ? 'text-red-600' : 'text-gray-400'}`}>
                              {totalCollDelta === 0 ? '-' : `${totalCollDelta > 0 ? '+' : ''}${fmt(totalCollDelta)}`}
                            </span>
                          </div>
                          <div className="text-[10px]">
                            <span className="text-gray-400">Net impact: </span>
                            <span className={`font-bold ${totalNetDelta > 0 ? 'text-green-600' : totalNetDelta < 0 ? 'text-red-600' : 'text-gray-400'}`}>
                              {totalNetDelta === 0 ? '-' : `${totalNetDelta > 0 ? '+' : ''}${fmt(totalNetDelta)}`}
                            </span>
                          </div>
                          <div className="text-[10px]">
                            <span className="text-gray-400">Dec closing: </span>
                            <span className={`font-bold ${endClosingDelta > 0 ? 'text-green-700' : endClosingDelta < 0 ? 'text-red-600' : 'text-gray-400'}`}>
                              {endClosingDelta === 0 ? '-' : `${endClosingDelta > 0 ? '+' : ''}${fmt(endClosingDelta)}`}
                            </span>
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div className="bg-green-50 rounded-lg p-3 mb-4 text-center">
                        <p className="text-[10px] text-green-600">Both scenarios have identical adjustments</p>
                      </div>
                    );
                  })()}
                  {/* Side-by-side cashflow comparison table */}
                  {leftCF && rightCF && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b-2 border-gray-200 text-gray-400 uppercase">
                            <th className="pb-2 pr-3 text-left">Month</th>
                            <th className="pb-2 pr-2 text-right text-violet-500" colSpan={1}>Salary A</th>
                            <th className="pb-2 pr-2 text-right text-blue-500" colSpan={1}>Salary B</th>
                            <th className="pb-2 pr-2 text-right text-violet-500">Inflows A</th>
                            <th className="pb-2 pr-2 text-right text-blue-500">Inflows B</th>
                            <th className="pb-2 pr-2 text-right text-violet-500">Net A</th>
                            <th className="pb-2 pr-2 text-right text-blue-500">Net B</th>
                            <th className="pb-2 pr-2 text-right text-violet-600 font-bold">Close A</th>
                            <th className="pb-2 pr-2 text-right text-blue-600 font-bold">Close B</th>
                            <th className="pb-2 pr-2 text-right">Delta</th>
                          </tr>
                        </thead>
                        <tbody>
                          {leftCF.map((l, i) => {
                            const r = rightCF[i];
                            const delta = r.closingBalance - l.closingBalance;
                            const isPast = i < now.getMonth();
                            const isCur = i === now.getMonth();
                            return (
                              <tr key={i} className={`border-b border-gray-100 ${isCur ? 'bg-blue-50/30' : isPast ? 'bg-gray-50/50' : ''}`}>
                                <td className="py-1.5 pr-3 font-medium text-gray-700 whitespace-nowrap">
                                  {monthNames[i]} {now.getFullYear()}
                                  {isPast ? <span className="ml-1 text-[8px] bg-green-100 text-green-600 px-1 rounded-full">ACT</span>
                                    : isCur ? <span className="ml-1 text-[8px] bg-amber-100 text-amber-600 px-1 rounded-full">CUR</span>
                                    : <span className="ml-1 text-[8px] bg-violet-100 text-violet-600 px-1 rounded-full">PRJ</span>}
                                </td>
                                <td className="py-1.5 pr-2 text-right text-amber-600">-{fmt(l.salary)}</td>
                                <td className={`py-1.5 pr-2 text-right ${l.salary !== r.salary ? 'text-amber-700 font-bold' : 'text-amber-600'}`}>-{fmt(r.salary)}</td>
                                <td className="py-1.5 pr-2 text-right text-green-600">{fmt(l.collections)}</td>
                                <td className={`py-1.5 pr-2 text-right ${l.collections !== r.collections ? 'text-green-700 font-bold' : 'text-green-600'}`}>{fmt(r.collections)}</td>
                                <td className={`py-1.5 pr-2 text-right ${l.net >= 0 ? 'text-green-600' : 'text-red-500'}`}>{fmt(l.net)}</td>
                                <td className={`py-1.5 pr-2 text-right font-medium ${r.net >= 0 ? 'text-green-700' : 'text-red-600'}`}>{fmt(r.net)}</td>
                                <td className={`py-1.5 pr-2 text-right font-bold ${l.closingBalance >= 0 ? 'text-violet-700' : 'text-red-600'}`}>{fmt(l.closingBalance)}</td>
                                <td className={`py-1.5 pr-2 text-right font-bold ${r.closingBalance >= 0 ? 'text-blue-700' : 'text-red-600'}`}>{fmt(r.closingBalance)}</td>
                                <td className={`py-1.5 pr-2 text-right font-bold ${Math.abs(delta) < 100 ? 'text-gray-300' : delta > 0 ? 'text-green-600' : 'text-red-500'}`}>
                                  {Math.abs(delta) < 100 ? '-' : `${delta > 0 ? '+' : ''}${fmt(delta)}`}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                        <tfoot>
                          <tr className="border-t-2 border-gray-300 font-bold bg-gray-50">
                            <td className="py-2 pr-3">Year Total</td>
                            <td className="py-2 pr-2 text-right text-amber-700">-{fmt(leftCF.reduce((s, r) => s + r.salary, 0))}</td>
                            <td className="py-2 pr-2 text-right text-amber-700">-{fmt(rightCF.reduce((s, r) => s + r.salary, 0))}</td>
                            <td className="py-2 pr-2 text-right text-green-700">{fmt(leftCF.reduce((s, r) => s + r.collections, 0))}</td>
                            <td className="py-2 pr-2 text-right text-green-700">{fmt(rightCF.reduce((s, r) => s + r.collections, 0))}</td>
                            <td className="py-2 pr-2 text-right">{fmt(leftCF.reduce((s, r) => s + r.net, 0))}</td>
                            <td className="py-2 pr-2 text-right">{fmt(rightCF.reduce((s, r) => s + r.net, 0))}</td>
                            <td className="py-2 pr-2 text-right text-violet-800">{fmt(leftCF[11]?.closingBalance || 0)}</td>
                            <td className="py-2 pr-2 text-right text-blue-800">{fmt(rightCF[11]?.closingBalance || 0)}</td>
                            <td className={`py-2 pr-2 text-right ${((rightCF[11]?.closingBalance || 0) - (leftCF[11]?.closingBalance || 0)) >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                              {((rightCF[11]?.closingBalance || 0) - (leftCF[11]?.closingBalance || 0)) >= 0 ? '+' : ''}{fmt((rightCF[11]?.closingBalance || 0) - (leftCF[11]?.closingBalance || 0))}
                            </td>
                          </tr>
                        </tfoot>
                      </table>
                    </div>
                  )}
                  {/* ── Comparison Charts ── */}
                  {leftCF && rightCF && leftScenario && rightScenario && (() => {
                    const chartData = leftCF.map((l, i) => ({
                      name: monthNames[i],
                      inflowsA: Math.round(l.collections / 1000),
                      inflowsB: Math.round(rightCF[i].collections / 1000),
                      outflowsA: Math.round(l.totalOutflow / 1000),
                      outflowsB: Math.round(rightCF[i].totalOutflow / 1000),
                      netA: Math.round(l.net / 1000),
                      netB: Math.round(rightCF[i].net / 1000),
                      closeA: Math.round(l.closingBalance / 1000),
                      closeB: Math.round(rightCF[i].closingBalance / 1000),
                    }));
                    return (
                      <div className="mt-6 space-y-6">
                        {/* Inflows & Outflows comparison */}
                        <div>
                          <p className="text-xs font-medium text-gray-600 mb-2">Inflows & Outflows (€K) — <span className="text-violet-600">{leftScenario.name}</span> vs <span className="text-blue-600">{rightScenario.name}</span></p>
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={chartData} margin={{ top: 15, right: 10, left: 10, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                              <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `${v}K`} />
                              <Tooltip formatter={(v: number, name: string) => [`€${v.toLocaleString()}K`, name]} />
                              <Bar dataKey="inflowsA" name={`Inflows (${leftScenario.name})`} fill="#a78bfa" opacity={0.5} radius={[2,2,0,0]} />
                              <Bar dataKey="inflowsB" name={`Inflows (${rightScenario.name})`} fill="#60a5fa" opacity={0.5} radius={[2,2,0,0]} />
                              <Bar dataKey="outflowsA" name={`Outflows (${leftScenario.name})`} fill="#f9a8d4" opacity={0.4} radius={[2,2,0,0]} />
                              <Bar dataKey="outflowsB" name={`Outflows (${rightScenario.name})`} fill="#fca5a5" opacity={0.4} radius={[2,2,0,0]} />
                              <Legend wrapperStyle={{ fontSize: 10 }} />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                        {/* Cash Balance comparison */}
                        <div>
                          <p className="text-xs font-medium text-gray-600 mb-2">Cash Balance (€K) — <span className="text-violet-600">{leftScenario.name}</span> vs <span className="text-blue-600">{rightScenario.name}</span></p>
                          <ResponsiveContainer width="100%" height={250}>
                            <ComposedChart data={chartData} margin={{ top: 15, right: 10, left: 10, bottom: 5 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                              <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `${v >= 1000 ? `${Math.round(v/1000)}M` : `${v}K`}`} />
                              <Tooltip formatter={(v: number, name: string) => [`€${v.toLocaleString()}K`, name]} />
                              <Area type="monotone" dataKey="closeA" name={`Closing (${leftScenario.name})`} fill="#c4b5fd" stroke="#7c3aed" strokeWidth={2} fillOpacity={0.2} />
                              <Area type="monotone" dataKey="closeB" name={`Closing (${rightScenario.name})`} fill="#bfdbfe" stroke="#2563eb" strokeWidth={2} fillOpacity={0.2} />
                              <Line type="monotone" dataKey="netA" name={`Net (${leftScenario.name})`} stroke="#8b5cf6" strokeWidth={1.5} strokeDasharray="5 5" dot={{ r: 2 }} />
                              <Line type="monotone" dataKey="netB" name={`Net (${rightScenario.name})`} stroke="#3b82f6" strokeWidth={1.5} strokeDasharray="5 5" dot={{ r: 2 }} />
                              <Legend wrapperStyle={{ fontSize: 10 }} />
                            </ComposedChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    );
                  })()}
                </div>
              </div>
            </div>
          );
        })()}

        <div className="text-center text-xs text-gray-400 py-4">
          Banks Dashboard | Subsidiary {companyConfig.subsidiary} | Data from NetSuite SuiteQL{companyConfig.hasSF ? ' + Snowflake' : ''}
        </div>
      </div>

      {/* ── AI Chat Panel with backdrop ── */}
      {chatOpen && (
        <div className="fixed inset-0 bg-black/30 z-40" onClick={() => setChatOpen(false)} />
      )}
      {chatOpen && (
        <div className="fixed top-16 left-1/2 -translate-x-1/2 bg-white rounded-xl shadow-2xl border border-gray-200 flex flex-col z-50 overflow-hidden resize"
             style={{ width: '600px', minWidth: '380px', maxWidth: '90vw', height: '75vh', minHeight: '300px', maxHeight: '90vh' }}>
          {/* Chat Header */}
          <div className="flex items-center justify-between px-4 py-2.5 bg-gradient-to-r from-emerald-600 to-teal-600 text-white flex-shrink-0">
            <div className="flex items-center gap-2">
              <Sparkles className="w-4 h-4" />
              <span className="font-semibold text-sm">AI Financial Analyst</span>
            </div>
            <div className="flex items-center gap-1">
              <button onClick={() => setChatShowHistory(prev => !prev)} className="text-[10px] hover:bg-white/20 rounded px-1.5 py-0.5 transition-colors" title="Chat history">
                History ({chatHistoryList.length})
              </button>
              <button onClick={startNewChat} className="text-[10px] hover:bg-white/20 rounded px-1.5 py-0.5 transition-colors" title="New chat">
                + New
              </button>
              <button onClick={() => setChatOpen(false)} className="hover:bg-white/20 rounded-full p-1 transition-colors">
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>

          {/* Chat History Sidebar */}
          {chatShowHistory && (
            <div className="border-b border-gray-200 bg-gray-50 max-h-[40%] overflow-y-auto flex-shrink-0">
              <div className="px-3 py-2 text-xs font-semibold text-gray-500 uppercase flex items-center justify-between">
                <span>Previous Chats</span>
                <button onClick={() => setChatShowHistory(false)} className="text-gray-400 hover:text-gray-600"><X className="w-3 h-3" /></button>
              </div>
              {chatHistoryList.length === 0 && (
                <p className="px-3 py-4 text-xs text-gray-400 text-center">No saved chats yet</p>
              )}
              {chatHistoryList.map(chat => (
                <div key={chat.id} className={`flex items-center justify-between px-3 py-2 hover:bg-white cursor-pointer border-b border-gray-100 ${chat.id === chatId ? 'bg-emerald-50 border-l-2 border-l-emerald-500' : ''}`}>
                  <div className="flex-1 min-w-0" onClick={() => loadChat(chat)}>
                    <p className="text-xs font-medium text-gray-700 truncate">{chat.title}</p>
                    <p className="text-[10px] text-gray-400">{new Date(chat.updatedAt).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })} · {chat.messages.length} msgs</p>
                  </div>
                  <button onClick={(e) => { e.stopPropagation(); deleteChat(chat.id); }} className="text-gray-300 hover:text-red-500 ml-2 flex-shrink-0" title="Delete">
                    <X className="w-3 h-3" />
                  </button>
                </div>
              ))}
            </div>
          )}

          {/* Chat Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {chatMessages.length === 0 && (
              <div className="text-center py-6">
                <Sparkles className="w-8 h-8 text-emerald-400 mx-auto mb-3" />
                <p className="text-sm font-medium text-gray-700 mb-1">Ask me anything about your data</p>
                <p className="text-xs text-gray-400 mb-4">I can analyze trends, create scenarios, and recommend changes</p>
                <div className="space-y-2">
                  {['What is our current cash runway?', 'What if we reduce vendor spend by 15%?', 'Which months have the highest outflows?', 'Recommend ways to improve net cash position'].map((q, i) => (
                    <button key={i} onClick={() => { if (chatPanelInputRef.current) { chatPanelInputRef.current.value = q; chatPanelInputRef.current.focus(); } }}
                      className="block w-full text-left text-xs px-3 py-2 rounded-lg bg-gray-50 hover:bg-emerald-50 text-gray-600 hover:text-emerald-700 transition-colors border border-gray-100 hover:border-emerald-200">
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}
            {chatMessages.map((msg, i) => (
              <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                <div className={`max-w-[90%] ${msg.role === 'user' ? '' : 'group'}`}>
                  <div id={`chat-msg-${i}`} className={`rounded-xl px-3 py-2 text-sm ${
                    msg.role === 'user'
                      ? 'bg-emerald-600 text-white rounded-br-sm'
                      : 'bg-gray-100 text-gray-800 rounded-bl-sm'
                  }`}>
                    {msg.role === 'assistant' ? (
                      <div className="text-xs leading-relaxed prose-sm" dangerouslySetInnerHTML={{ __html: mdToHtml(msg.content) }} />
                    ) : (
                      <span>{msg.content}</span>
                    )}
                  </div>
                  {msg.role === 'assistant' && (
                    <div className="flex items-center gap-1 mt-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={async () => {
                          const el = document.getElementById(`chat-msg-${i}`);
                          if (!el) return;
                          try {
                            const canvas = await html2canvas(el, { backgroundColor: '#f3f4f6', scale: 2 });
                            const link = document.createElement('a');
                            link.download = `ai-analysis-${new Date().toISOString().slice(0,10)}.png`;
                            link.href = canvas.toDataURL('image/png');
                            link.click();
                          } catch {}
                        }}
                        className="text-[10px] text-gray-400 hover:text-gray-600 bg-white border border-gray-200 rounded px-1.5 py-0.5 flex items-center gap-1"
                        title="Save as image"
                      >📷 Save Image</button>
                      <button
                        onClick={async () => {
                          const el = document.getElementById(`chat-msg-${i}`);
                          if (!el) return;
                          try {
                            const canvas = await html2canvas(el, { backgroundColor: '#f3f4f6', scale: 2 });
                            canvas.toBlob(async (blob) => {
                              if (!blob) return;
                              if (navigator.share && navigator.canShare) {
                                const file = new File([blob], 'analysis.png', { type: 'image/png' });
                                if (navigator.canShare({ files: [file] })) {
                                  await navigator.share({ files: [file], title: 'AI Financial Analysis', text: msg.content.slice(0, 200) });
                                  return;
                                }
                              }
                              // Fallback: WhatsApp web with text
                              const text = encodeURIComponent(msg.content.slice(0, 2000));
                              window.open(`https://wa.me/?text=${text}`, '_blank');
                            }, 'image/png');
                          } catch {}
                        }}
                        className="text-[10px] text-gray-400 hover:text-green-600 bg-white border border-gray-200 rounded px-1.5 py-0.5 flex items-center gap-1"
                        title="Share via WhatsApp"
                      >💬 WhatsApp</button>
                      <button
                        onClick={() => { navigator.clipboard.writeText(msg.content); }}
                        className="text-[10px] text-gray-400 hover:text-gray-600 bg-white border border-gray-200 rounded px-1.5 py-0.5 flex items-center gap-1"
                        title="Copy text"
                      >📋 Copy</button>
                    </div>
                  )}
                </div>
              </div>
            ))}
            {chatLoading && (
              <div className="flex justify-start">
                <div className="bg-gray-100 rounded-xl px-4 py-3 rounded-bl-sm">
                  <div className="flex items-center gap-1">
                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }}></div>
                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
                  </div>
                </div>
              </div>
            )}
            <div ref={chatEndRef} />
          </div>

          {/* Chat Input inside panel */}
          <div className="border-t border-gray-200 p-3 bg-gray-50 flex-shrink-0">
            {chatAttachment && (
              <div className="flex items-center gap-1 mb-2 bg-emerald-50 text-emerald-700 text-xs px-2 py-1 rounded-lg">
                <Paperclip className="w-3 h-3" /><span className="truncate">{chatAttachment.name}</span>
                <button onClick={() => setChatAttachment(null)} className="ml-auto hover:text-red-500"><X className="w-3 h-3" /></button>
              </div>
            )}
            <div className="flex items-center gap-2">
              <button onClick={() => chatFileInputRef.current?.click()} title="Attach file"
                className="p-2 rounded-lg border border-gray-200 text-gray-400 hover:text-emerald-600 hover:border-emerald-300 transition-colors flex-shrink-0">
                <Paperclip className="w-4 h-4" />
              </button>
              <input
                ref={chatPanelInputRef}
                type="text"
                defaultValue=""
                onKeyDown={e => { if (e.key === 'Enter' && (e.target as HTMLInputElement).value.trim()) { sendChatMessage(); } }}
                placeholder={chatAttachment ? "Ask about the file..." : "Follow up..."}
                className="flex-1 text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent"
              />
              <button
                onClick={sendChatMessage}
                disabled={chatLoading}
                className="p-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex-shrink-0"
              >
                {chatLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
