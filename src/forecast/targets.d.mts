// Type declarations for targets.mjs (projection-year targets on top of the Plan).

export interface RevenueTargets {
  mode: 'none' | 'growth' | 'newMrr';
  /** % growth per month, compounding (12 values). */
  growthPct: number[];
  /** € of new monthly revenue added per month, cumulative (12 values). */
  newMrr: number[];
  /** % of revenue lost per month, compounding (12 values). */
  churnPct: number[];
}
export interface DeptPct { dept: string; pct: number; from: number }
export interface Hire { dept: string; monthlyCost: number; start: number; count: number }

export interface Targets {
  revenue: RevenueTargets;
  payroll: { deptPct: DeptPct[]; hires: Hire[] };
  opex: { categoryPct: Record<string, number> };
  server: { enabled: boolean; pctOfRevenue: number; category: string };
}

export interface TargetsBaseMonth {
  mKey: string;
  revenue: number;
  collPct: number;
  payroll: number;
  payrollByDept: Record<string, number>;
  opex: number;
  opexByCategory: Record<string, number>;
  ilsRate: number;
}
export interface TargetsBase {
  version: number;
  year: number;
  months: TargetsBaseMonth[];
  departments: string[];
  categories: string[];
  serverCategory: string;
  serverRatioYtd: number | null;
}

export interface TargetDelta {
  mKey: string;
  dRevenue: number;
  dPayroll: number;
  dOpex: number;
  dServer: number;
  revenue: number;
  server: number;
  serverBase: number;
}

interface YearLike<F> { year: number; rows: { mKey: string; eur: F; ils: F }[] }

export const TARGETS_VERSION: number;
export const ALL_DEPARTMENTS: '*';
export function emptyTargets(): Targets;
export function validateTargets(input: unknown): { ok: boolean; targets: Targets | null; errors: string[] };
export function isEmptyTargets(t: Targets | null | undefined): boolean;
/** One line per driver that changes something, in plain words. */
export function describeTargets(t: Targets | null | undefined): string[];
export function buildTargetsBase(args: {
  year: number;
  months: { mKey: string; revenue: number; collPct: number; payroll: number; opex: number; ilsRate: number }[];
  deptAmounts: Record<string, number>;
  categoryAmountsByMonth: Record<string, Record<string, number>>;
  serverCategory?: string;
  serverRatioYtd?: number | null;
}): TargetsBase;
export function computeTargetDeltas(base: TargetsBase, targets: Targets | null): TargetDelta[];
export function applyCashTargets<Y extends YearLike<F>, F>(block: Y, base: TargetsBase, deltas: TargetDelta[]): Y;
export function applyPnlTargets<Y extends YearLike<F>, F>(block: Y, base: TargetsBase, deltas: TargetDelta[]): Y;
export function variantWithTargets<V extends { years: YearLike<F>[] }, F>(
  variant: V, base: TargetsBase | null | undefined, targets: Targets | null,
  apply: (block: V['years'][number], base: TargetsBase, deltas: TargetDelta[]) => V['years'][number],
): V;
