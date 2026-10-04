import { useCallback, useEffect, useState } from 'react';
import type { ComputingResponse, ErrorResponse, ProjectionPayload } from './types.ts';

const POLL_MS = 5000;

export type Phase = 'loading' | 'computing' | 'ready' | 'error';

/** What both projection pages' payloads share (the cached-handler envelope). */
export interface ReadyPayload {
  status: 'ready';
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}
export type Fetcher<P extends ReadyPayload> = (refresh: boolean) => Promise<P | ComputingResponse | ErrorResponse>;

export interface ProjectionState<P extends ReadyPayload = ProjectionPayload> {
  phase: Phase;
  data: P | null;
  /** Last error; with data present it means a refresh failed and the shown data is older. */
  error: string | null;
  /** Seconds the first computation has been running (from the server). */
  computingElapsedSec: number | null;
  refreshing: boolean;
  refresh: () => void;
}

/**
 * Loads the projection and keeps polling while the server is computing it (first load) or
 * refreshing it in the background, so new figures appear without a reload. `fetcher` must be
 * stable (a module-level function).
 */
export function useProjection<P extends ReadyPayload>(fetcher: Fetcher<P>): ProjectionState<P> {
  const [phase, setPhase] = useState<Phase>('loading');
  const [data, setData] = useState<P | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [computingElapsedSec, setComputingElapsedSec] = useState<number | null>(null);
  const [refreshRequested, setRefreshRequested] = useState(false);
  // Bumped after each response that asks for another look; the polling effect keys on it.
  const [pollToken, setPollToken] = useState(0);
  const [polling, setPolling] = useState(false);

  const load = useCallback(async (refresh: boolean) => {
    let again = false;
    try {
      const body = await fetcher(refresh);
      if (body.status === 'ready') {
        setData(body);
        setPhase('ready');
        setError(body.cache?.lastError ?? null);
        setComputingElapsedSec(null);
        again = !!body.cache?.refreshing;
        if (!again) setRefreshRequested(false);
      } else if (body.status === 'computing') {
        setPhase((p) => (p === 'ready' ? p : 'computing'));
        setComputingElapsedSec(body.elapsedSec);
        again = true;
      } else {
        setError(body.error);
        setPhase((p) => (p === 'ready' ? p : 'error'));
        setRefreshRequested(false);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase((p) => (p === 'ready' ? p : 'error'));
      setRefreshRequested(false);
    }
    setPolling(again);
    if (again) setPollToken((t) => t + 1);
  }, [fetcher]);

  useEffect(() => {
    void load(false);
  }, [load]);

  useEffect(() => {
    if (!polling) return;
    const t = setTimeout(() => { void load(false); }, POLL_MS);
    return () => clearTimeout(t);
  }, [polling, pollToken, load]);

  const refresh = useCallback(() => {
    setRefreshRequested(true);
    setError(null);
    void load(true);
  }, [load]);

  return {
    phase,
    data,
    error,
    computingElapsedSec,
    refreshing: refreshRequested || !!data?.cache?.refreshing,
    refresh,
  };
}
