// Last line of defence for the projection pages: if rendering ever throws, say so on the page (with a
// reload button) instead of leaving a blank frame inside finance-it.
import { Component, type ReactNode } from 'react';

interface State { error: Error | null }

export default class PageErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error) {
    console.error('[page] render failed:', error);
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="m-6 max-w-xl rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-900">
        <p className="font-semibold">This page could not be shown.</p>
        <p className="mt-1">{this.state.error.message || 'An unexpected error occurred.'}</p>
        <button type="button" onClick={() => window.location.reload()}
          className="mt-3 rounded-md border border-rose-300 bg-white px-3 py-1.5 text-xs font-medium text-rose-900 hover:bg-rose-100">
          Reload
        </button>
      </div>
    );
  }
}
