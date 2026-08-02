import { useEffect, useMemo, useState } from "react";
import { useWebSocket } from "../hooks/useWebSocket";
import type { ProviderInfo } from "../types";

const SDK_LABELS: Record<string, string> = {
  openai: "OpenAI SDK",
  anthropic: "Anthropic SDK",
  google: "Google SDK",
  auto: "Auto",
};

interface Toast {
  kind: "success" | "error";
  text: string;
}

export default function ProvidersPage() {
  const { status, events, sendCommand } = useWebSocket();

  const [providers, setProviders] = useState<ProviderInfo[]>([]);
  const [keys, setKeys] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState<Record<string, boolean>>({});
  const [toast, setToast] = useState<Toast | null>(null);
  const [loading, setLoading] = useState(true);

  const connected = status === "connected";

  // ── Parse provider-related events ────────────────────────────
  const providerEvents = useMemo(
    () => events.filter((e) =>
      e.type === "providers_list" || e.type === "providers_updated" ||
      e.type === "provider_operation_result"
    ),
    [events]
  );

  // Apply providers_list payloads
  useEffect(() => {
    for (const ev of providerEvents) {
      if (ev.type === "providers_list") {
        const list = ev.data?.providers;
        if (Array.isArray(list)) {
          setProviders(list as ProviderInfo[]);
          setLoading(false);
        }
      } else if (ev.type === "providers_updated") {
        const provider = ev.data?.provider as string | undefined;
        const hasKey = Boolean(ev.data?.has_key);
        if (provider) {
          setProviders((prev) =>
            prev.map((p) => (p.key === provider ? { ...p, has_key: hasKey } : p))
          );
        }
      } else if (ev.type === "provider_operation_result") {
        const success = Boolean(ev.data?.success);
        const provider = ev.data?.provider as string | undefined;
        const message = (ev.data?.message as string) || ev.action;
        if (provider) setBusy((prev) => ({ ...prev, [provider]: false }));
        setToast({ kind: success ? "success" : "error", text: message });
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [providerEvents]);

  // Request the provider list when connected
  useEffect(() => {
    if (connected && providers.length === 0) {
      sendCommand("providers_list");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connected, providers.length, sendCommand]);

  const saveKey = (provider: string) => {
    const apiKey = (keys[provider] || "").trim();
    if (!apiKey) {
      setToast({ kind: "error", text: `Paste an API key for ${provider} first.` });
      return;
    }
    setBusy((prev) => ({ ...prev, [provider]: true }));
    sendCommand("provider_save_key", { provider, api_key: apiKey });
  };

  const removeKey = (provider: string) => {
    setBusy((prev) => ({ ...prev, [provider]: true }));
    sendCommand("provider_remove_key", { provider });
  };

  const configured = providers.filter((p) => p.has_key || p.local).length;

  return (
    <div className="flex-1 flex flex-col overflow-hidden bg-brand-cream/60">
      {/* Page header */}
      <header className="border-b border-surface-border px-6 py-4 flex items-center justify-between shrink-0 bg-white/70 backdrop-blur-md">
        <div>
          <h2 className="text-xl font-extrabold text-ink">Provider Setup</h2>
          <p className="text-xs text-ink-muted mt-0.5">
            Manage LLM API keys securely — stored encrypted in the local credential vault.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className="chip">
            <span className={`w-2 h-2 rounded-full ${connected ? "bg-accent-green animate-pulse-glow" : "bg-accent-amber animate-pulse"}`} />
            {connected ? "Live" : "Offline"}
          </span>
          <span className="chip">
            <span className="text-brand-purple font-semibold">{configured}</span>/{providers.length} ready
          </span>
        </div>
      </header>

      {/* Toast */}
      {toast && (
        <div
          className={`mx-6 mt-4 px-4 py-2.5 rounded-xl text-sm font-medium shadow-card fade-up border ${
            toast.kind === "success"
              ? "bg-emerald-50 text-emerald-700 border-emerald-200"
              : "bg-rose-50 text-rose-700 border-rose-200"
          }`}
        >
          {toast.text}
          <button
            className="float-right text-xs opacity-60 hover:opacity-100 transition-opacity"
            onClick={() => setToast(null)}
          >
            ✕
          </button>
        </div>
      )}

      {/* Provider list */}
      <div className="flex-1 overflow-y-auto px-6 py-6">
        {loading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="panel h-44 shimmer" />
            ))}
          </div>
        ) : providers.length === 0 ? (
          <div className="flex items-center justify-center h-full text-ink-muted text-sm">
            {connected
              ? "No providers registered."
              : "Connect to the AELVO backend to load providers."}
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {providers.map((p, i) => (
              <ProviderCard
                key={p.key}
                provider={p}
                index={i}
                value={keys[p.key] || ""}
                busy={Boolean(busy[p.key])}
                onValueChange={(v) => setKeys((prev) => ({ ...prev, [p.key]: v }))}
                onSave={() => saveKey(p.key)}
                onRemove={() => removeKey(p.key)}
              />
            ))}
          </div>
        )}

        {/* Footer note */}
        <p className="text-[11px] text-ink-muted mt-6 leading-relaxed max-w-3xl">
          Keys are encrypted at rest in <code className="font-mono text-brand-deep">.aelvo_runtime/credential_vault.db</code> and
          never leave this machine. Saving a key stores it for future boots and — if the backend started without a
          provider — activates the agent immediately so you can start chatting without restarting.
        </p>
      </div>
    </div>
  );
}

/* ── Provider Card ───────────────────────────────────────── */

interface ProviderCardProps {
  provider: ProviderInfo;
  index: number;
  value: string;
  busy: boolean;
  onValueChange: (v: string) => void;
  onSave: () => void;
  onRemove: () => void;
}

function ProviderCard({
  provider,
  index,
  value,
  busy,
  onValueChange,
  onSave,
  onRemove,
}: ProviderCardProps) {
  const ready = provider.local || provider.has_key;
  const badge = provider.local
    ? { label: "Local", cls: "bg-cyan-50 text-cyan-700 border-cyan-200" }
    : provider.has_key
      ? { label: "Key set", cls: "bg-emerald-50 text-emerald-700 border-emerald-200" }
      : { label: "No key", cls: "bg-amber-50 text-amber-700 border-amber-200" };

  const sdk = SDK_LABELS[provider.sdk] || provider.sdk || "SDK";

  return (
    <div
      className={`panel fade-up flex flex-col`}
      style={{ animationDelay: `${Math.min(index * 40, 400)}ms` }}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2.5">
          <span className="w-9 h-9 rounded-xl bg-gradient-to-br from-brand-orange/20 to-brand-purple/20 flex items-center justify-center text-brand-deep font-extrabold uppercase text-sm">
            {provider.name.slice(0, 2)}
          </span>
          <div>
            <div className="text-sm font-bold text-ink capitalize">{provider.name}</div>
            <div className="text-[10px] font-mono text-ink-muted">{provider.key}</div>
          </div>
        </div>
        <span className={`chip !py-0.5 !px-2 text-[10px] ${badge.cls}`}>
          {ready ? "✓ " : ""}{badge.label}
        </span>
      </div>

      {/* Meta */}
      <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] mb-3">
        <div>
          <span className="text-ink-muted">SDK</span>
          <div className="text-ink font-medium">{sdk}</div>
        </div>
        <div>
          <span className="text-ink-muted">Default model</span>
          <div className="text-ink font-medium truncate" title={provider.default_model}>
            {provider.default_model || "—"}
          </div>
        </div>
      </div>

      {provider.local ? (
        <p className="text-[11px] text-ink-muted bg-cyan-50 border border-cyan-100 rounded-lg px-3 py-2">
          Local provider — no API key required.
        </p>
      ) : (
        <div className="mt-auto space-y-2">
          <input
            type="password"
            value={value}
            onChange={(e) => onValueChange(e.target.value)}
            placeholder={`Paste ${provider.env_key || "API key"}…`}
            disabled={busy}
            className="input-field !py-2 text-xs font-mono"
          />
          <div className="flex gap-2">
            <button onClick={onSave} disabled={busy || !value.trim()} className="btn-primary flex-1 !py-2 !text-xs">
              {busy ? (
                <>
                  <span className="w-3 h-3 border-2 border-white/40 border-t-white rounded-full animate-spin" />
                  Saving…
                </>
              ) : (
                <>Save key</>
              )}
            </button>
            {provider.has_key && (
              <button
                onClick={onRemove}
                disabled={busy}
                className="btn-ghost !py-2 text-rose-600 hover:!border-rose-300 hover:!text-rose-700"
              >
                Remove
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
