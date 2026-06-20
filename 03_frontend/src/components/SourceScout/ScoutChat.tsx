import { useState, useRef, useEffect, useCallback } from 'react';
import { Radar, ArrowUp, Database, MessageSquare, GitBranch, Play, ChevronDown, ChevronRight, Table2, Sparkles } from 'lucide-react';
import { streamChat, type ChatBlock, type AssetCard, type LineageNode } from '../../api/scout';

// ── Conversation model ────────────────────────────────────────────────────────
type Turn =
  | { role: 'user'; text: string }
  | { role: 'assistant'; blocks: ChatBlock[] };

const STARTERS = [
  { icon: Database,      text: 'Find payment data' },
  { icon: GitBranch,     text: 'Where does customer_360 come from?' },
  { icon: Table2,        text: 'Top 5 merchants by amount in payment_transactions' },
  { icon: MessageSquare, text: 'What is fraud_alerts?' },
];

export function ScoutChat() {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const ctxRef = useRef<{ asset?: string; assetType?: string }>({});
  const cancelRef = useRef<(() => void) | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [turns, streaming]);

  // Clear streaming flag and drop any leftover 'thinking' block on stream end.
  const finishStream = useCallback(() => {
    setStreaming(false);
    setTurns(prev => {
      const next = [...prev];
      const last = next[next.length - 1];
      if (last?.role === 'assistant') {
        next[next.length - 1] = { role: 'assistant', blocks: last.blocks.filter(b => b.type !== 'thinking') };
      }
      return next;
    });
  }, []);

  const send = useCallback((message: string) => {
    const msg = message.trim();
    if (!msg || streaming) return;
    setInput('');
    setStreaming(true);
    setTurns(prev => [...prev, { role: 'user', text: msg }, { role: 'assistant', blocks: [] }]);

    cancelRef.current?.();
    cancelRef.current = streamChat(
      msg,
      ctxRef.current,
      (block) => {
        if (block.type === 'context') {
          ctxRef.current = { asset: block.asset, assetType: block.asset_type };
          return; // context is state, not a rendered block
        }
        setTurns(prev => {
          const next = [...prev];
          const last = next[next.length - 1];
          if (last?.role !== 'assistant') return prev;
          // 'thinking' is transient status: never keep more than the latest one,
          // and clear them all the moment a substantive block arrives.
          const blocks: ChatBlock[] = last.blocks.filter(b => b.type !== 'thinking');
          blocks.push(block);
          next[next.length - 1] = { role: 'assistant', blocks };
          return next;
        });
      },
      finishStream,
      finishStream,
    );
  }, [streaming, finishStream]);

  const empty = turns.length === 0;

  return (
    <div className="flex flex-col h-full bg-agent-dark-bg">
      {/* Header */}
      <div className="flex items-center gap-2.5 px-6 py-4 border-b border-agent-dark-border flex-shrink-0">
        <div className="w-7 h-7 rounded-lg bg-cloudera/15 flex items-center justify-center">
          <Radar size={16} className="text-cloudera" />
        </div>
        <div>
          <h2 className="text-sm font-semibold text-agent-text-primary leading-tight">Source Scout</h2>
          <p className="text-xs text-agent-text-secondary leading-tight">Ask, discover, trace, and query — across your Cloudera platform</p>
        </div>
        {ctxRef.current.asset && !empty && (
          <div className="ml-auto flex items-center gap-1.5 text-xs text-agent-text-secondary bg-agent-dark-surface border border-agent-dark-border rounded-full px-3 py-1">
            <span className="w-1.5 h-1.5 rounded-full bg-cloudera" />
            <span className="font-mono">{ctxRef.current.asset}</span>
          </div>
        )}
      </div>

      {/* Thread / empty state */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {empty ? (
          <div className="h-full flex flex-col items-center justify-center px-6 text-center">
            <div className="w-12 h-12 rounded-2xl bg-cloudera/15 flex items-center justify-center mb-5">
              <Sparkles size={22} className="text-cloudera" />
            </div>
            <h1 className="text-xl font-semibold text-agent-text-primary mb-2">Ask anything about your data</h1>
            <p className="text-sm text-agent-text-secondary max-w-md mb-7">
              Discover assets in plain English, trace lineage from OpenMetadata, and get answers by running SQL on Cloudera — all in one conversation.
            </p>
            <div className="grid grid-cols-2 gap-2.5 w-full max-w-xl">
              {STARTERS.map(s => (
                <button
                  key={s.text}
                  onClick={() => send(s.text)}
                  className="flex items-center gap-2.5 text-left text-sm text-agent-text-primary bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-xl px-4 py-3 transition-colors"
                >
                  <s.icon size={15} className="text-cloudera flex-shrink-0" />
                  <span className="truncate">{s.text}</span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="max-w-3xl mx-auto px-6 py-6 space-y-6">
            {turns.map((turn, i) =>
              turn.role === 'user' ? (
                <div key={i} className="flex justify-end">
                  <div className="bg-cloudera text-white rounded-2xl rounded-br-md px-4 py-2.5 text-sm max-w-[80%]">
                    {turn.text}
                  </div>
                </div>
              ) : (
                <AssistantTurn key={i} blocks={turn.blocks} onAsk={send} streaming={streaming && i === turns.length - 1} />
              )
            )}
          </div>
        )}
      </div>

      {/* Composer */}
      <div className="flex-shrink-0 border-t border-agent-dark-border bg-agent-dark-bg px-6 py-4">
        <form
          onSubmit={e => { e.preventDefault(); send(input); }}
          className="max-w-3xl mx-auto flex items-end gap-2 bg-agent-dark-surface border border-agent-dark-border rounded-2xl px-4 py-2.5 focus-within:border-cloudera/60 transition-colors"
        >
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder={ctxRef.current.asset ? `Ask a follow-up about ${ctxRef.current.asset}…` : 'Ask anything about your data…'}
            disabled={streaming}
            className="flex-1 bg-transparent text-sm text-agent-text-primary placeholder-agent-text-secondary focus:outline-none py-1"
          />
          <button
            type="submit"
            disabled={!input.trim() || streaming}
            className="w-8 h-8 rounded-lg bg-cloudera hover:bg-cloudera-hover disabled:opacity-30 flex items-center justify-center transition-colors flex-shrink-0"
          >
            {streaming
              ? <div className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
              : <ArrowUp size={16} className="text-white" />}
          </button>
        </form>
      </div>
    </div>
  );
}

// ── Assistant turn = sequence of blocks ───────────────────────────────────────
function AssistantTurn({ blocks, onAsk, streaming }: { blocks: ChatBlock[]; onAsk: (q: string) => void; streaming: boolean }) {
  return (
    <div className="flex gap-3">
      <div className="w-7 h-7 rounded-lg bg-cloudera/15 flex items-center justify-center flex-shrink-0 mt-0.5">
        <Radar size={14} className="text-cloudera" />
      </div>
      <div className="flex-1 min-w-0 space-y-3">
        {blocks.length === 0 && streaming && <ThinkingDots />}
        {blocks.map((b, i) => <BlockView key={i} block={b} onAsk={onAsk} />)}
      </div>
    </div>
  );
}

function BlockView({ block, onAsk }: { block: ChatBlock; onAsk: (q: string) => void }) {
  switch (block.type) {
    case 'thinking':   return <ThinkingLine text={block.text} />;
    case 'text':       return <TextBlock text={block.text} />;
    case 'assets':     return <AssetsBlock assets={block.assets} onAsk={onAsk} />;
    case 'lineage':    return <LineageBlock asset={block.asset} upstream={block.upstream} downstream={block.downstream} edgeCount={block.edge_count} onAsk={onAsk} />;
    case 'sql_result': return <SqlResultBlock block={block} />;
    case 'schema':     return <SchemaBlock asset={block.asset} fields={block.fields} onAsk={onAsk} />;
    default:           return null;
  }
}

// ── Block renderers ───────────────────────────────────────────────────────────
function ThinkingDots() {
  return (
    <div className="flex items-center gap-1 py-1">
      {[0, 1, 2].map(i => (
        <span key={i} className="w-1.5 h-1.5 rounded-full bg-agent-text-secondary animate-pulse" style={{ animationDelay: `${i * 150}ms` }} />
      ))}
    </div>
  );
}

function ThinkingLine({ text }: { text: string }) {
  return (
    <div className="flex items-center gap-2 text-xs text-agent-text-secondary">
      <div className="w-3 h-3 border-2 border-cloudera border-t-transparent rounded-full animate-spin" />
      {text}
    </div>
  );
}

// Minimal markdown: **bold**, `code`, *italic*
function renderInline(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*)/g);
  return parts.map((p, i) => {
    if (p.startsWith('**') && p.endsWith('**')) return <strong key={i} className="font-semibold text-agent-text-primary">{p.slice(2, -2)}</strong>;
    if (p.startsWith('`') && p.endsWith('`')) return <code key={i} className="font-mono text-cloudera bg-agent-dark-surface px-1 py-0.5 rounded text-[0.85em]">{p.slice(1, -1)}</code>;
    if (p.startsWith('*') && p.endsWith('*')) return <em key={i} className="italic">{p.slice(1, -1)}</em>;
    return <span key={i}>{p}</span>;
  });
}

function TextBlock({ text }: { text: string }) {
  return <p className="text-sm text-agent-text-primary leading-relaxed">{renderInline(text)}</p>;
}

const TYPE_BADGE: Record<string, { label: string; cls: string }> = {
  iceberg_table: { label: 'Iceberg', cls: 'text-teal-300 bg-teal-500/10 border-teal-500/30' },
  kafka_topic:   { label: 'Kafka',   cls: 'text-blue-300 bg-blue-500/10 border-blue-500/30' },
};

function AssetsBlock({ assets, onAsk }: { assets: AssetCard[]; onAsk: (q: string) => void }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
      {assets.map(a => {
        const badge = TYPE_BADGE[a.asset_type] ?? { label: a.asset_type, cls: 'text-agent-text-secondary bg-agent-dark-surface border-agent-dark-border' };
        return (
          <button
            key={a.name}
            onClick={() => onAsk(a.asset_type === 'iceberg_table' ? `Show me the lineage of ${a.name}` : `What is ${a.name}?`)}
            className="text-left bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-xl p-3 transition-colors group"
          >
            <div className="flex items-center justify-between mb-1.5">
              <span className="font-mono text-sm text-agent-text-primary truncate">{a.name}</span>
              <span className={`text-[10px] px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ml-2 ${badge.cls}`}>{badge.label}</span>
            </div>
            <div className="text-xs text-agent-text-secondary truncate">
              {a.field_count} field{a.field_count !== 1 ? 's' : ''} · {a.fields.slice(0, 4).join(', ')}{a.fields.length > 4 ? '…' : ''}
            </div>
          </button>
        );
      })}
    </div>
  );
}

function LineageNodeRow({ node, tone }: { node: LineageNode; tone: 'up' | 'down' }) {
  const color = tone === 'up' ? 'bg-blue-400' : 'bg-green-400';
  // entity type inferred from FQN service prefix
  const kind = node.fqn?.startsWith('cdp_kafka') ? 'topic'
    : node.fqn?.startsWith('cdp_superset') ? 'dashboard'
    : node.fqn?.startsWith('cdp_mlflow') ? 'ml model'
    : 'table';
  return (
    <div className="flex items-start gap-2.5 py-1.5">
      <span className={`w-1.5 h-1.5 rounded-full ${color} mt-1.5 flex-shrink-0`} />
      <div className="min-w-0">
        <div className="text-sm text-agent-text-primary font-mono truncate">{node.name}</div>
        <div className="text-xs text-agent-text-secondary">{kind}</div>
      </div>
    </div>
  );
}

function LineageBlock({ asset, upstream, downstream, edgeCount, onAsk }: {
  asset: string; upstream: LineageNode[]; downstream: LineageNode[]; edgeCount: number; onAsk: (q: string) => void;
}) {
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <GitBranch size={14} className="text-cloudera" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">Lineage</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{edgeCount} edge{edgeCount !== 1 ? 's' : ''}</span>
      </div>
      <div className="p-4 grid grid-cols-[1fr_auto_1fr] gap-4 items-center">
        {/* upstream */}
        <div>
          <div className="text-[10px] font-semibold text-blue-300/80 uppercase tracking-wider mb-1">Upstream ({upstream.length})</div>
          {upstream.length ? upstream.map((n, i) => <LineageNodeRow key={i} node={n} tone="up" />)
            : <div className="text-xs text-agent-text-secondary py-1.5">— none —</div>}
        </div>
        {/* current */}
        <div className="flex flex-col items-center">
          <div className="px-3 py-2 rounded-lg bg-cloudera/15 border border-cloudera/40 text-center">
            <div className="text-xs font-mono font-semibold text-cloudera whitespace-nowrap">{asset.split('.').pop()}</div>
            <div className="text-[10px] text-agent-text-secondary">this asset</div>
          </div>
        </div>
        {/* downstream */}
        <div>
          <div className="text-[10px] font-semibold text-green-300/80 uppercase tracking-wider mb-1">Downstream ({downstream.length})</div>
          {downstream.length ? downstream.map((n, i) => <LineageNodeRow key={i} node={n} tone="down" />)
            : <div className="text-xs text-agent-text-secondary py-1.5">— none —</div>}
        </div>
      </div>
      <div className="px-4 pb-3 flex gap-2">
        <button onClick={() => onAsk(`Run a query on ${asset}`)}
          className="text-xs text-cloudera hover:underline">Query this asset →</button>
      </div>
    </div>
  );
}

function SqlResultBlock({ block }: { block: Extract<ChatBlock, { type: 'sql_result' }> }) {
  const [showSql, setShowSql] = useState(false);
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      {/* SQL toggle */}
      <button onClick={() => setShowSql(s => !s)}
        className="w-full flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border hover:bg-agent-dark-border/40 transition-colors">
        {showSql ? <ChevronDown size={14} className="text-agent-text-secondary" /> : <ChevronRight size={14} className="text-agent-text-secondary" />}
        <Play size={12} className="text-green-400" />
        <span className="text-xs font-semibold text-agent-text-primary">SQL</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{block.executed_on ? `ran on ${block.executed_on}` : ''}</span>
      </button>
      {showSql && (
        <pre className="px-4 py-3 text-xs font-mono text-[#9ab8cc] whitespace-pre-wrap border-b border-agent-dark-border bg-agent-dark-bg overflow-x-auto">{block.sql}</pre>
      )}
      {block.error ? (
        <div className="px-4 py-3 text-xs text-red-300">{block.error}</div>
      ) : block.columns.length === 0 ? (
        <div className="px-4 py-3 text-xs text-agent-text-secondary">No rows.</div>
      ) : (
        <div className="overflow-x-auto max-h-80 overflow-y-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-agent-dark-surface">
              <tr>{block.columns.map((c, i) => (
                <th key={i} className="text-left px-4 py-2 font-semibold text-cloudera border-b border-agent-dark-border whitespace-nowrap">{c}</th>
              ))}</tr>
            </thead>
            <tbody>
              {block.rows.map((row, ri) => (
                <tr key={ri} className="border-b border-agent-dark-border/40 hover:bg-agent-dark-border/30">
                  {row.map((cell, ci) => (
                    <td key={ci} className="px-4 py-1.5 font-mono text-agent-text-primary whitespace-nowrap">
                      {cell === null ? <span className="text-agent-text-secondary italic">null</span> : cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function SchemaBlock({ asset, fields, onAsk }: { asset: string; fields: Array<{ name: string; type?: string }>; onAsk: (q: string) => void }) {
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <Table2 size={14} className="text-cloudera" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">Schema</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{fields.length} fields</span>
      </div>
      <div className="p-3 grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-1.5">
        {fields.map((f, i) => (
          <div key={i} className="flex items-baseline gap-2 min-w-0">
            <span className="text-xs font-mono text-agent-text-primary truncate">{f.name}</span>
            <span className="text-[10px] text-agent-text-secondary ml-auto flex-shrink-0">{f.type ?? ''}</span>
          </div>
        ))}
      </div>
      <div className="px-4 pb-3 flex gap-3">
        <button onClick={() => onAsk(`Show me the lineage of ${asset}`)} className="text-xs text-cloudera hover:underline">Lineage →</button>
        <button onClick={() => onAsk(`Show 10 rows from ${asset}`)} className="text-xs text-cloudera hover:underline">Sample rows →</button>
      </div>
    </div>
  );
}
