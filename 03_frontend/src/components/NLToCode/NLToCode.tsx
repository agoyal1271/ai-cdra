import { useState, useRef, useEffect } from 'react';

interface TranslationStep {
  label: string;
  value: string;
  type: string;
}

interface NLResult {
  understanding: string;
  intent: string;
  entities: string[];
  table_used: string;
  columns_used: string[];
  filter_logic: string;
  pyiceberg: string;
  spark_sql: string;
  flink_sql: string;
  translation?: TranslationStep[];
  fallback?: boolean;
  model?: string;
}

interface OllamaModel {
  id: string;
  name: string;
  size: string;
  family: string;
}

interface LLMStats {
  model: string;
  tokens: number;
  elapsed_s: number;
  tokens_per_s: number;
}

type CodeTab = 'pyiceberg' | 'spark_sql' | 'flink_sql';

const SAMPLE_QUESTIONS = [
  'Show me all records in the table',
  'How many rows are there in total?',
  'Find all users where email is not null',
  'Get all records with id greater than 5',
  'Show all tables created in the last 1 hour',
];

const TAB_CONFIG: { key: CodeTab; label: string; icon: string }[] = [
  { key: 'pyiceberg', label: 'PyIceberg', icon: '🐍' },
  { key: 'spark_sql', label: 'Spark SQL',  icon: '⚡' },
  { key: 'flink_sql', label: 'Flink SQL',  icon: '🌊' },
];

const STEP_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  input:         { bg: 'bg-slate-800',      text: 'text-slate-200',   label: '💬' },
  understanding: { bg: 'bg-blue-950/60',    text: 'text-blue-200',    label: '🧠' },
  intent:        { bg: 'bg-purple-950/60',  text: 'text-purple-300',  label: '🎯' },
  table:         { bg: 'bg-teal-950/60',    text: 'text-teal-300',    label: '🧊' },
  columns:       { bg: 'bg-slate-800',      text: 'text-slate-300',   label: '📋' },
  filter_nl:     { bg: 'bg-amber-950/60',   text: 'text-amber-300',   label: '🔍' },
  time_resolve:  { bg: 'bg-orange-950/60',  text: 'text-orange-300',  label: '⏱' },
  sql_expr:      { bg: 'bg-emerald-950/60', text: 'text-emerald-300', label: '⚡' },
  py_expr:       { bg: 'bg-cyan-950/60',    text: 'text-cyan-300',    label: '🐍' },
};

export function NLToCode() {
  const [question, setQuestion]           = useState('');
  const [submitted, setSubmitted]         = useState('');
  const [streaming, setStreaming]         = useState(false);
  const [result, setResult]               = useState<NLResult | null>(null);
  const [activeTab, setActiveTab]         = useState<CodeTab>('pyiceberg');
  const [rawTokens, setRawTokens]         = useState('');
  const [catalogTables, setCatalogTables] = useState<string[]>([]);
  const [phase, setPhase]                 = useState<'idle' | 'loading' | 'reasoning' | 'done'>('idle');
  const [llmStats, setLlmStats]           = useState<LLMStats | null>(null);
  const [activeModel, setActiveModel]     = useState<string>('');
  const [models, setModels]               = useState<OllamaModel[]>([]);
  const [selectedModel, setSelectedModel] = useState<string>('');
  const cancelRef = useRef<() => void>(() => {});
  const tokensRef = useRef<HTMLPreElement>(null);

  // Fetch available models on mount
  useEffect(() => {
    fetch('/api/models')
      .then(r => r.json())
      .then(d => {
        setModels(d.models ?? []);
        setSelectedModel(d.default ?? d.models?.[0]?.id ?? '');
      })
      .catch(() => {});
  }, []);

  // Auto-scroll token stream
  useEffect(() => {
    if (tokensRef.current) tokensRef.current.scrollTop = tokensRef.current.scrollHeight;
  }, [rawTokens]);

  const run = (q: string, model: string) => {
    if (!q.trim() || streaming) return;
    setSubmitted(q);
    setStreaming(true);
    setResult(null);
    setRawTokens('');
    setCatalogTables([]);
    setLlmStats(null);
    setActiveModel('');
    setPhase('loading');

    let aborted = false;
    cancelRef.current = () => { aborted = true; setStreaming(false); setPhase('idle'); };

    (async () => {
      try {
        const res = await fetch('/api/nl-to-code/generate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: q, model: model || undefined }),
        });
        const reader = res.body!.getReader();
        const dec = new TextDecoder();
        let buf = '';
        while (!aborted) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          const lines = buf.split('\n');
          buf = lines.pop() ?? '';
          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            try {
              const ev = JSON.parse(line.slice(6));
              if (ev.type === 'catalog')  setCatalogTables(ev.tables ?? []);
              if (ev.type === 'model')    setActiveModel(ev.model ?? '');
              if (ev.type === 'thought' && ev.content?.includes('generating')) setPhase('reasoning');
              if (ev.type === 'token')    setRawTokens(p => p + ev.text);
              if (ev.type === 'llm_done') setLlmStats({ model: ev.model, tokens: ev.tokens, elapsed_s: ev.elapsed_s, tokens_per_s: ev.tokens_per_s });
              if (ev.type === 'understanding') {
                const { type: _, ...r } = ev;
                setResult(prev => ({ ...prev, ...r } as NLResult));
              }
              if (ev.type === 'complete') {
                const { type: _, fallback: __, ...r } = ev;
                setResult(r as NLResult);
                setPhase('done');
                setStreaming(false);
              }
            } catch { /* skip */ }
          }
        }
      } catch {
        setPhase('idle');
        setStreaming(false);
      }
    })();
  };

  const handleSubmit = (e: React.FormEvent) => { e.preventDefault(); run(question, selectedModel); };

  return (
    <div className="flex flex-col h-full p-6 gap-4 overflow-hidden">

      {/* Header */}
      <div className="flex items-center justify-between gap-4 flex-shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-2xl">💬</span>
          <div>
            <h2 className="text-lg font-bold text-white">Natural Language → Code</h2>
            <p className="text-xs text-orange-400">OLLAMA · Language Understanding Demo</p>
          </div>
        </div>

        {/* Model picker */}
        <div className="flex items-center gap-2">
          {catalogTables.length > 0 && (
            <div className="flex items-center gap-1 text-[10px] text-slate-500">
              <span className="text-teal-500">🧊</span>
              {catalogTables.map(t => (
                <span key={t} className="bg-teal-900/40 text-teal-400 px-2 py-0.5 rounded-full">{t}</span>
              ))}
            </div>
          )}
          <div className="flex items-center gap-1.5 bg-slate-800 border border-slate-700 rounded-lg px-2 py-1">
            <span className="text-[10px] text-slate-500">Model</span>
            <select
              value={selectedModel}
              onChange={e => setSelectedModel(e.target.value)}
              disabled={streaming}
              className="bg-transparent text-xs text-orange-400 font-semibold focus:outline-none cursor-pointer"
            >
              {models.map(m => (
                <option key={m.id} value={m.id} className="bg-slate-900 text-white">
                  {m.name}{m.size ? ` (${m.size})` : ''}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Input */}
      <form onSubmit={handleSubmit} className="flex gap-2 flex-shrink-0">
        <input
          value={question}
          onChange={e => setQuestion(e.target.value)}
          placeholder="Ask anything about your data — e.g. How many users signed up last month?"
          className="flex-1 bg-slate-800 border border-slate-600 rounded-xl px-4 py-3 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-orange-500 transition-colors"
          disabled={streaming}
        />
        {streaming ? (
          <button type="button" onClick={() => cancelRef.current()} className="px-4 py-2 bg-red-700 hover:bg-red-600 text-white text-sm rounded-xl">Stop</button>
        ) : (
          <button type="submit" disabled={!question.trim()} className="px-5 py-2 bg-orange-600 hover:bg-orange-500 disabled:opacity-40 text-white text-sm rounded-xl font-semibold">
            Ask
          </button>
        )}
      </form>

      {/* Sample chips */}
      {phase === 'idle' && (
        <div className="flex flex-wrap gap-2 flex-shrink-0">
          {SAMPLE_QUESTIONS.map(q => (
            <button key={q} onClick={() => { setQuestion(q); run(q, selectedModel); }}
              className="text-xs bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition-colors">
              {q}
            </button>
          ))}
        </div>
      )}

      {/* Main content */}
      {phase !== 'idle' && (
        <div className="flex flex-1 gap-4 min-h-0">

          {/* Left: How Ollama thinks */}
          <div className="w-80 flex-shrink-0 flex flex-col gap-3 min-h-0">

            {/* Question echo */}
            <div className="bg-slate-800 rounded-xl border border-slate-700 p-3 flex-shrink-0">
              <div className="text-[9px] uppercase tracking-wider text-slate-500 mb-1">You asked</div>
              <p className="text-sm text-white leading-snug">"{submitted}"</p>
            </div>

            {/* Live token stream — Ollama thinking */}
            <div className="bg-slate-900 rounded-xl border border-slate-700 flex flex-col flex-shrink-0" style={{ maxHeight: '180px' }}>
              <div className="px-3 py-2 border-b border-slate-700 flex items-center gap-2 flex-shrink-0">
                <span className="text-[9px] uppercase tracking-wider text-slate-500">
                  {activeModel ? `${activeModel} — raw output` : 'Waiting for model...'}
                </span>
                {streaming && !llmStats && (
                  <span className="ml-auto w-1.5 h-1.5 bg-orange-400 rounded-full animate-pulse" />
                )}
                {llmStats && (
                  <span className="ml-auto text-[9px] text-emerald-400 font-mono">
                    {llmStats.tokens} tok · {llmStats.elapsed_s}s · {llmStats.tokens_per_s} t/s
                  </span>
                )}
              </div>
              <pre
                ref={tokensRef}
                className="flex-1 overflow-y-auto p-3 text-[10px] font-mono text-orange-300 leading-relaxed whitespace-pre-wrap min-h-0"
                style={{ maxHeight: '130px' }}
              >
                {rawTokens || (streaming ? '...' : '')}
              </pre>
            </div>

            {/* Translation chain */}
            <div className="bg-slate-900 rounded-xl border border-slate-700 flex-1 flex flex-col min-h-0">
              <div className="px-3 py-2 border-b border-slate-700 flex items-center gap-2 flex-shrink-0">
                <span className="text-[9px] uppercase tracking-wider text-slate-500">How the model interpreted this</span>
                {streaming && result && <span className="w-1.5 h-1.5 bg-orange-400 rounded-full animate-pulse ml-auto" />}
              </div>
              <div className="flex-1 overflow-y-auto p-3 min-h-0">
                {!result && (
                  <div className="flex flex-col gap-2">
                    <Step done={catalogTables.length > 0} loading={phase === 'loading'} label="Load Iceberg catalog" detail={catalogTables.length > 0 ? `${catalogTables.length} table(s)` : undefined} />
                    <Step done={!!rawTokens} loading={phase === 'loading' && !rawTokens} label="Model generating tokens..." detail={undefined} />
                    <Step done={phase === 'done'} loading={false} label="Generate code" detail={undefined} />
                  </div>
                )}
                {result?.translation && (
                  <div className="flex flex-col gap-0">
                    {result.translation.map((step, i) => (
                      <TranslationRow key={i} step={step} isLast={i === result.translation!.length - 1} />
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Right: generated code */}
          <div className="flex-1 flex flex-col min-w-0 bg-slate-900 rounded-xl border border-slate-700">
            <div className="flex items-center gap-1 px-3 py-2 border-b border-slate-700 flex-shrink-0">
              {TAB_CONFIG.map(tab => (
                <button key={tab.key} onClick={() => setActiveTab(tab.key)}
                  className={`flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg transition-colors font-mono ${
                    activeTab === tab.key ? 'bg-orange-600 text-white' : 'text-slate-400 hover:bg-slate-800'
                  }`}>
                  <span>{tab.icon}</span> {tab.label}
                </button>
              ))}
              <div className="ml-auto flex items-center gap-3">
                {streaming && (
                  <span className="text-[10px] text-orange-400 flex items-center gap-1">
                    <span className="w-1.5 h-1.5 bg-orange-400 rounded-full animate-pulse" />
                    {activeModel || 'model'} is thinking...
                  </span>
                )}
                {result && !streaming && (
                  <span className="text-[10px] text-emerald-400">✓ Done</span>
                )}
              </div>
            </div>

            <div className="flex-1 overflow-auto p-5 min-h-0">
              {result ? (
                <pre className="text-xs font-mono text-slate-200 leading-relaxed whitespace-pre-wrap">
                  {result[activeTab] || '(no output for this format)'}
                </pre>
              ) : (
                <div className="h-full flex items-center justify-center text-slate-600 text-sm">
                  {streaming ? 'Waiting for model to finish...' : 'Code will appear here'}
                </div>
              )}
            </div>
          </div>

        </div>
      )}
    </div>
  );
}

function Step({ done, loading, label, detail }: { done: boolean; loading: boolean; label: string; detail?: string }) {
  return (
    <div className="flex gap-2.5 items-start">
      <div className={`w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 text-[9px] ${
        done ? 'bg-emerald-500 text-white' : loading ? 'bg-orange-500 animate-pulse' : 'bg-slate-700'
      }`}>
        {done ? '✓' : loading ? '…' : '·'}
      </div>
      <div>
        <div className={`text-[11px] font-medium ${done ? 'text-white' : 'text-slate-500'}`}>{label}</div>
        {detail && <div className="text-[10px] text-slate-400 mt-0.5 leading-snug">{detail}</div>}
      </div>
    </div>
  );
}

function TranslationRow({ step, isLast }: { step: TranslationStep; isLast: boolean }) {
  const style = STEP_STYLES[step.type] ?? STEP_STYLES.input;
  return (
    <div className="flex flex-col">
      <div className={`rounded-lg p-2.5 ${style.bg}`}>
        <div className="text-[8px] uppercase tracking-wider text-slate-500 mb-0.5">{style.label} {step.label}</div>
        <div className={`text-[11px] font-mono leading-snug ${style.text}`}>{step.value}</div>
      </div>
      {!isLast && <div className="flex justify-center py-0.5 text-slate-600 text-[10px]">↓</div>}
    </div>
  );
}
