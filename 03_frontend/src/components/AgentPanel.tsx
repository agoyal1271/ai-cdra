import { useState, useRef, useEffect } from 'react';
import { Play, Square, RotateCcw, AlertCircle, CheckCircle2, Clock, ChevronDown, ChevronRight } from 'lucide-react';

interface Event {
  type: string;
  agent?: string;
  [key: string]: any;
}

interface Props {
  agent: {
    id: string;
    name: string;
    tagline: string;
    description: string;
    icon: string;
  };
  endpoint: string;
  extraParams?: Record<string, any>;
}

function SystemPromptComparison({ agentId }: { agentId?: string }) {
  const [expanded, setExpanded] = useState(true);
  const isHierarchical = agentId?.includes('hierarchical');
  const isReact = agentId?.includes('react');

  const hierarchicalPrompt = `You are a DATA GOVERNANCE EXPERT analyzing database schemas.

TASK: Analyze the table columns and classify the table's sensitivity level based on what data each column likely contains.

SENSITIVITY LEVELS:
🔴 CONFIDENTIAL (7-year retention): High-risk PII enabling identity theft or financial fraud
   Examples: SSN, credit_card_number, password, medical_id, biometric_data, passport_number

🟠 RESTRICTED (90-day retention): Medium-risk PII enabling targeting or tracking
   Examples: email, phone_number, home_address, latitude/longitude, device_id, ip_address

🟡 INTERNAL (1-year retention): Low-risk employee/business data
   Examples: employee_name, department, employee_id, job_title, birth_date

🟢 PUBLIC (indefinite): No PII, can be published
   Examples: product_name, category, public_statistics

ANALYSIS APPROACH:
1. Look at each COLUMN NAME - what does it suggest the data is?
2. Look at each COLUMN TYPE - is it string, int, float, date?
3. Infer what PII this table contains based on column names/types
4. Classify the table by its HIGHEST RISK column
5. Explain your reasoning`;

  const reactFieldAnalysisPrompt = `You are a DATA METADATA ANALYST specializing in field classification.

TASK: Analyze database field names and types to infer what kind of data each represents.

FIELD TYPES TO DETECT:
🆔 IDENTIFIER: Unique IDs, accounts, customer numbers
🔴 PII (Personally Identifiable Information):
   - Email, phone, SSN, credit card, passport, driver license
   - Name + address combinations
   - Biometric data, medical records
🗺️  GEOLOCATION: Latitude, longitude, address, country, city, postal code
💰 FINANCIAL: Amount, price, balance, credit limit, salary
📅 TEMPORAL: Date, timestamp, created_at, updated_at, time
⚪ OTHER: Categories, flags, statuses, descriptions

ANALYSIS METHOD:
1. Read field NAME - what does it suggest?
2. Read field TYPE - is it string/int/float/date?
3. Look for PII indicators in the name
4. Infer what real-world data this likely contains
5. Provide confidence level (0-1)`;

  const reactPiiPrompt = `You are a PII (Personally Identifiable Information) DETECTION EXPERT.

TASK: Identify which database fields contain PII based on field classification analysis.

PII CATEGORIES:
🔴 HIGH RISK (Identity Theft): SSN, credit_card, passport, driver_license, password, medical_id, biometric_data
🟠 MEDIUM RISK (Targeting/Tracking): email, phone_number, home_address, latitude/longitude, device_id, ip_address
🟡 LOW RISK (General Info): employee_name, job_title, department

DETECTION RULES:
1. If field is "PII" type → likely contains PII
2. If field is "Geolocation" type → RESTRICTED
3. If field contains "SSN", "credit", "password" → CONFIDENTIAL
4. If field contains email/phone → RESTRICTED`;

  return (
    <div className="rounded-agent-md p-4 mb-4 border border-agent-orange/50 bg-agent-dark-surface">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left flex items-center gap-2 hover:opacity-80"
      >
        <div className="flex-shrink-0">
          {expanded ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
        </div>
        <h3 className="text-sm font-bold text-agent-orange flex items-center gap-2">
          📋 SYSTEM PROMPT {isHierarchical ? '(Hierarchical Agent)' : isReact ? '(ReAct Agent)' : ''}
        </h3>
      </button>

      {expanded && (
        <div className="mt-4 space-y-4">
          {/* Show Hierarchical or both depending on agent */}
          {(isHierarchical || !isReact) && (
          <div>
            <div className="text-xs font-semibold text-blue-400 mb-2">🏗️ HIERARCHICAL AGENT - Schema Classification</div>
            <div className="bg-agent-dark-bg border border-agent-dark-border rounded p-3 max-h-60 overflow-y-auto">
              <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">
                {hierarchicalPrompt}
              </pre>
            </div>
            <div className="text-[10px] text-agent-text-secondary mt-2">
              <strong>Approach:</strong> Single unified prompt analyzing all columns together in one LLM call
            </div>
          </div>

          )}

          {/* Show ReAct or comparison depending on agent */}
          {(isReact || !isHierarchical) && (
          <div className={isHierarchical ? 'border-t border-agent-dark-border pt-4' : ''}>
            <div className="text-xs font-semibold text-purple-400 mb-2">💭 REACT AGENT - Step-by-Step Prompts</div>

            {/* Step 1: Field Analysis */}
            <div className="mb-3">
              <div className="text-xs text-purple-300 mb-1 font-semibold">Step 1: Field Analysis</div>
              <div className="bg-agent-dark-bg border border-agent-dark-border rounded p-3 max-h-40 overflow-y-auto">
                <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">
                  {reactFieldAnalysisPrompt}
                </pre>
              </div>
            </div>

            {/* Step 2: PII Detection */}
            <div>
              <div className="text-xs text-purple-300 mb-1 font-semibold">Step 2: PII Detection</div>
              <div className="bg-agent-dark-bg border border-agent-dark-border rounded p-3 max-h-40 overflow-y-auto">
                <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">
                  {reactPiiPrompt}
                </pre>
              </div>
            </div>

            <div className="text-[10px] text-agent-text-secondary mt-2">
              <strong>Approach:</strong> Multiple specialized prompts for each analysis step (field analysis → PII detection → sensitivity classification → owner suggestion)
            </div>
          </div>
          )}

          {/* Comparison Summary - only show if not specifically on one agent */}
          {!isHierarchical && !isReact && (
          <div className="border-t border-agent-dark-border pt-4 mt-4">
            <div className="text-xs font-semibold text-agent-orange mb-2">📊 Key Differences</div>
            <div className="grid grid-cols-2 gap-4 text-[11px] text-agent-text-secondary">
              <div>
                <div className="font-semibold text-blue-400 mb-1">Hierarchical</div>
                <ul className="space-y-1">
                  <li>✅ 1 unified system prompt</li>
                  <li>✅ All columns analyzed together</li>
                  <li>✅ Faster (1 LLM call per table)</li>
                  <li>✅ Holistic schema understanding</li>
                  <li>✅ Confidence scoring built-in</li>
                </ul>
              </div>
              <div>
                <div className="font-semibold text-purple-400 mb-1">ReAct</div>
                <ul className="space-y-1">
                  <li>✅ 3+ specialized prompts</li>
                  <li>✅ Step-by-step field analysis</li>
                  <li>✅ Slower (5+ LLM calls per table)</li>
                  <li>✅ Detailed reasoning visible</li>
                  <li>✅ More transparent intermediate steps</li>
                </ul>
              </div>
            </div>
          </div>
          )}
        </div>
      )}
    </div>
  );
}

function QuestionEvent({ event }: { event: any }) {
  const [expanded, setExpanded] = useState(true);
  const [selectedAnswer, setSelectedAnswer] = useState<string | null>(null);

  const options = event.options || [];

  return (
    <div className="bg-purple-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-purple-400">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left flex items-start gap-2 hover:opacity-80"
      >
        <div className="flex-shrink-0 mt-0.5">
          {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        </div>
        <div className="flex-1 min-w-0">
          <div className="text-xs font-semibold text-purple-400">❓ Needs Clarification</div>
          {event.table && (
            <div className="text-xs text-agent-text-secondary mt-1">
              <strong>{event.table}</strong>
            </div>
          )}
          <div className="text-xs text-agent-text-secondary mt-1">
            Current guess: <strong>{event.current_guess}</strong> ({(event.confidence * 100).toFixed(0)}% confidence)
          </div>
        </div>
      </button>

      {expanded && (
        <div className="mt-3 space-y-3 border-t border-purple-400/20 pt-3">
          {/* Question */}
          {event.message && (
            <div>
              <div className="text-xs font-semibold text-purple-400 mb-1">Question</div>
              <p className="text-xs text-agent-text-secondary">{event.message}</p>
            </div>
          )}

          {/* Learning Goal */}
          {event.learning_goal && (
            <div>
              <div className="text-xs font-semibold text-purple-400 mb-1">🧠 What I'm Learning</div>
              <p className="text-xs text-agent-text-secondary">{event.learning_goal}</p>
            </div>
          )}

          {/* Columns Being Analyzed */}
          {event.columns && event.columns.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1">📋 Columns in {event.table} ({event.columns.length})</div>
              <div className="space-y-1">
                {event.columns.map((col: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-purple-400/30">
                    <strong>{col.name}</strong> <span className="text-agent-text-secondary">: {col.type}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Sensitive Fields Found */}
          {event.sensitive_fields && event.sensitive_fields.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-red-400 mb-1">⚠️ Sensitive Fields Detected</div>
              <div className="space-y-1">
                {event.sensitive_fields.map((field: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-red-400/30">
                    <strong>{field.name || field}</strong>
                    {field.reason && <div className="text-[10px] text-agent-text-secondary italic mt-0.5">{field.reason}</div>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Reasoning */}
          {event.reasoning && (
            <div>
              <div className="text-xs font-semibold text-purple-400 mb-1">💭 My Reasoning</div>
              <p className="text-xs text-agent-text-secondary">{event.reasoning}</p>
            </div>
          )}

          {/* Answer Options */}
          {options.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-purple-400 mb-2">Your Response</div>
              <div className="space-y-2">
                {options.map((option: string, i: number) => (
                  <button
                    key={i}
                    onClick={() => setSelectedAnswer(option)}
                    className={`w-full px-3 py-2 rounded text-xs font-semibold transition-colors text-left ${
                      selectedAnswer === option
                        ? 'bg-purple-500 text-white'
                        : 'bg-agent-dark-border text-agent-text-secondary hover:text-agent-text-primary hover:bg-agent-dark-border/80'
                    }`}
                  >
                    {option === 'accept_guess' ? '✅ Accept my guess' : `↪️ Change to: ${option}`}
                  </button>
                ))}
              </div>
              {selectedAnswer && (
                <div className="mt-2 p-2 bg-purple-500/20 rounded border border-purple-500/30">
                  <div className="text-xs text-purple-400">✅ Selected: <strong>{selectedAnswer === 'accept_guess' ? 'Accept guess' : selectedAnswer}</strong></div>
                  <div className="text-[10px] text-agent-text-secondary mt-1">Feedback submitted. Agent will learn from this.</div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ClassificationEvent({ event }: { event: any }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-yellow-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-yellow-400">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left flex items-start gap-2 hover:opacity-80"
      >
        <div className="flex-shrink-0 mt-0.5">
          {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        </div>
        <div className="flex-1 min-w-0">
          {event.table && (
            <div className="text-xs font-semibold text-agent-text-secondary mb-1 uppercase">
              📊 {event.table}
            </div>
          )}
          <div className="text-sm font-semibold text-yellow-400">
            🔐 {event.level?.toUpperCase()}
          </div>
          <div className="text-xs text-agent-text-secondary mt-1">
            Confidence: {event.confidence ? (event.confidence * 100).toFixed(0) : 0}%
          </div>
        </div>
      </button>

      {expanded && (
        <div className="mt-3 space-y-3 border-t border-yellow-400/20 pt-3">
          {/* Reasoning */}
          {event.reasoning && (
            <div>
              <div className="text-xs font-semibold text-yellow-400 mb-1">💭 REASONING</div>
              <p className="text-xs text-agent-text-secondary">{event.reasoning}</p>
            </div>
          )}

          {/* Columns Analyzed */}
          {event.columns && event.columns.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-agent-text-secondary mb-1">📋 COLUMNS ANALYZED ({event.columns.length})</div>
              <div className="space-y-1">
                {event.columns.map((col: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-yellow-400/30">
                    <strong>{col.name}</strong> <span className="text-agent-text-secondary">: {col.type}</span>
                    {col.description && <div className="text-[10px] text-agent-text-secondary italic mt-0.5">{col.description}</div>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Sensitive Fields */}
          {event.sensitive_fields && event.sensitive_fields.length > 0 && (
            <div>
              <div className="text-xs font-semibold text-red-400 mb-1">⚠️ SENSITIVE FIELDS</div>
              <div className="space-y-1">
                {event.sensitive_fields.map((field: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-red-400/30">
                    <strong>{field.name || field}</strong>
                    {field.reason && <div className="text-[10px] text-agent-text-secondary italic mt-0.5">{field.reason}</div>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* System Prompt */}
          {event.system_prompt && (
            <div>
              <div className="text-xs font-semibold text-blue-400 mb-1">💬 SYSTEM PROMPT</div>
              <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2 max-h-48 overflow-y-auto">
                <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">{event.system_prompt}</pre>
              </div>
            </div>
          )}

          {/* User Prompt */}
          {event.user_prompt && (
            <div>
              <div className="text-xs font-semibold text-blue-400 mb-1">👤 USER PROMPT</div>
              <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2 max-h-32 overflow-y-auto">
                <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">{event.user_prompt}</pre>
              </div>
            </div>
          )}

          {/* LLM Response */}
          {event.llm_response && (
            <div>
              <div className="text-xs font-semibold text-green-400 mb-1">🤖 LLM RESPONSE</div>
              <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2 max-h-32 overflow-y-auto">
                <pre className="text-[10px] text-agent-text-secondary whitespace-pre-wrap break-words font-mono">{event.llm_response}</pre>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function AgentPanel({ agent, endpoint, extraParams = {} }: Props) {
  const [goal, setGoal] = useState('');
  const [events, setEvents] = useState<Event[]>([]);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const eventsEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    eventsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [events]);

  const handleRun = async () => {
    if (!goal.trim()) {
      setError('Please enter a goal');
      return;
    }

    setRunning(true);
    setError(null);
    setEvents([]);

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ goal, ...extraParams }),
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const reader = response.body?.getReader();
      if (!reader) throw new Error('No response body');

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const event = JSON.parse(line.slice(6));
              if (event.type !== 'stream_end') {
                setEvents(prev => [...prev, event]);
              } else {
                setRunning(false);
              }
            } catch (e) {
              // Ignore JSON parse errors
            }
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
      setRunning(false);
    }
  };

  const handleStop = () => {
    setRunning(false);
  };

  const handleReset = () => {
    setEvents([]);
    setError(null);
    setGoal('');
  };

  return (
    <div className="flex flex-col h-full bg-agent-dark-bg">
      {/* Header */}
      <div className="border-b border-agent-dark-border px-8 py-6">
        <div className="flex items-center gap-3 mb-2">
          <span className="text-2xl">{agent.icon}</span>
          <h1 className="text-xl font-bold text-agent-text-primary">{agent.name}</h1>
        </div>
        <p className="text-sm text-agent-text-secondary">{agent.description}</p>
        <span className="inline-block mt-2 px-2 py-1 rounded bg-agent-dark-border text-xs text-agent-orange font-semibold">
          {agent.tagline}
        </span>
      </div>

      {/* Input Section */}
      <div className="flex-shrink-0 border-b border-agent-dark-border px-8 py-6 bg-agent-dark-surface">
        <label className="block text-xs font-semibold text-agent-text-secondary uppercase mb-2">Goal</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={goal}
            onChange={e => setGoal(e.target.value)}
            onKeyPress={e => e.key === 'Enter' && handleRun()}
            disabled={running}
            placeholder={`Ask ${agent.name} to...`}
            className="flex-1 px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-agent-md text-agent-text-primary placeholder-agent-text-secondary focus:outline-none focus:border-agent-orange disabled:opacity-50"
          />
          <button
            onClick={running ? handleStop : handleRun}
            disabled={!goal.trim() && !running}
            className={`px-4 py-2 rounded-agent-md font-semibold text-sm flex items-center gap-2 transition-colors ${
              running
                ? 'bg-red-500/20 text-red-400 hover:bg-red-500/30'
                : 'bg-agent-orange text-white hover:bg-agent-orange/90 disabled:opacity-50 disabled:cursor-not-allowed'
            }`}
          >
            {running ? (
              <>
                <Square size={16} />
                Stop
              </>
            ) : (
              <>
                <Play size={16} />
                Run
              </>
            )}
          </button>
          <button
            onClick={handleReset}
            disabled={running}
            className="px-3 py-2 rounded-agent-md border border-agent-dark-border text-agent-text-secondary hover:bg-agent-dark-border disabled:opacity-50"
          >
            <RotateCcw size={16} />
          </button>
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div className="flex-shrink-0 mx-8 mt-4 p-3 bg-red-500/10 border border-red-500/30 rounded-agent-md flex items-start gap-3">
          <AlertCircle size={16} className="text-red-400 mt-0.5 flex-shrink-0" />
          <span className="text-sm text-red-300">{error}</span>
        </div>
      )}

      {/* Events Output */}
      <div className="flex-1 overflow-y-auto px-8 py-6">
        {events.length === 0 && !running && (
          <div className="flex flex-col items-center justify-center h-full text-agent-text-secondary space-y-4">
            <p className="text-sm">Enter a goal and click Run to get started</p>
            <SystemPromptComparison agentId={agent.id} />
          </div>
        )}

        {/* Show Prompt Comparison at top when running */}
        {events.length > 0 && (
          <SystemPromptComparison agentId={agent.id} />
        )}

        {events.map((event, idx) => (
          <div key={idx} className="mb-4">
            {/* THOUGHT - Agent's reasoning */}
            {event.type === 'thought' && (
              <div className="flex gap-2 text-sm bg-agent-dark-border/50 p-2 rounded border-l-2 border-agent-orange">
                <Clock size={14} className="flex-shrink-0 mt-0.5 text-agent-orange" />
                <p className="text-agent-text-secondary italic">💭 {event.message}</p>
              </div>
            )}

            {/* ANALYSIS - Agent's analysis step with reasoning */}
            {event.type === 'analysis' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2 border-l-2 border-agent-orange">
                <div className="text-xs font-semibold text-agent-orange uppercase mb-2">
                  📊 {event.stage?.replace(/_/g, ' ') || 'Analysis'}
                </div>
                {event.intent && <div className="text-sm text-agent-text-primary mb-1"><strong>Intent:</strong> {event.intent}</div>}
                {event.keywords && <div className="text-xs text-agent-text-secondary mb-1"><strong>Keywords:</strong> {Array.isArray(event.keywords) ? event.keywords.join(', ') : event.keywords}</div>}
                {event.reasoning && <div className="text-xs text-agent-text-secondary"><strong>Reasoning:</strong> {event.reasoning}</div>}
                {event.message && <div className="text-sm text-agent-text-secondary">{event.message}</div>}
              </div>
            )}

            {/* REASONING - Explicit reasoning from agent */}
            {event.type === 'reasoning' && (
              <div className="bg-blue-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-blue-400">
                <div className="text-xs font-semibold text-blue-400 uppercase mb-1">🧠 {event.method}</div>
                <div className="text-xs text-agent-text-secondary">{event.description}</div>
              </div>
            )}

            {/* DISCOVERY MATCHING - Show how tables were matched */}
            {event.type === 'discovery_matching' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2 border-l-2 border-blue-400">
                <div className="text-xs font-semibold text-blue-400 mb-2">🔍 HOW TABLES WERE MATCHED</div>
                <div className="text-xs text-agent-text-secondary mb-2">
                  Checked <strong>{event.total_checked}</strong> tables, Found <strong>{event.total_matched}</strong> matches
                </div>
                <div className="text-xs text-agent-text-secondary mb-2">
                  <strong>Rules Applied:</strong> Looking for {event.rules?.sensitivity} data
                </div>
                <div className="space-y-1">
                  {event.matched_tables?.map((match: any, i: number) => (
                    <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-blue-400/30 bg-agent-dark-bg/50 p-1">
                      <strong>✓ {match.table}</strong>
                      <div className="text-[10px] text-agent-text-secondary mt-0.5">
                        {match.field_count} fields: {match.fields?.slice(0, 3).join(', ')}{match.field_count > 3 ? '...' : ''}
                      </div>
                      <div className="text-[10px] text-blue-300 mt-0.5">{match.reason}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* SEARCH RESULTS - Catalog search with reasoning */}
            {event.type === 'search_results' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2">
                <div className="text-xs font-semibold text-agent-orange mb-2">🔍 SEARCH RESULTS</div>
                <div className="text-xs text-agent-text-secondary mb-2">{event.message}</div>
                <div className="space-y-1">
                  {event.top_10?.map((table: any, i: number) => (
                    <div key={i} className="text-xs text-agent-text-secondary pl-2 border-l border-agent-dark-border">
                      <strong>{table.name}</strong> - {table.description} {table.similarity && `(${(table.similarity * 100).toFixed(0)}%)`}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* QUESTION - Agent asking for clarification */}
            {event.type === 'question' && (
              <QuestionEvent event={event} />
            )}

            {/* FIELD DETAIL - Individual field analysis */}
            {event.type === 'field_detail' && (
              <div className="bg-agent-dark-border/50 rounded-agent-md p-2 mb-1 text-xs">
                <div className="text-agent-text-secondary">
                  <strong>{event.emoji} {event.field_name}</strong>
                  <span className="text-[10px] text-agent-text-secondary ml-2">{event.field_type}</span>
                  <span className="text-[10px] text-agent-orange ml-2">{(event.confidence * 100).toFixed(0)}%</span>
                  {event.reason && <div className="text-[10px] text-agent-text-secondary italic mt-1 pl-2 border-l border-agent-dark-border">{event.reason}</div>}
                </div>
              </div>
            )}

            {/* FIELD ANALYSIS SUMMARY - All fields analyzed */}
            {event.type === 'field_analysis' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2 border-l-2 border-agent-orange">
                <div className="text-xs font-semibold text-agent-orange mb-2">
                  📋 FIELD ANALYSIS
                  {event.table && <span className="text-agent-text-secondary font-normal ml-2">({event.table}) {event.total_fields} fields</span>}
                </div>
                {event.fields?.map((field: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary mb-1 pl-2 border-l border-agent-dark-border">
                    <strong>{field.name}</strong> → {field.detected_as} ({(field.confidence * 100).toFixed(0)}%)
                    {field.reason && <div className="text-[10px] text-agent-text-secondary italic mt-0.5">{field.reason}</div>}
                  </div>
                ))}
              </div>
            )}

            {/* METADATA GENERATED - Complete metadata */}
            {event.type === 'metadata_generated' && (
              <div className="bg-green-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-green-400">
                <div className="text-xs font-semibold text-green-400 mb-2">✅ METADATA GENERATED</div>
                {event.table && (
                  <div className="text-xs text-agent-text-secondary mb-2">
                    <strong>Table:</strong> {event.table}
                  </div>
                )}
                {event.metadata && (
                  <div className="space-y-2 text-xs text-agent-text-secondary">
                    <div>
                      <strong>Sensitivity:</strong> {event.metadata.sensitivity} ({(event.metadata.confidence * 100).toFixed(0)}% confidence)
                    </div>
                    <div>
                      <strong>Fields:</strong> {event.metadata.total_fields} total, {event.metadata.pii_fields} with PII
                    </div>
                    <div className="bg-agent-dark-surface border border-agent-dark-border rounded p-2 max-h-40 overflow-y-auto">
                      <div className="text-xs font-semibold text-green-400 mb-2">Field Details:</div>
                      {event.metadata.fields?.slice(0, 10).map((field: any, i: number) => (
                        <div key={i} className="text-[10px] text-agent-text-secondary mb-1 pl-2 border-l border-green-400/30">
                          <strong>{field.name}</strong>
                          <span className="ml-2">{field.type}</span>
                          {field.is_pii && <span className="text-red-400 ml-2">🔴 PII: {field.pii_type}</span>}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* PII DETECTED - With reasoning */}
            {event.type === 'pii_detected' && (
              <div className="bg-red-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-red-400">
                {event.table && (
                  <div className="text-xs font-semibold text-agent-text-secondary mb-1 uppercase">
                    📊 {event.table}
                  </div>
                )}
                <div className="text-xs font-semibold text-red-400 mb-2">⚠️ PII DETECTED: {event.count} field(s)</div>
                {event.fields?.map((field: any, i: number) => (
                  <div key={i} className="text-xs text-agent-text-secondary mb-1 pl-2 border-l border-red-400/30">
                    <strong>{field.name}</strong> - {field.pii_type} (Risk: {field.risk})
                  </div>
                ))}
                {event.reasoning && <div className="text-xs text-agent-text-secondary mt-2 italic">💡 {event.reasoning}</div>}
                {event.message && <div className="text-xs text-agent-text-secondary mt-1 font-semibold">{event.message}</div>}
              </div>
            )}

            {/* SENSITIVITY CLASSIFICATION - With reasoning, columns, and prompts */}
            {event.type === 'sensitivity_classification' && (
              <ClassificationEvent event={event} />
            )}

            {/* OWNER SUGGESTION - With reasoning and alternatives */}
            {event.type === 'owner_suggestion' && (
              <div className="bg-green-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-green-400">
                <div className="text-sm font-semibold text-green-400 mb-1">👤 Owner: {event.owner}</div>
                {event.reasoning && <div className="text-xs text-agent-text-secondary mb-2">{event.reasoning}</div>}
                {event.alternatives?.length > 0 && (
                  <div className="text-xs text-agent-text-secondary">
                    <strong>Alternatives:</strong> {event.alternatives.join(', ')}
                  </div>
                )}
              </div>
            )}

            {/* POLICY APPLIED - Governance rules */}
            {event.type === 'policy_applied' && (
              <div className="bg-cyan-500/10 rounded-agent-md p-3 mb-2 border-l-2 border-cyan-400">
                <div className="text-xs font-semibold text-cyan-400 mb-1">📋 POLICY APPLIED</div>
                {event.table && (
                  <div className="text-xs text-agent-text-secondary font-semibold mb-1">
                    📊 {event.table}
                  </div>
                )}
                {(event.policy?.retention || event.retention) && (
                  <div className="text-xs text-agent-text-secondary">Retention: <strong>{event.policy?.retention || event.retention}</strong></div>
                )}
                {(event.policy?.access_level || event.access_level) && (
                  <div className="text-xs text-agent-text-secondary">Access Level: <strong>{event.policy?.access_level || event.access_level}</strong></div>
                )}
                {event.detail && <div className="text-xs text-agent-text-secondary mt-1">{event.detail}</div>}
                {event.message && <div className="text-xs text-agent-text-secondary mt-1 italic">{event.message}</div>}
              </div>
            )}

            {event.type === 'step' && (
              <div className="flex gap-2 text-sm">
                <div className="flex-shrink-0 mt-0.5">
                  {event.status === 'running' ? (
                    <Clock size={14} className="text-agent-orange animate-spin" />
                  ) : (
                    <CheckCircle2 size={14} className="text-green-400" />
                  )}
                </div>
                <p className="text-agent-text-primary">
                  {event.name}
                  <span className="text-agent-text-secondary ml-2">
                    {event.status === 'running' ? 'running...' : 'complete'}
                  </span>
                </p>
              </div>
            )}

            {event.type === 'complete' && (
              <div className="flex gap-2 text-sm">
                <CheckCircle2 size={14} className="text-green-400 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-green-400 font-semibold">✅ {event.summary}</p>
                </div>
              </div>
            )}

            {event.type === 'error' && (
              <div className="flex gap-2 text-sm">
                <AlertCircle size={14} className="text-red-400 flex-shrink-0 mt-0.5" />
                <p className="text-red-300">{event.message}</p>
              </div>
            )}

            {event.type === 'action' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 text-xs font-mono text-agent-text-secondary mb-2">
                <div className="text-agent-orange">→ {event.name}</div>
                {event.input && <div className="mt-1">{JSON.stringify(event.input, null, 2)}</div>}
              </div>
            )}

            {event.type === 'generated_code' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2">
                <div className="text-xs font-semibold text-agent-orange mb-2">{event.engine.toUpperCase()}</div>
                <pre className="text-[10px] text-agent-text-secondary overflow-x-auto max-h-40">
                  {typeof event.code === 'string' ? event.code : JSON.stringify(event.code, null, 2)}
                </pre>
              </div>
            )}

            {event.type === 'scores' && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2">
                <div className="text-xs font-semibold text-agent-orange mb-2">Quality Scores</div>
                {Object.entries(event.data || {}).map(([col, score]: [string, any]) => (
                  <div key={col} className="flex justify-between items-center mb-1 text-xs text-agent-text-secondary">
                    <span>{col}</span>
                    <div className="flex items-center gap-2">
                      <div className="w-20 h-1.5 bg-agent-dark-bg rounded-full overflow-hidden">
                        <div
                          className="h-full bg-agent-orange"
                          style={{ width: `${score.score}%` }}
                        />
                      </div>
                      <span className="w-8 text-right">{score.score.toFixed(0)}%</span>
                    </div>
                  </div>
                ))}
              </div>
            )}

            {['violations_detected', 'conflicts_detected', 'suggested_metrics', 'pii_detected', 'owner_assigned'].includes(event.type) && (
              <div className="bg-agent-dark-border rounded-agent-md p-3 mb-2">
                <div className="text-xs font-semibold text-agent-orange mb-2">{event.type.replace('_', ' ').toUpperCase()}</div>
                {event.count && <div className="text-xs text-agent-text-secondary mb-1">Count: {event.count}</div>}
                {event.owner && <div className="text-xs text-agent-text-secondary">Owner: {event.owner}</div>}
                {Array.isArray(event.items) && (
                  <div className="space-y-1">
                    {event.items.slice(0, 5).map((item: any, i: number) => (
                      <div key={i} className="text-xs text-agent-text-secondary">
                        • {typeof item === 'string' ? item : JSON.stringify(item).slice(0, 100)}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        ))}

        {running && (
          <div className="flex items-center gap-2 text-agent-text-secondary animate-pulse">
            <div className="w-2 h-2 bg-agent-orange rounded-full animate-bounce" />
            <span className="text-sm">Processing...</span>
          </div>
        )}

        <div ref={eventsEndRef} />
      </div>
    </div>
  );
}
