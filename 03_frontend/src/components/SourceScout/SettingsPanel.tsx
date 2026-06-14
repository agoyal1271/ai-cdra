import React, { useState, useEffect } from 'react';
import { AlertCircle, CheckCircle, Loader2, Key } from 'lucide-react';

interface LLMSettings {
  llm_provider: string;
  llm_model: string;
  openai_api_key?: string;
  anthropic_api_key?: string;
  azure_openai_key?: string;
  azure_openai_endpoint?: string;
  azure_openai_deployment?: string;
  ollama_url?: string;
}

interface TestResult {
  status: string;
  provider?: string;
  model?: string;
  embedding_dim?: number;
  message?: string;
}

const PROVIDERS = [
  { id: 'ollama', name: 'Ollama (Local)', description: 'Fast local model via HTTP' },
  { id: 'openai', name: 'OpenAI', description: 'ChatGPT & GPT-4 embeddings' },
  { id: 'anthropic', name: 'Anthropic', description: 'Claude models' },
  { id: 'azure_openai', name: 'Azure OpenAI', description: 'OpenAI hosted on Azure' },
];

export const SettingsPanel: React.FC = () => {
  const [settings, setSettings] = useState<LLMSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingModels, setLoadingModels] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const currentProvider = PROVIDERS.find(p => p.id === settings?.llm_provider);

  useEffect(() => {
    // Only fetch settings immediately (non-blocking)
    fetchSettings();
  }, []);

  useEffect(() => {
    // Fetch available models only after settings are loaded
    // and only for specific providers that support it
    if (settings?.llm_provider && ['ollama', 'openai', 'anthropic', 'azure_openai'].includes(settings.llm_provider)) {
      // Debounce model fetching - only fetch after a short delay
      const timer = setTimeout(() => {
        fetchAvailableModels();
      }, 500);
      return () => clearTimeout(timer);
    }
  }, [settings?.llm_provider]);

  const fetchSettings = async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await fetch('/api/settings');

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();

      if (data.status === 'ok') {
        setSettings(data.settings as LLMSettings);
      } else {
        throw new Error(data.message || 'Unknown error loading settings');
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      setError(`Failed to load settings: ${errorMsg}. Make sure backend is running on http://localhost:8000`);
      console.error('Settings fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchAvailableModels = async () => {
    try {
      setLoadingModels(true);

      // Add a 5-second timeout to avoid hanging
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const response = await fetch('/api/llm/available-models', {
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();

      if (data.status === 'ok') {
        setAvailableModels(data.models || []);
      } else {
        console.warn('Failed to fetch available models:', data.message);
        setAvailableModels([]);
      }
    } catch (err) {
      // Silently fail - models are optional
      console.warn('Failed to fetch available models (this is ok):', err);
      setAvailableModels([]);
    } finally {
      setLoadingModels(false);
    }
  };

  const handleSettingChange = async (key: string, value: string) => {
    try {
      setSaving(true);
      setError(null);
      setSuccess(null);

      const response = await fetch(`/api/settings/${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value }),
      });

      const data = await response.json();

      if (data.status === 'ok' || data.status === 'warning') {
        setSettings(prev => prev ? { ...prev, [key]: value } : null);
        setSuccess(`Updated ${key}`);
      } else {
        setError(data.message || 'Failed to update setting');
      }
    } catch (err) {
      setError(`Error updating setting: ${err}`);
    } finally {
      setSaving(false);
    }
  };

  const handleTestConnection = async () => {
    try {
      setTesting(true);
      setTestResult(null);

      const response = await fetch('/api/llm/test', { method: 'POST' });
      const data = await response.json();

      setTestResult(data);

      if (data.status === 'ok') {
        setSuccess('Connection successful!');
      } else {
        setError(data.message || 'Connection test failed');
      }
    } catch (err) {
      setError(`Error testing connection: ${err}`);
    } finally {
      setTesting(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center p-8 bg-agent-dark-surface">
        <Loader2 className="w-6 h-6 animate-spin text-cloudera" />
        <span className="ml-2 text-agent-text-secondary">Loading settings...</span>
      </div>
    );
  }

  if (!settings) {
    return (
      <div className="p-6 bg-agent-dark-bg border border-red-800 rounded-lg m-6">
        <p className="text-red-400">Failed to load settings</p>
      </div>
    );
  }

  return (
    <div className="space-y-6 p-6 max-w-2xl bg-agent-dark-surface">
      <h2 className="text-2xl font-bold text-agent-text-primary">LLM Settings</h2>

      {error && (
        <div className="flex items-start gap-3 p-4 bg-red-900/20 border border-red-700 rounded-lg">
          <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <p className="text-red-300">{error}</p>
        </div>
      )}

      {success && (
        <div className="flex items-start gap-3 p-4 bg-green-900/20 border border-green-700 rounded-lg">
          <CheckCircle className="w-5 h-5 text-green-500 flex-shrink-0 mt-0.5" />
          <p className="text-green-300">{success}</p>
        </div>
      )}

      {testResult && testResult.status !== 'ok' && (
        <div className="p-4 bg-amber-900/20 border border-amber-700 rounded-lg">
          <p className="text-amber-300 font-medium">Connection Test Failed</p>
          <p className="text-amber-200 text-sm mt-1">{testResult.message}</p>
        </div>
      )}

      {testResult && testResult.status === 'ok' && (
        <div className="p-4 bg-emerald-900/20 border border-emerald-700 rounded-lg">
          <p className="text-emerald-300 font-medium">Connection Successful</p>
          <p className="text-emerald-200 text-sm mt-1">
            Provider: {testResult.provider} | Model: {testResult.model} | Embedding Dim: {testResult.embedding_dim}
          </p>
        </div>
      )}

      {/* Provider Selection */}
      <div className="space-y-3">
        <label className="block text-sm font-semibold text-agent-text-primary">LLM Provider</label>
        <div className="grid grid-cols-1 gap-3">
          {PROVIDERS.map(provider => (
            <div
              key={provider.id}
              className={`p-4 border-2 rounded-lg cursor-pointer transition ${
                settings.llm_provider === provider.id
                  ? 'border-cloudera bg-agent-dark-bg'
                  : 'border-agent-dark-border bg-agent-dark-bg hover:border-cloudera'
              }`}
              onClick={() => handleSettingChange('llm_provider', provider.id)}
            >
              <p className="font-medium text-agent-text-primary">{provider.name}</p>
              <p className="text-sm text-agent-text-secondary">{provider.description}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Provider-specific settings */}
      <div className="space-y-4 border-t border-agent-dark-border pt-6">
        {currentProvider?.id === 'ollama' && (
          <div>
            <label className="block text-sm font-medium text-agent-text-primary mb-2">Ollama URL</label>
            <input
              type="text"
              defaultValue={settings.ollama_url || 'http://localhost:11434'}
              onBlur={e => handleSettingChange('ollama_url', e.target.value)}
              className="w-full px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
              placeholder="http://localhost:11434"
            />
          </div>
        )}

        {currentProvider?.id === 'openai' && (
          <div>
            <label className="block text-sm font-medium text-agent-text-primary mb-2">OpenAI API Key</label>
            <div className="relative">
              <Key className="absolute left-3 top-3 w-5 h-5 text-agent-text-secondary" />
              <input
                type="password"
                placeholder="sk-..."
                onBlur={e => handleSettingChange('openai_api_key', e.target.value)}
                defaultValue={settings.openai_api_key ? '***' : ''}
                className="w-full pl-10 pr-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
              />
            </div>
          </div>
        )}

        {currentProvider?.id === 'anthropic' && (
          <div>
            <label className="block text-sm font-medium text-agent-text-primary mb-2">Anthropic API Key</label>
            <div className="relative">
              <Key className="absolute left-3 top-3 w-5 h-5 text-agent-text-secondary" />
              <input
                type="password"
                placeholder="sk-ant-..."
                onBlur={e => handleSettingChange('anthropic_api_key', e.target.value)}
                defaultValue={settings.anthropic_api_key ? '***' : ''}
                className="w-full pl-10 pr-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
              />
            </div>
          </div>
        )}

        {currentProvider?.id === 'azure_openai' && (
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-agent-text-primary mb-2">Azure OpenAI API Key</label>
              <div className="relative">
                <Key className="absolute left-3 top-3 w-5 h-5 text-agent-text-secondary" />
                <input
                  type="password"
                  placeholder="Your API key"
                  onBlur={e => handleSettingChange('azure_openai_key', e.target.value)}
                  defaultValue={settings.azure_openai_key ? '***' : ''}
                  className="w-full pl-10 pr-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
                />
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-agent-text-primary mb-2">Endpoint URL</label>
              <input
                type="text"
                defaultValue={settings.azure_openai_endpoint || ''}
                onBlur={e => handleSettingChange('azure_openai_endpoint', e.target.value)}
                className="w-full px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
                placeholder="https://<resource>.openai.azure.com/"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-agent-text-primary mb-2">Deployment</label>
              <input
                type="text"
                defaultValue={settings.azure_openai_deployment || 'text-embedding-ada-002'}
                onBlur={e => handleSettingChange('azure_openai_deployment', e.target.value)}
                className="w-full px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary placeholder-agent-text-secondary focus:border-cloudera focus:ring-cloudera"
              />
            </div>
          </div>
        )}

        {/* Model Selection */}
        <div>
          <label className="block text-sm font-medium text-agent-text-primary mb-2">
            Model
            {loadingModels && <span className="text-xs text-agent-text-secondary ml-2">(loading...)</span>}
          </label>
          <select
            value={settings.llm_model || ''}
            onChange={e => handleSettingChange('llm_model', e.target.value)}
            disabled={loadingModels}
            className={`w-full px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-agent-text-primary focus:border-cloudera focus:ring-cloudera ${
              loadingModels ? 'opacity-50' : ''
            }`}
          >
            <option value="">
              {loadingModels ? 'Loading models...' : availableModels.length > 0 ? 'Select a model' : 'Manual entry'}
            </option>
            {availableModels.map(model => (
              <option key={model} value={model}>
                {model}
              </option>
            ))}
          </select>
          {availableModels.length === 0 && !loadingModels && (
            <p className="text-xs text-agent-text-secondary mt-1">Enter model name manually or wait for list to load</p>
          )}
        </div>
      </div>

      {/* Test Connection Button */}
      <div className="flex gap-2 pt-4 border-t border-agent-dark-border">
        <button
          onClick={handleTestConnection}
          disabled={testing || saving}
          className={`flex items-center gap-2 px-6 py-2 rounded-md font-medium transition ${
            testing || saving
              ? 'bg-agent-dark-border text-agent-text-secondary cursor-not-allowed'
              : 'bg-cloudera text-white hover:bg-cloudera-hover'
          }`}
        >
          {testing ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle className="w-4 h-4" />}
          Test Connection
        </button>
        {saving && (
          <span className="flex items-center gap-2 text-agent-text-secondary">
            <Loader2 className="w-4 h-4 animate-spin" />
            Saving...
          </span>
        )}
      </div>
    </div>
  );
};
