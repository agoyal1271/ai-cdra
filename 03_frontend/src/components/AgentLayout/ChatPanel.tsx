import { useState, useRef, useEffect } from 'react';
import { Send, Zap, Copy, Check } from 'lucide-react';

interface Message {
  id: string;
  role: 'user' | 'agent';
  content: string;
}

interface ChatPanelProps {
  messages: Message[];
}

export function ChatPanel({ messages }: ChatPanelProps) {
  const [inputValue, setInputValue] = useState('');
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSendMessage = () => {
    if (!inputValue.trim()) return;
    // Message handling would go here
    setInputValue('');
  };

  const handleCopy = (id: string, content: string) => {
    navigator.clipboard.writeText(content);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      handleSendMessage();
    }
  };

  return (
    <div className="flex-1 flex flex-col bg-agent-dark-bg">
      {/* Header */}
      <div className="h-16 px-6 flex items-center border-b border-agent-dark-border">
        <h2 className="text-lg font-semibold text-agent-text-primary">Agent Chat</h2>
        <span className="ml-auto text-xs text-agent-text-secondary px-3 py-1 bg-agent-dark-surface rounded-agent-md border border-agent-dark-border">
          {messages.length} messages
        </span>
      </div>

      {/* Messages Container */}
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {messages.length === 0 ? (
          <div className="h-full flex items-center justify-center">
            <div className="text-center">
              <Zap size={32} className="mx-auto mb-3 text-agent-orange opacity-60" />
              <p className="text-agent-text-secondary text-sm">Start a conversation with your AI Agent</p>
            </div>
          </div>
        ) : (
          messages.map(message => (
            <div
              key={message.id}
              className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={`max-w-md lg:max-w-lg px-4 py-3 rounded-agent-lg ${
                  message.role === 'user'
                    ? 'bg-agent-orange text-white'
                    : 'bg-agent-dark-surface border border-agent-dark-border text-agent-text-primary'
                }`}
              >
                <p className="text-sm leading-relaxed break-words">{message.content}</p>

                {message.role === 'agent' && (
                  <button
                    onClick={() => handleCopy(message.id, message.content)}
                    className="mt-2 text-xs opacity-70 hover:opacity-100 flex items-center gap-1 transition-opacity"
                  >
                    {copiedId === message.id ? (
                      <>
                        <Check size={12} /> Copied
                      </>
                    ) : (
                      <>
                        <Copy size={12} /> Copy
                      </>
                    )}
                  </button>
                )}
              </div>
            </div>
          ))
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input Area */}
      <div className="h-24 px-6 py-4 border-t border-agent-dark-border bg-agent-dark-bg flex flex-col gap-3">
        <textarea
          value={inputValue}
          onChange={e => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask me to discover data, analyze quality, or run checks... (Ctrl+Enter to send)"
          className={`
            flex-1 px-4 py-3 bg-agent-dark-surface border border-agent-dark-border rounded-agent-md
            text-agent-text-primary placeholder-agent-text-secondary text-sm resize-none
            focus:outline-none focus:border-agent-orange focus:ring-1 focus:ring-agent-orange/30
            transition-colors
          `}
        />
        <div className="flex gap-2">
          <button
            onClick={handleSendMessage}
            disabled={!inputValue.trim()}
            className={`
              flex items-center gap-2 px-4 py-2 rounded-agent-md font-medium text-sm
              transition-all
              ${
                inputValue.trim()
                  ? 'bg-agent-orange text-white hover:bg-orange-600 active:scale-95'
                  : 'bg-agent-dark-border text-agent-text-secondary opacity-40 cursor-not-allowed'
              }
            `}
          >
            <Send size={16} />
            Send
          </button>
          <button className="px-4 py-2 rounded-agent-md border border-agent-dark-border hover:border-agent-orange text-agent-text-secondary hover:text-agent-orange text-sm transition-colors">
            Clear
          </button>
        </div>
      </div>
    </div>
  );
}
