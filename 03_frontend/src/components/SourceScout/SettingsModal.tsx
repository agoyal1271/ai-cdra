import React from 'react';
import { X } from 'lucide-react';
import { SettingsPanel } from './SettingsPanel';

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export const SettingsModal: React.FC<SettingsModalProps> = ({ isOpen, onClose }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 backdrop-blur-sm">
      <div className="bg-agent-dark-surface rounded-lg shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto border border-agent-dark-border">
        <div className="flex items-center justify-between p-6 border-b border-agent-dark-border sticky top-0 bg-agent-dark-surface">
          <h1 className="text-xl font-bold text-agent-text-primary">Configuration</h1>
          <button
            onClick={onClose}
            className="p-1 hover:bg-agent-dark-border rounded-lg transition-colors text-agent-text-secondary hover:text-agent-text-primary"
            title="Close"
          >
            <X size={20} />
          </button>
        </div>
        <SettingsPanel />
      </div>
    </div>
  );
};
