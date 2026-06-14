import { useState, useEffect } from 'react';

interface KnoxStatus {
  configured: boolean;
  host?: string;
}

export function useKnoxStatus() {
  const [status, setStatus] = useState<KnoxStatus>({ configured: false });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const checkKnox = async () => {
      try {
        const res = await fetch('/api/system/knox-status');
        const data = await res.json();
        setStatus(data);
      } catch (err) {
        console.warn('Could not check Knox status:', err);
        setStatus({ configured: false });
      } finally {
        setLoading(false);
      }
    };

    checkKnox();
  }, []);

  return { ...status, loading };
}
