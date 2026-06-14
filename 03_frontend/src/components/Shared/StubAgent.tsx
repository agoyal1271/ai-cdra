interface Props {
  name: string;
  tagline: string;
  description: string;
  icon: string;
  phase: string;
}

export function StubAgent({ name, tagline, description, icon, phase }: Props) {
  return (
    <div className="flex flex-col items-center justify-center h-full gap-6 p-8 text-center">
      <span className="text-6xl opacity-40">{icon}</span>
      <div>
        <div className="text-xs text-orange-400 font-semibold uppercase tracking-wider">{tagline}</div>
        <h2 className="text-2xl font-bold text-slate-300 mt-1">{name}</h2>
      </div>
      <p className="text-slate-500 max-w-md leading-relaxed">{description}</p>
      <div className="bg-slate-800 border border-slate-700 rounded-xl px-6 py-3">
        <span className="text-slate-400 text-sm">Shipping in </span>
        <span className="text-orange-400 font-bold text-sm">{phase}</span>
      </div>
    </div>
  );
}
