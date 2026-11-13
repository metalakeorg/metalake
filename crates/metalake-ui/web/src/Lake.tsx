import { useQuery } from "@tanstack/react-query";
import { Failure, type Status, status } from "./client";
import { Notice, where } from "./Frame";

/** The query of what the lake is, which the frame's header and the Lake page share. */
export const lakeQuery = { queryKey: ["status"], queryFn: () => status() } as const;

/** The Lake page: what the lake is. */
export function LakePage() {
  const lake = useQuery(lakeQuery);
  if (lake.error) return <Failed error={lake.error} />;
  if (!lake.data) return null;
  return <LakeView lake={lake.data} />;
}

export function LakeView({ lake }: { lake: Status }) {
  const rows: [string, string][] = [
    ["Catalog", where(lake)],
    ["Schema", lake.schema],
    ["Warehouse", lake.warehouse ?? "none"],
    ["Format", lake.format_version],
    ["Writer", lake.writer_version],
    ["Protocol", lake.protocol_version ?? "none"],
  ];
  return (
    <section className="max-w-3xl p-6">
      <h1 className="font-semibold text-lg">Lake</h1>
      <dl className="mt-4 grid grid-cols-[8rem_1fr] gap-x-4 gap-y-2 text-sm">
        {rows.map(([term, value]) => (
          <div key={term} className="contents">
            <dt className="text-zinc-500 dark:text-zinc-400">{term}</dt>
            <dd className="break-all font-mono">{value}</dd>
          </div>
        ))}
      </dl>
      <h2 className="mt-8 font-semibold">Refs</h2>
      <ul className="mt-2 space-y-1 text-sm" aria-label="Refs">
        {lake.refs.map((ref) => (
          <li key={ref.name} className="flex items-center gap-2">
            <span className="font-mono">{ref.name}</span>
            <span className="rounded bg-zinc-100 px-1.5 text-xs text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
              {ref.kind}
            </span>
          </li>
        ))}
      </ul>
    </section>
  );
}

/** A request that failed, said as the command line says it. */
export function Failed({ error }: { error: Error }) {
  const kind = error instanceof Failure ? error.fault.kind : "failed";
  return (
    <Notice title={`The lake could not be read (${kind})`}>
      <code>metalake: {error.message}</code>
    </Notice>
  );
}
