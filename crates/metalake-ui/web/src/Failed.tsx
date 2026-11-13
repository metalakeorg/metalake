import { Failure } from "./client";
import { Notice } from "./Frame";

/** A request that failed, said as the command line says it. */
export function Failed({ title, error }: { title: string; error: Error }) {
  const kind = error instanceof Failure ? error.fault.kind : "failed";
  return (
    <Notice title={`${title} (${kind})`}>
      <code>metalake: {error.message}</code>
    </Notice>
  );
}
