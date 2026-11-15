// The icons of the tree (Lucide, ISC): a catalog, a schema, a table, and a column by the family of its type, of the
// types MetaLake reads and writes (Iceberg's: `crates/metalake/src/schema.rs`).

import {
  Binary,
  Braces,
  Brackets,
  Calendar,
  CalendarClock,
  CaseSensitive,
  CircleDashed,
  Clock,
  Database,
  DecimalsArrowRight,
  FingerprintPattern,
  Folder,
  Hash,
  KeyRound,
  type LucideIcon,
  Table2,
  ToggleLeft,
} from "lucide-react";

const CLASS = "size-3.5 shrink-0 text-zinc-500 dark:text-zinc-400";

function shown(Icon: LucideIcon) {
  return <Icon aria-hidden strokeWidth={1.75} className={CLASS} />;
}

export const CatalogIcon = () => shown(Database);
export const SchemaIcon = () => shown(Folder);
export const TableIcon = () => shown(Table2);

/** A type's family, by its name as the lake says it: `long`, `decimal(10,2)`, `fixed[16]`, `timestamptz_ns`… */
export function family(type: string): LucideIcon {
  if (type === "int" || type === "long") return Hash;
  if (type === "float" || type === "double" || type.startsWith("decimal")) return DecimalsArrowRight;
  if (type === "string") return CaseSensitive;
  if (type === "boolean") return ToggleLeft;
  if (type === "date") return Calendar;
  if (type === "time") return Clock;
  if (type.startsWith("timestamp")) return CalendarClock;
  if (type === "uuid") return FingerprintPattern;
  if (type === "binary" || type.startsWith("fixed")) return Binary;
  if (type.startsWith("struct")) return Braces;
  if (type.startsWith("list")) return Brackets;
  if (type.startsWith("map")) return KeyRound;
  return CircleDashed;
}

/** A column's icon, by its type's family. */
export function TypeIcon({ type }: { type: string }) {
  const Icon = family(type);
  return <Icon aria-hidden data-type={type} strokeWidth={1.75} className={CLASS} />;
}
