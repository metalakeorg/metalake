import {
  Binary,
  Braces,
  Brackets,
  Calendar,
  CalendarClock,
  CaseSensitive,
  CircleDashed,
  Clock,
  DecimalsArrowRight,
  FingerprintPattern,
  Hash,
  KeyRound,
  ToggleLeft,
} from "lucide-react";
import { describe, expect, it } from "vitest";
import { family } from "./icons";

describe("family", () => {
  it("gives each type the lake reads and writes its family's icon", () => {
    const cases: [string, unknown][] = [
      ["int", Hash],
      ["long", Hash],
      ["float", DecimalsArrowRight],
      ["double", DecimalsArrowRight],
      ["decimal(10,2)", DecimalsArrowRight],
      ["string", CaseSensitive],
      ["boolean", ToggleLeft],
      ["date", Calendar],
      ["time", Clock],
      ["timestamp", CalendarClock],
      ["timestamptz", CalendarClock],
      ["timestamp_ns", CalendarClock],
      ["timestamptz_ns", CalendarClock],
      ["uuid", FingerprintPattern],
      ["binary", Binary],
      ["fixed[16]", Binary],
      ["struct", Braces],
      ["list", Brackets],
      ["map", KeyRound],
      ["unknown", CircleDashed],
    ];
    for (const [type, icon] of cases) expect([type, family(type)]).toEqual([type, icon]);
  });
});
