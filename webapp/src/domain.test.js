import { describe, expect, it } from "vitest";
import { dealAgeLabel, formatCount, pageCount, plainSnippet } from "./domain";

describe("table display helpers", () => {
  it("formats counts and page totals", () => {
    expect(formatCount(41308)).toMatch(/41.308|41,308/);
    expect(pageCount(41308, 100)).toBe(414);
    expect(pageCount(0, 100)).toBe(1);
  });

  it("renders crawler highlights as plain safe text", () => {
    expect(plainSnippet("A <mark>robotics</mark> company")).toBe(
      "A robotics company",
    );
  });

  it("formats deal age from its first classification time", () => {
    const now = new Date("2026-08-10T20:00:00Z");
    expect(dealAgeLabel("2026-08-10T02:00:00Z", now)).toBe("Today");
    expect(dealAgeLabel("2026-08-09T01:00:00Z", now)).toBe("1 day old");
    expect(dealAgeLabel("2026-08-01T20:00:00Z", now)).toBe("9 days old");
    expect(dealAgeLabel("", now)).toBe("Age unknown");
  });
});
