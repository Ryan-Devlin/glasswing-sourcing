import { describe, expect, it } from "vitest";
import { formatCount, pageCount, plainSnippet } from "./domain";

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
});
