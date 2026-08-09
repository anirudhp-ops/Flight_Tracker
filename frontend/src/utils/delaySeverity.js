// Color-blind-safe palette (task 13): delay severity is conveyed by shape/
// label everywhere it's shown, and these colors are chosen to stay
// distinguishable under the common red-green deficiencies (deuteranopia/
// protanopia) — blue for "fine," amber for "moderate," red-orange for
// "severe," rather than a pure red/green pair.
export const SEVERITY = {
  NONE: { key: "none", color: "#58a6ff", label: "On time" },
  LOW: { key: "low", color: "#d29922", label: "Minor delay" },
  MODERATE: { key: "moderate", color: "#e8871e", label: "Moderate delay" },
  HIGH: { key: "high", color: "#f85149", label: "Severe delay" },
};

/** 0 -> none, 1-10 -> low, 10-30 -> moderate, 30+ -> high (task 4's bands). */
export function delaySeverity(delayMinutes) {
  const m = delayMinutes || 0;
  if (m <= 0) return SEVERITY.NONE;
  if (m <= 10) return SEVERITY.LOW;
  if (m <= 30) return SEVERITY.MODERATE;
  return SEVERITY.HIGH;
}

export function confidenceLabel(confidence) {
  if (confidence == null) return { label: "unknown", key: "unknown" };
  if (confidence >= 0.7) return { label: "high confidence", key: "high" };
  if (confidence >= 0.4) return { label: "moderate confidence", key: "moderate" };
  return { label: "low confidence", key: "low" };
}
