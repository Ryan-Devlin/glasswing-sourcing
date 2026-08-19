export const formatCount = (value) => Number(value || 0).toLocaleString();

export function pageCount(total, pageSize = 100) {
  return Math.max(1, Math.ceil(Number(total || 0) / pageSize));
}

export function plainSnippet(value) {
  return String(value || "").replace(/<\/?mark>/gi, "");
}

export function dealAgeLabel(value, now = new Date()) {
  const observed = new Date(value);
  if (!value || Number.isNaN(observed.getTime())) return "Age unknown";
  const days = Math.max(0, Math.floor((now.getTime() - observed.getTime()) / 86400000));
  if (days === 0) return "Today";
  return `${days} day${days === 1 ? "" : "s"} old`;
}

/** "Today", "Yesterday", "3 days ago" -- how long ago a record arrived.
 *
 * Dates a record by the first timestamp that means "when we found this",
 * falling back through the ones different tables happen to carry. updated_at
 * is last on purpose: every re-screen refreshes it, so leading with it dates
 * every long-standing record as today.
 */
export function addedAtValue(row) {
  if (!row) return "";
  return (
    row.first_seen ||
    row.classified_at ||
    row.labeled_at ||
    row.detected_at ||
    row.created_at ||
    row.requested_at ||
    row.started_at ||
    row.updated_at ||
    ""
  );
}

export function daysAgoLabel(value, now = new Date()) {
  if (!value) return "—";
  const observed = new Date(value);
  if (Number.isNaN(observed.getTime())) return "—";
  // Calendar days, not elapsed 24-hour periods: something added at 11pm
  // yesterday reads "Yesterday" this morning rather than "Today", which is
  // what anyone comparing the column against a date expects.
  const startOfToday = new Date(
    now.getFullYear(), now.getMonth(), now.getDate(),
  ).getTime();
  const startOfThen = new Date(
    observed.getFullYear(), observed.getMonth(), observed.getDate(),
  ).getTime();
  const days = Math.round((startOfToday - startOfThen) / 86400000);
  if (days <= 0) return "Today";
  if (days === 1) return "Yesterday";
  return `${days} days ago`;
}
