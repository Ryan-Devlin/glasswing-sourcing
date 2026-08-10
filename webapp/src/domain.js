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
