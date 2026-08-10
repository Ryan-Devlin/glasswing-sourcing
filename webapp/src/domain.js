export const formatCount = (value) => Number(value || 0).toLocaleString();

export function pageCount(total, pageSize = 100) {
  return Math.max(1, Math.ceil(Number(total || 0) / pageSize));
}

export function plainSnippet(value) {
  return String(value || "").replace(/<\/?mark>/gi, "");
}
