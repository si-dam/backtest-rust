export function isoDateHoursAgo(hours: number): string {
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
}

export function toBusinessDayOrTimestamp(isoTs: string): { time: number } {
  const t = Date.parse(isoTs);
  return { time: Number.isFinite(t) ? Math.floor(t / 1000) : 0 };
}
