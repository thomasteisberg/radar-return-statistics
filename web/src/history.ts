import { IcechunkStore } from "icechunk-js";
import { STORE_URL } from "./config";

export interface CommitEntry {
  id: string;
  parentId: string | null;
  message: string;
  timestamp: string;
  parsedDate: Date | null;
}

function parseIcechunkDate(iso: string): Date | null {
  // icechunk-js bug: flushedAt is stored as microseconds since Unix epoch
  // but passed directly to new Date() which expects milliseconds, giving
  // dates in year ~58000. Fix: divide getTime() by 1000.
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  if (d.getFullYear() <= 3000) return d;
  return new Date(d.getTime() / 1000);
}

// Icechunk uses this as the parent of the very first commit
const INITIAL_COMMIT_SENTINEL = "1CECHNKREP0F1RSTCMT0";

export async function getCommitLog(): Promise<CommitEntry[]> {
  const log: CommitEntry[] = [];
  let ref: string | undefined = "main";
  let snapshotId: string | undefined;
  const maxDepth = 100; // safety limit

  for (let i = 0; i < maxDepth; i++) {
    try {
      const store = await IcechunkStore.open(
        STORE_URL,
        snapshotId ? { snapshot: snapshotId } : { ref: ref! }
      );
      const snapshot = store.getSnapshot();
      if (!snapshot) break;
      log.push({
        id: snapshot.id,
        parentId: snapshot.parentId ?? null,
        message: snapshot.message,
        timestamp: snapshot.flushedAt,
        parsedDate: parseIcechunkDate(snapshot.flushedAt),
      });
      if (
        !snapshot.parentId ||
        snapshot.parentId === INITIAL_COMMIT_SENTINEL
      ) {
        break;
      }
      snapshotId = snapshot.parentId;
      ref = undefined;
    } catch {
      break;
    }
  }
  return log;
}

export function formatTimestamp(iso: string): string {
  const d = new Date(iso);
  // Guard against bogus timestamps from icechunk
  if (isNaN(d.getTime()) || d.getFullYear() > 3000) {
    return "unknown time";
  }
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
