import { Repository, HttpStorage, encodeObjectId12 } from "@carbonplan/icechunk-js";
import { STORE_URL } from "./config";

export interface CommitEntry {
  id: string;
  parentId: Uint8Array | null;
  message: string;
  date: Date;
}

const INITIAL_COMMIT_SENTINEL = "1CECHNKREP0F1RSTCMT0";

export async function getCommitLog(): Promise<CommitEntry[]> {
  const storage = new HttpStorage(STORE_URL);
  const repo = await Repository.open({ storage });
  let session = await repo.checkoutBranch("main");
  const log: CommitEntry[] = [];
  const maxDepth = 100;

  for (let i = 0; i < maxDepth; i++) {
    const id = encodeObjectId12(session.getSnapshotId());
    const date = session.getFlushedAt();
    const message = session.snapshot.message;
    const parentId: Uint8Array | null = (session as any).snapshot.parentId ?? null;

    log.push({ id, parentId, message, date });

    if (!parentId) break;
    if (encodeObjectId12(parentId) === INITIAL_COMMIT_SENTINEL) break;

    try {
      session = await repo.checkoutSnapshot(parentId);
    } catch {
      break;
    }
  }

  return log;
}

export function formatDate(date: Date): string {
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
