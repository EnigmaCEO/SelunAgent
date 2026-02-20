import { EXECUTION_MODEL_VERSION } from "../config";

export type ExecutionStatus = "started" | "pending" | "success" | "error";

export type ExecutionLog = {
  phase: string;
  action: string;
  status: ExecutionStatus;
  transactionHash: string | null;
  timestamp: string;
  executionModelVersion: typeof EXECUTION_MODEL_VERSION;
};

const MAX_LOGS = 500;
const executionLogs: ExecutionLog[] = [];

export function emitExecutionLog(log: Omit<ExecutionLog, "timestamp" | "executionModelVersion">): ExecutionLog {
  const payload: ExecutionLog = {
    ...log,
    timestamp: new Date().toISOString(),
    executionModelVersion: EXECUTION_MODEL_VERSION,
  };

  executionLogs.push(payload);
  if (executionLogs.length > MAX_LOGS) {
    executionLogs.shift();
  }

  console.log(JSON.stringify(payload));
  return payload;
}

export function getExecutionLogs(limit = 100): ExecutionLog[] {
  if (limit <= 0) return [];
  return executionLogs.slice(-limit);
}
