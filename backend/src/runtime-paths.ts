import fs from "node:fs";
import path from "node:path";

let cachedBackendRootDir: string | null = null;

export function resolveBackendRootDir(): string {
  if (cachedBackendRootDir) return cachedBackendRootDir;

  const cwd = process.cwd();
  const cwdBase = path.basename(cwd).toLowerCase();

  if (cwdBase === "backend") {
    cachedBackendRootDir = cwd;
    return cachedBackendRootDir;
  }

  const backendFromRepoRoot = path.join(cwd, "backend");
  if (fs.existsSync(backendFromRepoRoot) && fs.statSync(backendFromRepoRoot).isDirectory()) {
    cachedBackendRootDir = backendFromRepoRoot;
    return cachedBackendRootDir;
  }

  cachedBackendRootDir = cwd;
  return cachedBackendRootDir;
}

export function resolveBackendPath(...parts: string[]): string {
  return path.join(resolveBackendRootDir(), ...parts);
}

function syncLegacyDataFile(canonicalPath: string, legacyPath: string) {
  if (!fs.existsSync(legacyPath)) return;

  const shouldSync =
    !fs.existsSync(canonicalPath) ||
    fs.statSync(legacyPath).mtimeMs > fs.statSync(canonicalPath).mtimeMs;

  if (!shouldSync) return;

  fs.mkdirSync(path.dirname(canonicalPath), { recursive: true });
  fs.copyFileSync(legacyPath, canonicalPath);
}

export function resolveBackendDataFilePath(filename: string): string {
  const backendRoot = resolveBackendRootDir();
  const canonicalPath = path.join(backendRoot, "data", filename);
  const legacyNestedPath = path.join(backendRoot, "backend", "data", filename);

  try {
    syncLegacyDataFile(canonicalPath, legacyNestedPath);
  } catch {
    // If sync fails, continue with canonical path resolution.
  }

  return canonicalPath;
}
