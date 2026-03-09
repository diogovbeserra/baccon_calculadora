import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .map((arg) => arg.trim())
    .filter((arg) => arg.startsWith('--') && arg.includes('='))
    .map((arg) => {
      const [key, value] = arg.slice(2).split('=');
      return [key, value];
    }),
);

const toNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const config = {
  projectRoot: process.cwd(),
  syncScriptPath: path.resolve(args.syncScriptPath ?? process.env.DIVINE_PRIDE_SYNC_SCRIPT_PATH ?? 'scripts/sync-divine-pride-items.mjs'),
  mergeScriptPath: path.resolve(
    args.mergeScriptPath ?? process.env.DIVINE_PRIDE_MERGE_SCRIPT_PATH ?? 'scripts/merge-synced-items-into-original-item.mjs',
  ),
  runMergeAfterCycle: (args.runMergeAfterCycle ?? process.env.DIVINE_PRIDE_AUTO_RUN_MERGE_AFTER_CYCLE ?? 'true').toLowerCase() !== 'false',
  outputDir: path.resolve(args.outputDir ?? process.env.DIVINE_PRIDE_OUTPUT_DIR ?? 'data'),
  endId: toNumber(args.endId ?? process.env.DIVINE_PRIDE_END_ID, 200000),
  waitAfterBlockMs: Math.max(60_000, toNumber(args.waitAfterBlockMs ?? process.env.DIVINE_PRIDE_AUTO_WAIT_AFTER_BLOCK_MS, 3_700_000)),
  maxCycles: Math.max(0, toNumber(args.maxCycles ?? process.env.DIVINE_PRIDE_AUTO_MAX_CYCLES, 0)),
  maxNoProgressCycles: Math.max(1, toNumber(args.maxNoProgressCycles ?? process.env.DIVINE_PRIDE_AUTO_MAX_NO_PROGRESS_CYCLES, 3)),
  quickRetryMs: Math.max(5_000, toNumber(args.quickRetryMs ?? process.env.DIVINE_PRIDE_AUTO_QUICK_RETRY_MS, 30_000)),
};

const statePath = path.join(config.outputDir, '.items-sync-state.json');
const rawPath = path.join(config.outputDir, 'items.raw.json');

const safeReadJson = async (filePath, fallback = null) => {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return JSON.parse(content);
  } catch {
    return fallback;
  }
};

const readRawCount = async () => {
  const raw = await safeReadJson(rawPath, {});
  return raw && typeof raw === 'object' ? Object.keys(raw).length : 0;
};

const isCompleted = (state) => {
  if (!state) return false;
  const nextId = Number(state.nextId ?? 0);
  const pendingSize = Number(state.pendingSize ?? 0);
  const inFlightSize = Number(state.inFlightSize ?? 0);
  return nextId > config.endId && pendingSize === 0 && inFlightSize === 0;
};

const runSyncCycle = async () =>
  new Promise((resolve) => {
    const childEnv = {
      ...process.env,
      DIVINE_PRIDE_RESUME: process.env.DIVINE_PRIDE_RESUME ?? 'true',
      DIVINE_PRIDE_MAX_AUTH_COOLDOWN_CYCLES: process.env.DIVINE_PRIDE_MAX_AUTH_COOLDOWN_CYCLES ?? '1',
      DIVINE_PRIDE_OUTPUT_DIR: process.env.DIVINE_PRIDE_OUTPUT_DIR ?? config.outputDir,
      DIVINE_PRIDE_END_ID: process.env.DIVINE_PRIDE_END_ID ?? String(config.endId),
    };

    const child = spawn(process.execPath, [config.syncScriptPath], {
      cwd: config.projectRoot,
      env: childEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let authBlocked = false;
    const matchAuthBlock = (chunk) =>
      chunk.includes('too many auth errors') ||
      chunk.includes('auth kept failing') ||
      chunk.includes('401') ||
      chunk.includes('403');

    child.stdout.on('data', (buffer) => {
      const chunk = buffer.toString();
      process.stdout.write(chunk);
      if (matchAuthBlock(chunk)) authBlocked = true;
    });

    child.stderr.on('data', (buffer) => {
      const chunk = buffer.toString();
      process.stderr.write(chunk);
      if (matchAuthBlock(chunk)) authBlocked = true;
    });

    child.on('close', (code, signal) => {
      resolve({
        code: code ?? 0,
        signal: signal ?? null,
        authBlocked,
      });
    });
  });

const runMergeStep = async () =>
  new Promise((resolve) => {
    const child = spawn(process.execPath, [config.mergeScriptPath], {
      cwd: config.projectRoot,
      env: {
        ...process.env,
        DIVINE_PRIDE_RAW_PATH: process.env.DIVINE_PRIDE_RAW_PATH ?? path.join(config.outputDir, 'items.raw.json'),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    child.stdout.on('data', (buffer) => {
      process.stdout.write(buffer.toString());
    });

    child.stderr.on('data', (buffer) => {
      process.stderr.write(buffer.toString());
    });

    child.on('close', (code, signal) => {
      resolve({
        code: code ?? 0,
        signal: signal ?? null,
      });
    });
  });

const main = async () => {
  let cycle = 0;
  let noProgressCycles = 0;

  console.log('[auto-sync] configuration', {
    syncScriptPath: config.syncScriptPath,
    mergeScriptPath: config.mergeScriptPath,
    runMergeAfterCycle: config.runMergeAfterCycle,
    outputDir: config.outputDir,
    endId: config.endId,
    waitAfterBlockMs: config.waitAfterBlockMs,
    maxCycles: config.maxCycles,
    maxNoProgressCycles: config.maxNoProgressCycles,
    quickRetryMs: config.quickRetryMs,
  });

  while (true) {
    cycle += 1;
    if (config.maxCycles > 0 && cycle > config.maxCycles) {
      console.log(`[auto-sync] reached maxCycles=${config.maxCycles}. stopping.`);
      break;
    }

    const stateBefore = await safeReadJson(statePath, null);
    const rawCountBefore = await readRawCount();
    const nextBefore = Number(stateBefore?.nextId ?? 0);

    if (isCompleted(stateBefore)) {
      console.log('[auto-sync] dataset already complete for configured endId. nothing to do.');
      break;
    }

    console.log(
      `[auto-sync] cycle ${cycle} started at ${new Date().toISOString()} nextId=${nextBefore || 'unknown'} rawCount=${rawCountBefore}`,
    );

    const result = await runSyncCycle();

    if (config.runMergeAfterCycle) {
      const mergeResult = await runMergeStep();
      if (mergeResult.code !== 0) {
        console.error('[auto-sync] merge step failed', mergeResult);
        process.exit(1);
      }
    }

    const stateAfter = await safeReadJson(statePath, null);
    const rawCountAfter = await readRawCount();
    const nextAfter = Number(stateAfter?.nextId ?? 0);
    const progressed = rawCountAfter > rawCountBefore || (nextAfter > nextBefore && nextBefore > 0);

    console.log('[auto-sync] cycle summary', {
      cycle,
      exitCode: result.code,
      signal: result.signal,
      authBlocked: result.authBlocked,
      nextBefore,
      nextAfter,
      rawCountBefore,
      rawCountAfter,
      progressed,
    });

    if (isCompleted(stateAfter)) {
      console.log('[auto-sync] completed full configured range.');
      break;
    }

    if (config.maxCycles > 0 && cycle >= config.maxCycles) {
      console.log(`[auto-sync] reached maxCycles=${config.maxCycles}. stopping.`);
      break;
    }

    if (progressed) {
      noProgressCycles = 0;
    } else {
      noProgressCycles += 1;
      if (noProgressCycles >= config.maxNoProgressCycles) {
        console.error(
          `[auto-sync] no progress for ${noProgressCycles} consecutive cycles. stopping to avoid infinite loop.`,
        );
        process.exit(1);
      }
    }

    if (result.authBlocked) {
      const waitMinutes = Math.ceil(config.waitAfterBlockMs / 60000);
      console.log(`[auto-sync] auth/rate block detected. waiting ${waitMinutes} minute(s) before next cycle.`);
      await sleep(config.waitAfterBlockMs);
      continue;
    }

    if (result.code !== 0) {
      console.warn(`[auto-sync] sync exited with code=${result.code}. retrying in ${Math.ceil(config.quickRetryMs / 1000)}s.`);
      await sleep(config.quickRetryMs);
      continue;
    }

    if (!progressed) {
      console.warn(
        `[auto-sync] cycle ended without progress and without explicit block. retrying in ${Math.ceil(
          config.quickRetryMs / 1000,
        )}s.`,
      );
      await sleep(config.quickRetryMs);
      continue;
    }
  }
};

main().catch((error) => {
  console.error('[auto-sync] fatal error', error);
  process.exit(1);
});
