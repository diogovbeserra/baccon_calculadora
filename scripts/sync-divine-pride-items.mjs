import fs from 'node:fs/promises';
import path from 'node:path';

const ITEM_TYPE = {
  WEAPON: 1,
  ARMOR: 2,
  CONSUMABLE: 3,
  AMMO: 4,
  ETC: 5,
  CARD: 6,
  ENCHANT: 11,
};

const ITEM_SUB_TYPE = {
  UPPER: 512,
  ARMOR: 513,
  SHIELD: 514,
  GARMENT: 515,
  BOOT: 516,
  ACC: 517,
  ACC_R: 510,
  ACC_L: 511,
  PET: 518,
  COSTUME_UPPER: 519,
  COSTUME_MIDDLE: 520,
  COSTUME_LOWER: 521,
  COSTUME_GARMENT: 522,
  SHADOW_WEAPON: 280,
  SHADOW_ARMOR: 526,
  SHADOW_SHIELD: 527,
  SHADOW_BOOT: 528,
  SHADOW_EARRING: 529,
  SHADOW_PENDANT: 530,
};

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .map((arg) => arg.trim())
    .filter((arg) => arg.startsWith('--') && arg.includes('='))
    .map((arg) => {
      const [k, v] = arg.slice(2).split('=');
      return [k, v];
    }),
);

const toNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toBool = (value, fallback) => {
  if (value == null) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n'].includes(normalized)) return false;
  return fallback;
};

const resolvedOutputDir = path.resolve(args.outputDir ?? process.env.DIVINE_PRIDE_OUTPUT_DIR ?? 'data');
const defaultWriteGeneratedAsset = false;

const config = {
  apiBaseUrl: String(args.apiBaseUrl ?? process.env.DIVINE_PRIDE_API_BASE_URL ?? 'https://www.divine-pride.net/api/database').replace(/\/+$/, ''),
  apiKey: String(args.apiKey ?? process.env.DIVINE_PRIDE_API_KEY ?? '').trim(),
  server: String(args.server ?? process.env.DIVINE_PRIDE_SERVER ?? 'LATAM').trim(),
  language: String(args.language ?? process.env.DIVINE_PRIDE_LANGUAGE ?? 'en-US').trim(),
  startId: toNumber(args.startId ?? process.env.DIVINE_PRIDE_START_ID, 1),
  endId: toNumber(args.endId ?? process.env.DIVINE_PRIDE_END_ID, 20000),
  concurrency: Math.max(1, toNumber(args.concurrency ?? process.env.DIVINE_PRIDE_CONCURRENCY, 4)),
  requestDelayMs: Math.max(0, toNumber(args.delayMs ?? process.env.DIVINE_PRIDE_DELAY_MS, 30)),
  timeoutMs: Math.max(500, toNumber(args.timeoutMs ?? process.env.DIVINE_PRIDE_TIMEOUT_MS, 12000)),
  retries: Math.max(0, toNumber(args.retries ?? process.env.DIVINE_PRIDE_RETRIES, 2)),
  checkpointEvery: Math.max(10, toNumber(args.checkpointEvery ?? process.env.DIVINE_PRIDE_CHECKPOINT_EVERY, 100)),
  authCooldownMs: Math.max(10_000, toNumber(args.authCooldownMs ?? process.env.DIVINE_PRIDE_AUTH_COOLDOWN_MS, 180_000)),
  maxAuthErrorsPerWindow: Math.max(1, toNumber(args.maxAuthErrorsPerWindow ?? process.env.DIVINE_PRIDE_MAX_AUTH_ERRORS_PER_WINDOW, 10)),
  maxAuthCooldownCycles: Math.max(1, toNumber(args.maxAuthCooldownCycles ?? process.env.DIVINE_PRIDE_MAX_AUTH_COOLDOWN_CYCLES, 20)),
  outputDir: resolvedOutputDir,
  resume: toBool(args.resume ?? process.env.DIVINE_PRIDE_RESUME, true),
  refreshExisting: toBool(args.refreshExisting ?? process.env.DIVINE_PRIDE_REFRESH_EXISTING, false),
  writeGeneratedAsset: toBool(args.writeGeneratedAsset ?? process.env.DIVINE_PRIDE_WRITE_GENERATED_ASSET, defaultWriteGeneratedAsset),
  generatedAssetsPath: path.resolve(
    args.generatedAssetsPath ?? process.env.DIVINE_PRIDE_GENERATED_ASSETS_PATH ?? 'src/assets/generated/items.normalized.json',
  ),
};

if (!config.apiKey) {
  console.error('Missing API key. Set DIVINE_PRIDE_API_KEY or pass --apiKey=...');
  process.exit(1);
}

if (config.endId < config.startId) {
  console.error(`Invalid range: startId (${config.startId}) > endId (${config.endId})`);
  process.exit(1);
}

const rawPath = path.join(config.outputDir, 'items.raw.json');
const normalizedPath = path.join(config.outputDir, 'items.normalized.json');
const categoriesDir = path.join(config.outputDir, 'categories');
const statePath = path.join(config.outputDir, '.items-sync-state.json');
const generatedAssetsPath = config.generatedAssetsPath;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class RateLimiter {
  nextAllowedAt = 0;
  constructor(delayMs) {
    this.delayMs = delayMs;
  }

  async waitTurn() {
    const now = Date.now();
    const waitMs = Math.max(0, this.nextAllowedAt - now);
    this.nextAllowedAt = now + waitMs + this.delayMs;
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }
}

const rateLimiter = new RateLimiter(config.requestDelayMs);

const safeReadJson = async (filePath, fallbackValue) => {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return JSON.parse(content);
  } catch {
    return fallbackValue;
  }
};

const safeWriteJson = async (filePath, data) => {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
};

const normalizeText = (value) => {
  if (value == null) return '';
  return String(value).replace(/\^[0-9A-Fa-f]{6}/g, '').replace(/\s+/g, ' ').trim();
};

const resolveCategory = (item) => {
  const itemTypeId = Number(item.itemTypeId);
  const itemSubTypeId = Number(item.itemSubTypeId);
  const location = String(item.location ?? '').toLowerCase();

  if (itemTypeId === ITEM_TYPE.WEAPON) return 'weapon';
  if (itemTypeId === ITEM_TYPE.AMMO) return 'ammo';
  if (itemTypeId === ITEM_TYPE.CARD) return 'card';
  if (itemTypeId === ITEM_TYPE.CONSUMABLE) return 'consumable';
  if (itemTypeId === ITEM_TYPE.ENCHANT) return 'enchant';

  switch (itemSubTypeId) {
    case ITEM_SUB_TYPE.SHIELD:
      return 'shield';
    case ITEM_SUB_TYPE.GARMENT:
      return 'garment';
    case ITEM_SUB_TYPE.BOOT:
      return 'shoes';
    case ITEM_SUB_TYPE.ACC:
    case ITEM_SUB_TYPE.ACC_L:
    case ITEM_SUB_TYPE.ACC_R:
      return 'accessory';
    case ITEM_SUB_TYPE.UPPER:
      if (location.includes('middle')) return 'head_mid';
      if (location.includes('lower')) return 'head_low';
      return 'head_top';
    case ITEM_SUB_TYPE.COSTUME_UPPER:
    case ITEM_SUB_TYPE.COSTUME_MIDDLE:
    case ITEM_SUB_TYPE.COSTUME_LOWER:
    case ITEM_SUB_TYPE.COSTUME_GARMENT:
      return 'costume';
    case ITEM_SUB_TYPE.SHADOW_WEAPON:
    case ITEM_SUB_TYPE.SHADOW_ARMOR:
    case ITEM_SUB_TYPE.SHADOW_SHIELD:
    case ITEM_SUB_TYPE.SHADOW_BOOT:
    case ITEM_SUB_TYPE.SHADOW_EARRING:
    case ITEM_SUB_TYPE.SHADOW_PENDANT:
      return 'shadow';
    case ITEM_SUB_TYPE.PET:
      return 'pet';
    case ITEM_SUB_TYPE.ARMOR:
      return 'armor';
    default:
      if (itemTypeId === ITEM_TYPE.ARMOR) return 'armor';
      return 'etc';
  }
};

const normalizeItem = (item) => {
  const category = resolveCategory(item);
  const itemLevel = item.itemLevel ?? null;
  const displayName = itemLevel ? `[LV ${itemLevel}] ${item.name}` : item.name;
  const searchableText = [
    item.name,
    item.aegisName,
    item.unidName,
    item.description,
    item.resName,
    category,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return {
    id: Number(item.id),
    aegisName: item.aegisName ?? '',
    name: item.name ?? '',
    unidName: item.unidName ?? '',
    resName: item.resName ?? '',
    description: item.description ?? '',
    slots: Number(item.slots ?? 0),
    itemTypeId: Number(item.itemTypeId ?? 0),
    itemSubTypeId: Number(item.itemSubTypeId ?? 0),
    itemLevel,
    attack: item.attack ?? null,
    defense: item.defense ?? null,
    weight: Number(item.weight ?? 0),
    requiredLevel: item.requiredLevel ?? null,
    limitLevel: item.limitLevel ?? null,
    location: item.location ?? null,
    compositionPos: Number(item.compositionPos ?? 0),
    propertyAtk: item.propertyAtk ?? null,
    range: item.range ?? null,
    matk: item.matk ?? null,
    refinable: Boolean(item.refinable),
    indestructible: Boolean(item.indestructible),
    classNum: Number(item.classNum ?? 0) || null,
    attribute: Number(item.attribute ?? 0) || null,
    source: 'divine-pride',
    script: {},
    category,
    subcategory: category,
    displayName,
    searchableText: normalizeText(searchableText),
  };
};

const createHeaders = () => ({
  'Accept-Language': config.language,
  Accept: 'application/json, text/plain, */*',
});

const fetchItemOnce = async (itemId) => {
  await rateLimiter.waitTurn();
  const requestUrl = `${config.apiBaseUrl}/Item/${itemId}?apiKey=${encodeURIComponent(config.apiKey)}&server=${encodeURIComponent(config.server)}`;
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), config.timeoutMs);

  try {
    const response = await fetch(requestUrl, {
      method: 'GET',
      headers: createHeaders(),
      signal: controller.signal,
    });

    if (response.status === 404) {
      return { kind: 'missing' };
    }
    if (response.status === 401 || response.status === 403) {
      return { kind: 'auth', status: response.status };
    }
    if (!response.ok) {
      return { kind: 'http', status: response.status };
    }

    const payload = await response.json();
    if (!payload || typeof payload !== 'object' || !payload.id || !payload.name) {
      return { kind: 'missing' };
    }
    return {
      kind: 'ok',
      item: payload,
    };
  } catch (error) {
    if (error?.name === 'AbortError') {
      return { kind: 'timeout' };
    }
    return { kind: 'network', error: String(error?.message ?? error) };
  } finally {
    clearTimeout(timeoutHandle);
  }
};

const fetchItemWithRetry = async (itemId) => {
  for (let attempt = 0; attempt <= config.retries; attempt += 1) {
    const result = await fetchItemOnce(itemId);
    if (result.kind === 'ok' || result.kind === 'missing' || result.kind === 'auth') {
      return result;
    }

    const isLastAttempt = attempt === config.retries;
    if (isLastAttempt) {
      return result;
    }
    await sleep(250 * (attempt + 1));
  }
  return { kind: 'network', error: 'unknown' };
};

const formatNumber = (value) => new Intl.NumberFormat('en-US').format(value);

const buildCategoryFiles = (normalizedMap) => {
  const items = Object.values(normalizedMap).sort((a, b) => (a.name > b.name ? 1 : -1));
  const byCategory = {
    weapons: items.filter((item) => item.category === 'weapon'),
    armors: items.filter((item) => item.category === 'armor'),
    shields: items.filter((item) => item.category === 'shield'),
    garments: items.filter((item) => item.category === 'garment'),
    shoes: items.filter((item) => item.category === 'shoes'),
    accessories: items.filter((item) => item.category === 'accessory'),
    headgear: items.filter((item) => item.category === 'head_top' || item.category === 'head_mid' || item.category === 'head_low'),
    ammo: items.filter((item) => item.category === 'ammo'),
    costume: items.filter((item) => item.category === 'costume'),
    shadow: items.filter((item) => item.category === 'shadow'),
  };

  return byCategory;
};

const findMinInArray = (values) => {
  if (!Array.isArray(values) || values.length === 0) return null;
  let min = values[0];
  for (let index = 1; index < values.length; index += 1) {
    if (values[index] < min) min = values[index];
  }
  return min;
};

const findMinInSet = (values) => {
  if (!values || values.size === 0) return null;
  let min = null;
  for (const value of values) {
    if (min == null || value < min) min = value;
  }
  return min;
};

const run = async () => {
  await fs.mkdir(config.outputDir, { recursive: true });
  await fs.mkdir(categoriesDir, { recursive: true });
  if (config.writeGeneratedAsset) {
    await fs.mkdir(path.dirname(generatedAssetsPath), { recursive: true });
  }

  const rawMap = await safeReadJson(rawPath, {});
  const existingState = config.resume ? await safeReadJson(statePath, null) : null;
  const initialId = existingState?.nextId && Number(existingState.nextId) >= config.startId
    ? Number(existingState.nextId)
    : config.startId;

  const idsToProcess = [];
  for (let id = initialId; id <= config.endId; id += 1) {
    if (!config.refreshExisting && rawMap[id]) continue;
    idsToProcess.push(id);
  }

  console.log('[sync] configuration', {
    apiBaseUrl: config.apiBaseUrl,
    server: config.server,
    startId: config.startId,
    endId: config.endId,
    initialId,
    concurrency: config.concurrency,
    requestDelayMs: config.requestDelayMs,
    retries: config.retries,
    timeoutMs: config.timeoutMs,
    queueSize: idsToProcess.length,
    outputDir: config.outputDir,
  });

  const pendingIds = [...idsToProcess];
  const inFlightIds = new Set();
  let processed = 0;
  let found = 0;
  let missing = 0;
  let errors = 0;
  let authErrors = 0;
  let authWindowErrors = 0;
  let authCooldownUntil = 0;
  let authCooldownCycles = 0;
  let highestIdProcessed = initialId - 1;
  let isInCooldown = false;
  let abortSync = false;

  const persistCheckpoint = async () => {
    const pendingMinId = findMinInArray(pendingIds);
    const inFlightMinId = findMinInSet(inFlightIds);

    let nextId = highestIdProcessed + 1;
    if (pendingMinId != null) nextId = Math.min(nextId, pendingMinId);
    if (inFlightMinId != null) nextId = Math.min(nextId, inFlightMinId);
    if (!Number.isFinite(nextId)) nextId = config.startId;

    await safeWriteJson(statePath, {
      server: config.server,
      startId: config.startId,
      endId: config.endId,
      nextId,
      pendingSize: pendingIds.length,
      inFlightSize: inFlightIds.size,
      processed,
      found,
      missing,
      errors,
      authErrors,
      updatedAt: new Date().toISOString(),
    });
    await safeWriteJson(rawPath, rawMap);
  };

  const getNextId = () => {
    if (pendingIds.length === 0) return null;
    const nextId = pendingIds.shift();
    inFlightIds.add(nextId);
    return nextId;
  };

  const maybeWaitForAuthCooldown = async () => {
    const waitMs = authCooldownUntil - Date.now();
    if (waitMs <= 0) return;
    if (!isInCooldown) {
      isInCooldown = true;
      console.warn(`[sync] auth cooldown active. waiting ${Math.ceil(waitMs / 1000)}s before retrying...`);
      await persistCheckpoint();
    }
    await sleep(waitMs);
    isInCooldown = false;
  };

  const worker = async () => {
    while (true) {
      if (abortSync) return;
      await maybeWaitForAuthCooldown();
      if (abortSync) return;

      const itemId = getNextId();
      if (itemId == null) return;

      const result = await fetchItemWithRetry(itemId);

      if (result.kind === 'ok') {
        inFlightIds.delete(itemId);
        highestIdProcessed = Math.max(highestIdProcessed, itemId);
        processed += 1;
        authWindowErrors = 0;
        rawMap[itemId] = result.item;
        found += 1;
      } else if (result.kind === 'missing') {
        inFlightIds.delete(itemId);
        highestIdProcessed = Math.max(highestIdProcessed, itemId);
        processed += 1;
        authWindowErrors = 0;
        missing += 1;
      } else {
        errors += 1;
        if (result.kind === 'auth') {
          inFlightIds.delete(itemId);
          authErrors += 1;
          authWindowErrors += 1;
          pendingIds.unshift(itemId);
          if (authWindowErrors >= config.maxAuthErrorsPerWindow) {
            authWindowErrors = 0;
            authCooldownUntil = Date.now() + config.authCooldownMs;
            authCooldownCycles += 1;
            console.warn(
              `[sync] too many auth errors in short window. entering cooldown (${Math.ceil(config.authCooldownMs / 1000)}s).`,
            );
            if (authCooldownCycles >= config.maxAuthCooldownCycles) {
              abortSync = true;
              console.error(
                `[sync] auth kept failing after ${authCooldownCycles} cooldown cycles. stopping safely. rerun later with resume.`,
              );
            }
          }
          continue;
        }

        inFlightIds.delete(itemId);
        highestIdProcessed = Math.max(highestIdProcessed, itemId);
        processed += 1;
      }

      if (processed % config.checkpointEvery === 0) {
        await persistCheckpoint();
        const progress = ((processed / Math.max(1, idsToProcess.length)) * 100).toFixed(2);
        console.log(
          `[sync] ${progress}% processed=${formatNumber(processed)} found=${formatNumber(found)} missing=${formatNumber(missing)} errors=${formatNumber(errors)}`,
        );
      }
    }
  };

  const workers = Array.from({ length: config.concurrency }, () => worker());
  await Promise.all(workers);

  await persistCheckpoint();

  const normalizedMap = Object.values(rawMap).reduce((acc, rawItem) => {
    const normalized = normalizeItem(rawItem);
    if (!normalized.id || !normalized.name) return acc;
    acc[normalized.id] = normalized;
    return acc;
  }, {});

  const categoryFiles = buildCategoryFiles(normalizedMap);
  for (const [name, items] of Object.entries(categoryFiles)) {
    await safeWriteJson(path.join(categoriesDir, `${name}.json`), items);
  }

  await safeWriteJson(normalizedPath, normalizedMap);
  if (config.writeGeneratedAsset) {
    await safeWriteJson(generatedAssetsPath, normalizedMap);
  }

  console.log('[sync] completed', {
    found: formatNumber(found),
    missing: formatNumber(missing),
    errors: formatNumber(errors),
    rawPath,
    normalizedPath,
    generatedAssetsPath: config.writeGeneratedAsset ? generatedAssetsPath : 'disabled',
  });
};

run().catch((error) => {
  console.error('[sync] fatal error', error);
  process.exit(1);
});
