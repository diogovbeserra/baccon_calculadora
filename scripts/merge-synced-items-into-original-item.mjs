import fs from 'node:fs/promises';
import path from 'node:path';

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

const toBool = (value, fallback) => {
  if (value == null) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n'].includes(normalized)) return false;
  return fallback;
};

const config = {
  rawPath: path.resolve(args.rawPath ?? process.env.DIVINE_PRIDE_RAW_PATH ?? 'data/items.raw.json'),
  targetItemPath: path.resolve(args.targetItemPath ?? process.env.ORIGINAL_ITEM_JSON_PATH ?? 'src/assets/demo/data/item.json'),
  maxRefineTotal: Math.max(20, toNumber(args.maxRefineTotal ?? process.env.SET_MAX_REFINE_TOTAL, 40)),
  mergeExistingBase: toBool(args.mergeExistingBase ?? process.env.MERGE_EXISTING_BASE, false),
  mirrorSetRules: toBool(args.mirrorSetRules ?? process.env.MIRROR_SET_RULES, false),
  dryRun: toBool(args.dryRun ?? process.env.MERGE_DRY_RUN, false),
};

const safeReadJson = async (filePath) => JSON.parse(await fs.readFile(filePath, 'utf8'));

const safeWriteJson = async (filePath, data) => {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
};

const stripColorCode = (text) =>
  String(text ?? '')
    .replace(/\^[0-9A-Fa-f]{6}/g, '')
    .replace(/\r/g, '')
    .trim();

const mapRefineType = (item) => {
  const itemTypeId = Number(item?.itemTypeId ?? 0);
  const itemSubTypeId = Number(item?.itemSubTypeId ?? 0);
  if (itemTypeId === 1) return 'weapon';
  if (itemSubTypeId === 513) return 'armor';
  if (itemSubTypeId === 515) return 'garment';
  if (itemSubTypeId === 516) return 'boot';
  if (itemSubTypeId === 514) return 'shield';
  if (itemSubTypeId === 512 || itemSubTypeId === 519 || itemSubTypeId === 520 || itemSubTypeId === 521) return 'headUpper';
  return null;
};

const pickRefinePair = (selfItem, partnerItem) => {
  const pair = [mapRefineType(selfItem), mapRefineType(partnerItem)].filter(Boolean);
  const dedup = [...new Set(pair)];
  if (dedup.length >= 2) return dedup.slice(0, 2);
  if (dedup.length === 1) return [dedup[0], dedup[0] === 'armor' ? 'garment' : 'armor'];
  return ['armor', 'garment'];
};

const addRule = (script, attr, rule) => {
  if (!rule) return;
  if (!script[attr]) {
    script[attr] = [rule];
    return;
  }
  if (!script[attr].includes(rule)) {
    script[attr].push(rule);
  }
};

const parseNumericBonus = (line) => {
  const normalized = stripColorCode(line);
  if (!normalized) return null;

  const matchers = [
    { regex: /^(ATK)\s*\+(\d+)\s*%\.?$/i, attr: 'atkPercent' },
    { regex: /^(ATK)\s*\+(\d+)\.?$/i, attr: 'atk' },
    { regex: /^(MATK)\s*\+(\d+)\s*%\.?$/i, attr: 'matkPercent' },
    { regex: /^(MATK)\s*\+(\d+)\.?$/i, attr: 'matk' },
    { regex: /^Attack Speed\s*\+(\d+)%\.?$/i, attr: 'aspdPercent', group: 1 },
    { regex: /^(?:Additional\s+)?(?:Skill delay|After Cast Delay)\s*-(\d+)%\.?$/i, attr: 'acd', group: 1 },
    { regex: /^Ranged Physical Damage\s*\+(\d+)%\.?$/i, attr: 'range', group: 1 },
    { regex: /^Melee Physical Damage\s*\+(\d+)%\.?$/i, attr: 'melee', group: 1 },
    { regex: /^Physical Damage against all properties\s*\+(\d+)%\.?$/i, attr: 'p_element_all', group: 1 },
  ];

  for (const matcher of matchers) {
    const found = normalized.match(matcher.regex);
    if (!found) continue;
    if (matcher.group != null) {
      return { attr: matcher.attr, value: Number(found[matcher.group]) };
    }
    return { attr: matcher.attr, value: Number(found[2]) };
  }

  const dynamicRefineMatch = normalized.match(
    /^Ranged and Melee Physical Damage\s*\+(\d+)%\s*per refine of the (.+)\.?$/i,
  );
  if (dynamicRefineMatch) {
    return {
      dynamicPair: true,
      attrs: ['range', 'melee'],
      perRefine: Number(dynamicRefineMatch[1]),
    };
  }

  return null;
};

const buildGeneratedScript = (rawItem, rawMap) => {
  const baseScript = {};
  const setScript = {};
  const description = stripColorCode(rawItem?.description ?? '');
  const lines = description
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  const setItemIds = (rawItem?.sets ?? [])
    .flatMap((setRec) => setRec?.items ?? [])
    .map((item) => Number(item?.itemId))
    .filter((id) => Number.isFinite(id) && id > 0 && id !== Number(rawItem?.id));
  const relatedItemIds = [...new Set(setItemIds)];

  let currentRefineCond = null;
  let pendingEveryRefine = null;
  let inSetSection = false;
  let currentSetRefineCond = null;

  for (const rawLine of lines) {
    let line = rawLine;

    const refineLine = line.match(/Refine\s*\+(\d+)\s*or more:/i);
    if (refineLine) {
      currentRefineCond = Number(refineLine[1]);
      pendingEveryRefine = null;
      line = line.replace(refineLine[0], '').trim();
      if (!line) continue;
    }

    const everyRefineLine = line.match(/For every\s*(\d+)\s*refines?:/i);
    if (everyRefineLine) {
      pendingEveryRefine = Number(everyRefineLine[1]);
      line = line.replace(everyRefineLine[0], '').trim();
      if (!line) continue;
    }

    const setRefineLine = line.match(/Set refine sum\s*(\d+)\s*or more:/i);
    if (setRefineLine) {
      inSetSection = true;
      currentSetRefineCond = Number(setRefineLine[1]);
      line = line.replace(setRefineLine[0], '').trim();
      if (!line) continue;
    }

    if (/^Set\b/i.test(line)) {
      inSetSection = true;
      currentSetRefineCond = null;
      continue;
    }
    if (/^\[.+]$/.test(line)) {
      inSetSection = true;
      continue;
    }

    const parsed = parseNumericBonus(line);
    if (!parsed) continue;

    if (parsed.dynamicPair) {
      if (!inSetSection || !currentSetRefineCond || relatedItemIds.length === 0) continue;
      for (const partnerId of relatedItemIds) {
        const partner = rawMap[String(partnerId)] ?? {};
        const [refineA, refineB] = pickRefinePair(rawItem, partner);
        const prefix = `EQUIP[${partnerId}]REFINE[${refineA},${refineB}==`;

        for (const attr of parsed.attrs) {
          addRule(
            setScript,
            attr,
            `${prefix}${currentSetRefineCond}]===${currentSetRefineCond * parsed.perRefine}`,
          );
          for (let refine = currentSetRefineCond + 1; refine <= config.maxRefineTotal; refine += 1) {
            addRule(setScript, attr, `${prefix}${refine}]===${parsed.perRefine}`);
          }
        }
      }
      continue;
    }

    if (inSetSection && relatedItemIds.length > 0) {
      for (const partnerId of relatedItemIds) {
        const partner = rawMap[String(partnerId)] ?? {};
        const [refineA, refineB] = pickRefinePair(rawItem, partner);
        if (currentSetRefineCond) {
          addRule(
            setScript,
            parsed.attr,
            `EQUIP[${partnerId}]REFINE[${refineA},${refineB}==${currentSetRefineCond}]===${parsed.value}`,
          );
        } else {
          addRule(setScript, parsed.attr, `EQUIP[${partnerId}]===${parsed.value}`);
        }
      }
      continue;
    }

    if (currentRefineCond) {
      addRule(baseScript, parsed.attr, `${currentRefineCond}===${parsed.value}`);
      continue;
    }

    if (pendingEveryRefine) {
      addRule(baseScript, parsed.attr, `${pendingEveryRefine}---${parsed.value}`);
      pendingEveryRefine = null;
      continue;
    }

    addRule(baseScript, parsed.attr, String(parsed.value));
  }

  return { baseScript, setScript, relatedItemIds };
};

const mergeScripts = (targetScript, sourceScript) => {
  for (const [attr, lines] of Object.entries(sourceScript)) {
    if (!Array.isArray(lines) || lines.length === 0) continue;
    if (!targetScript[attr]) {
      targetScript[attr] = [...lines];
      continue;
    }
    for (const line of lines) {
      if (!targetScript[attr].includes(line)) {
        targetScript[attr].push(line);
      }
    }
  }
};

const removeScriptRules = (targetScript, sourceScript) => {
  let removed = 0;
  for (const [attr, lines] of Object.entries(sourceScript)) {
    if (!Array.isArray(lines) || lines.length === 0) continue;
    const targetLines = targetScript[attr];
    if (!Array.isArray(targetLines) || targetLines.length === 0) continue;
    const nextLines = targetLines.filter((line) => !lines.includes(line));
    removed += targetLines.length - nextLines.length;
    if (nextLines.length > 0) {
      targetScript[attr] = nextLines;
    } else {
      delete targetScript[attr];
    }
  }

  return removed;
};

const normalizeItemName = (name) =>
  String(name ?? '')
    .replace(/\[\d]$/, '')
    .trim()
    .toLowerCase();

const isNumericToken = (token) => /^\d+$/.test(String(token ?? '').trim());

const buildItemNameIdMaps = (itemMap) => {
  const byNormalizedName = new Map();
  const byId = new Map();

  for (const item of Object.values(itemMap)) {
    if (!item || typeof item !== 'object') continue;
    const id = Number(item.id);
    if (!Number.isFinite(id) || id <= 0) continue;
    byId.set(id, item);

    const names = [item.name, item.unidName].map(normalizeItemName).filter(Boolean);
    for (const normalizedName of names) {
      if (!byNormalizedName.has(normalizedName)) {
        byNormalizedName.set(normalizedName, id);
      }
    }
  }

  return { byId, byNormalizedName };
};

const hasPartnerBackReference = (itemMap, nameMaps, rawItem, relatedItemIds) => {
  if (relatedItemIds.length === 0) return false;
  const rawItemId = Number(rawItem?.id);
  const rawName = normalizeItemName(rawItem?.name);

  for (const partnerId of relatedItemIds) {
    const partner = itemMap[String(partnerId)];
    if (!partner?.script || typeof partner.script !== 'object') continue;

    for (const lines of Object.values(partner.script)) {
      if (!Array.isArray(lines)) continue;
      for (const line of lines) {
        if (typeof line !== 'string' || !line.startsWith('EQUIP[')) continue;
        const equipToken = line.match(/^EQUIP\[(.+?)]/)?.[1]?.trim();
        if (!equipToken) continue;

        if (isNumericToken(equipToken) && Number(equipToken) === rawItemId) {
          return true;
        }

        const partnerEquipId = nameMaps.byNormalizedName.get(normalizeItemName(equipToken));
        if (partnerEquipId && partnerEquipId === rawItemId) {
          return true;
        }

        if (!partnerEquipId && normalizeItemName(equipToken) === rawName) {
          return true;
        }
      }
    }
  }

  return false;
};

const dedupeEquipAliasRules = (script, nameMaps) => {
  let removed = 0;
  for (const [attr, lines] of Object.entries(script)) {
    if (!Array.isArray(lines) || lines.length === 0) continue;
    const seenCanonical = new Set();
    const nextLines = [];

    for (const line of lines) {
      if (typeof line !== 'string' || !line.startsWith('EQUIP[')) {
        nextLines.push(line);
        continue;
      }
      const equipToken = line.match(/^EQUIP\[(.+?)]/)?.[1]?.trim();
      if (!equipToken) {
        nextLines.push(line);
        continue;
      }

      let canonicalToken = equipToken;
      if (isNumericToken(equipToken)) {
        canonicalToken = String(Number(equipToken));
      } else {
        const resolvedId = nameMaps.byNormalizedName.get(normalizeItemName(equipToken));
        if (resolvedId) canonicalToken = String(resolvedId);
      }

      const canonicalLine = line.replace(/^EQUIP\[(.+?)]/, `EQUIP[${canonicalToken}]`);
      if (seenCanonical.has(canonicalLine)) {
        removed += 1;
        continue;
      }
      seenCanonical.add(canonicalLine);
      nextLines.push(line);
    }

    script[attr] = nextLines;
  }

  return removed;
};

const removeWeakerEquipRules = (script, nameMaps) => {
  let removed = 0;
  for (const [attr, lines] of Object.entries(script)) {
    if (!Array.isArray(lines) || lines.length === 0) continue;

    const parsed = lines
      .map((line, index) => {
        const match = String(line).match(/^EQUIP\[(.+?)](.*)===(-?\d+(?:\.\d+)?)$/);
        if (!match) return null;
        const [, equipToken, rawCondition, rawValue] = match;
        const condition = rawCondition ?? '';
        const normalizedName = normalizeItemName(equipToken);
        const resolvedId = isNumericToken(equipToken)
          ? Number(equipToken)
          : nameMaps.byNormalizedName.get(normalizedName);
        const canonicalKey = resolvedId ? String(resolvedId) : `name:${normalizedName}`;
        return {
          index,
          line,
          canonicalKey,
          condition,
          value: rawValue,
          isUnconditional: condition === '',
          hasRefineCondition: condition.includes('REFINE['),
        };
      })
      .filter(Boolean);

    const conditionalKeys = new Set(
      parsed
        .filter((entry) => entry.hasRefineCondition)
        .map((entry) => `${entry.canonicalKey}|${entry.value}`),
    );

    const nextLines = lines.filter((line, index) => {
      const entry = parsed.find((current) => current.index === index);
      if (!entry || !entry.isUnconditional) return true;
      const key = `${entry.canonicalKey}|${entry.value}`;
      if (!conditionalKeys.has(key)) return true;
      removed += 1;
      return false;
    });

    script[attr] = nextLines;
  }

  return removed;
};

const rewriteExoskeletonSetRules = (itemMap) => {
  const wingId = '480124';
  const armorId = '450405';
  const wing = itemMap[wingId];
  const armor = itemMap[armorId];
  let changed = 0;

  if (wing?.script && typeof wing.script === 'object') {
    for (const [attr, lines] of Object.entries(wing.script)) {
      if (!Array.isArray(lines)) continue;
      const nextLines = lines.map((line) =>
        String(line).replace(/^EQUIP\[Physical Exoskeleton]/, 'EQUIP[450405]'),
      );
      if (JSON.stringify(nextLines) !== JSON.stringify(lines)) {
        changed += 1;
        wing.script[attr] = nextLines;
      }
    }
  }

  if (armor?.script && typeof armor.script === 'object') {
    const before = JSON.stringify(armor.script);
    const cleaned = {};
    for (const [attr, lines] of Object.entries(armor.script)) {
      if (!Array.isArray(lines)) continue;
      const nextLines = lines.filter((line) => !String(line).includes('EQUIP[480124]'));
      if (nextLines.length > 0) cleaned[attr] = nextLines;
    }
    armor.script = cleaned;
    if (before !== JSON.stringify(armor.script)) {
      changed += 1;
    }
  }

  return changed;
};

const createItemFromRaw = (rawItem, generatedScript) => ({
  id: Number(rawItem.id),
  aegisName: rawItem.aegisName ?? '',
  name: rawItem.name ?? '',
  unidName: rawItem.unidName ?? '',
  resName: rawItem.resName ?? '',
  description: rawItem.description ?? '',
  slots: Number(rawItem.slots ?? 0),
  itemTypeId: Number(rawItem.itemTypeId ?? 0),
  itemSubTypeId: Number(rawItem.itemSubTypeId ?? 0),
  itemLevel: rawItem.itemLevel ?? null,
  attack: rawItem.attack ?? null,
  defense: rawItem.defense ?? null,
  weight: Number(rawItem.weight ?? 0),
  requiredLevel: rawItem.requiredLevel ?? null,
  location: rawItem.location ?? null,
  compositionPos: rawItem.compositionPos ?? null,
  usableClass: ['all'],
  script: generatedScript,
});

const run = async () => {
  const rawMap = await safeReadJson(config.rawPath);
  const itemMap = await safeReadJson(config.targetItemPath);

  const stats = {
    totalRaw: Object.keys(rawMap).length,
    targetBefore: Object.keys(itemMap).length,
    inserted: 0,
    updatedSetRules: 0,
    enrichedBaseRules: 0,
    mirroredSetRules: 0,
    prunedSetRules: 0,
    cleanedAliasRules: 0,
    cleanedWeakRules: 0,
    knownSetFixes: 0,
  };
  const mirroredByItemId = {};
  const nameMaps = buildItemNameIdMaps(itemMap);

  for (const [id, rawItem] of Object.entries(rawMap)) {
    if (!rawItem || typeof rawItem !== 'object' || !rawItem.id || !rawItem.name) continue;
    const { baseScript, setScript, relatedItemIds } = buildGeneratedScript(rawItem, rawMap);
    const combinedScript = {};
    mergeScripts(combinedScript, baseScript);
    mergeScripts(combinedScript, setScript);

    if (config.mirrorSetRules && relatedItemIds.length > 0) {
      for (const partnerId of relatedItemIds) {
        const key = String(partnerId);
        if (!mirroredByItemId[key]) mirroredByItemId[key] = {};
        for (const [attr, lines] of Object.entries(setScript)) {
          for (const line of lines) {
            const marker = `EQUIP[${partnerId}]`;
            if (!line.includes(marker)) continue;
            addRule(mirroredByItemId[key], attr, line.replace(marker, `EQUIP[${rawItem.id}]`));
          }
        }
      }
    }

    const existing = itemMap[id];
    if (!existing) {
      itemMap[id] = createItemFromRaw(rawItem, combinedScript);
      stats.inserted += 1;
      continue;
    }

    if (!existing.script || typeof existing.script !== 'object') {
      existing.script = {};
    }

    const beforeSetSignature = JSON.stringify(existing.script);
    if (config.mergeExistingBase) {
      mergeScripts(existing.script, baseScript);
    }
    const skipSetMerge = hasPartnerBackReference(itemMap, nameMaps, rawItem, relatedItemIds);
    if (skipSetMerge) {
      stats.prunedSetRules += removeScriptRules(existing.script, setScript);
    } else {
      mergeScripts(existing.script, setScript);
    }
    const afterSetSignature = JSON.stringify(existing.script);

    if (beforeSetSignature !== afterSetSignature) {
      stats.updatedSetRules += 1;
    }
    if (config.mergeExistingBase && Object.keys(baseScript).length > 0) {
      stats.enrichedBaseRules += 1;
    }
  }

  if (config.mirrorSetRules) {
    for (const [targetId, mirroredScript] of Object.entries(mirroredByItemId)) {
      if (!mirroredScript || Object.keys(mirroredScript).length === 0) continue;
      const targetItem = itemMap[targetId];
      if (!targetItem) continue;
      if (!targetItem.script || typeof targetItem.script !== 'object') {
        targetItem.script = {};
      }
      const beforeMirror = JSON.stringify(targetItem.script);
      mergeScripts(targetItem.script, mirroredScript);
      if (beforeMirror !== JSON.stringify(targetItem.script)) {
        stats.mirroredSetRules += 1;
      }
    }
  }

  for (const item of Object.values(itemMap)) {
    if (!item?.script || typeof item.script !== 'object') continue;
    stats.cleanedAliasRules += dedupeEquipAliasRules(item.script, nameMaps);
    stats.cleanedWeakRules += removeWeakerEquipRules(item.script, nameMaps);
  }
  stats.knownSetFixes += rewriteExoskeletonSetRules(itemMap);

  stats.targetAfter = Object.keys(itemMap).length;

  if (!config.dryRun) {
    await safeWriteJson(config.targetItemPath, itemMap);
  }

  console.log('[merge] completed', {
    ...stats,
    dryRun: config.dryRun,
    rawPath: config.rawPath,
    targetItemPath: config.targetItemPath,
  });
};

run().catch((error) => {
  console.error('[merge] fatal error', error);
  process.exit(1);
});
