// Bundled _worker.js for CF-Server-Monitor (Pages)
"use strict";
const __mods = {};

// /src/database/schema.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_database_schema_js"] = {};




let dbInitialized = false;

async function initDatabase(db) {
  if (dbInitialized) return;

  debug('初始化数据库');
  
  try {
    const SettingTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='settings'
    `).first();
    if (!SettingTableExists) {
      await db.prepare(`
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY, 
          value TEXT
        )
      `).run();
      await saveSiteOptions(db, { servers_optimized: 'true' });
      await saveSiteOptions(db, { history_id_optimized: 'true' });
    }

    // 判断servers表是否存在
    const ServerTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='servers'
    `).first();
    if (!ServerTableExists) {
      await db.prepare(`
        CREATE TABLE IF NOT EXISTS servers (
          id TEXT PRIMARY KEY,
          name TEXT,
          server_group TEXT DEFAULT 'Default',
          tags TEXT DEFAULT '',
          note TEXT DEFAULT '',
          price TEXT DEFAULT '',
          expire_date TEXT DEFAULT '',
          traffic_limit TEXT DEFAULT '',
          traffic_calc_type TEXT DEFAULT 'total',
          reset_day INTEGER DEFAULT 1,
          collect_interval INTEGER DEFAULT 0,
          report_interval INTEGER DEFAULT 60,
          ping_mode TEXT DEFAULT 'tcp',
          custom_ct TEXT DEFAULT '',
          custom_cu TEXT DEFAULT '',
          custom_cm TEXT DEFAULT '',
          custom_bd TEXT DEFAULT '',
          rx_correction REAL DEFAULT NULL,
          tx_correction REAL DEFAULT NULL,
          offline_notify_disabled TEXT DEFAULT '0',
          is_hidden TEXT DEFAULT '0',
          sort_order INTEGER DEFAULT 0,
          history_partition_id INTEGER DEFAULT 0,
          timestamp INTEGER DEFAULT 0
        )
      `).run();
    } else {
      debug('检查servers表优化状态');
      await ensureServerOptimization(db);
    }

    // 判断metrics_history表是否存在
    const historyTableExists = await db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history'
    `).first();
    if (!historyTableExists) {
      await db.prepare(`
        CREATE TABLE IF NOT EXISTS metrics_history (
          id INTEGER PRIMARY KEY,
          server_id TEXT NOT NULL,
          timestamp INTEGER DEFAULT 0,
          cpu REAL DEFAULT 0,
          load_avg TEXT DEFAULT '0',
          net_in_speed REAL DEFAULT 0,
          net_out_speed REAL DEFAULT 0,
          net_rx REAL DEFAULT 0,
          net_tx REAL DEFAULT 0,
          processes INTEGER DEFAULT 0,
          tcp_conn INTEGER DEFAULT 0,
          udp_conn INTEGER DEFAULT 0,
          ping_ct INTEGER DEFAULT 0,
          ping_cu INTEGER DEFAULT 0,
          ping_cm INTEGER DEFAULT 0,
          ping_bd INTEGER DEFAULT 0,
          loss_ct INTEGER DEFAULT NULL,
          loss_cu INTEGER DEFAULT NULL,
          loss_cm INTEGER DEFAULT NULL,
          loss_bd INTEGER DEFAULT NULL,
          ram_total REAL DEFAULT 0,
          ram_used REAL DEFAULT 0,
          swap_total REAL DEFAULT 0,
          swap_used REAL DEFAULT 0,
          disk_total REAL DEFAULT 0,
          disk_used REAL DEFAULT 0,
          cpu_cores INTEGER DEFAULT 0,
          cpu_info TEXT DEFAULT '',
          gpu REAL DEFAULT NULL,
          gpu_info TEXT DEFAULT '',
          arch TEXT DEFAULT '',
          os TEXT DEFAULT '',
          region TEXT DEFAULT '',
          ip_v4 TEXT DEFAULT '0',
          ip_v6 TEXT DEFAULT '0',
          boot_time TEXT DEFAULT '',
          net_rx_monthly REAL DEFAULT 0,
          net_tx_monthly REAL DEFAULT 0
        )
      `).run();
    }else{
      await ensureHistoryIndex(db);
    }

    debug('✅ 数据库初始化完成');
    dbInitialized = true;
  } catch (e) {
    console.error('❌ 数据库初始化失败:', e);
  }
}

async function clearHistory(db) {
  debug('开始清空历史数据...');
  
  try {
    await db.prepare(`DROP TABLE IF EXISTS metrics_history`).run();
    debug('✅ 已删除 metrics_history 表');

    await db.prepare(`DROP TABLE IF EXISTS metrics_history_old`).run();
    debug('✅ 已删除 metrics_history_old 表');
    
    dbInitialized = false;
    
    await initDatabase(db);

    await saveSiteOptions(db, { history_id_optimized: 'true' });

    await clearAllCaches(db);
    
    debug('✅ 数据库重建完成');
    
    return {
      success: true,
      message: 'databaseRebuiltSuccess'
    };
  } catch (e) {
    console.error('❌ 数据库清理失败:', e);
    return {
      success: false,
      message: 'databaseRebuiltFailed',
      error: e.message
    };
  }
}

async function hasHistoryServerTimeIndex(db, tableName) {
  const index = await db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'index'
      AND tbl_name = ?
      AND sql IS NOT NULL
      AND LOWER(sql) LIKE '%server_id%'
      AND LOWER(sql) LIKE '%timestamp%'
    LIMIT 1
  `).bind(tableName).first();

  return !!index;
}

function buildHistorySourceQuery(tableName, useIdRange, columns) {
  if (useIdRange) {
    return `
      SELECT timestamp, ${columns} FROM ${tableName}
      WHERE id >= ?
        AND id <= ?
    `;
  }

  return `
    SELECT timestamp, ${columns} FROM ${tableName}
    WHERE server_id = ?
      AND typeof(timestamp) = 'integer'
      AND timestamp >= ?
  `;
}

async function getMetricsHistory(db, serverId, hours, columns, server = null) {
  const now = Date.now();
  const cacheDuration = getCacheDuration(hours);
  
  const cached = getMetricsHistoryCache(serverId, hours, columns);
  if (cached && now - cached.timestamp < cacheDuration) {
    debug(`[History] CACHE HIT: ${serverId}, hours: ${hours}`);
    return cached.data;
  }
  
  // 最多返回160个数据点,前端需要配合这个计算断点阈值
  const queryHours = Math.min(hours, 168);
  const MAX_POINTS = 160;
  const totalMs = queryHours * 60 * 60 * 1000;
  const intervalMs = Math.max(10_000, Math.ceil(totalMs / MAX_POINTS));

  const cutoff = now - queryHours * 60 * 60 * 1000;
  const historyInfo = await getServerHistoryInfo(db, serverId, server);
  const queryStart = Math.max(cutoff, historyInfo.startTimestamp);

  debug(
    '[History]',
    'server:', serverId,
    'hours:', hours,
    'queryHours:', queryHours,
    'interval:', intervalMs,
    'cutoff:', new Date(cutoff).toISOString(),
    'start:', new Date(queryStart).toISOString()
  );

  // 判断是否需要查询 metrics_history_old 表
  // 如果实际查询起点早于本周日 00:00 UTC（表轮换时间），说明需要查旧表
  const nowDate = new Date(now);
  const day = nowDate.getUTCDay();
  const thisSunday = new Date(Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate() - day));
  const needOldTable = queryStart < thisSunday.getTime();
  
  const oldTableExists = needOldTable && !!await db.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history_old'`
  ).first();

  const history_id_optimized = await getSettingByKey(db, 'history_id_optimized', true);
  const currentHasServerTimeIndex = history_id_optimized
    ? false
    : await hasHistoryServerTimeIndex(db, 'metrics_history');
  const currentUsesIdRange = history_id_optimized || !currentHasServerTimeIndex;
  const oldUsesIdRange = oldTableExists
    ? history_id_optimized || !await hasHistoryServerTimeIndex(db, 'metrics_history_old')
    : false;
  const needsIdRange = currentUsesIdRange || oldUsesIdRange;

  let idRange = null;
  if (needsIdRange) {
    if (!historyInfo.partitionId) {
      throw new Error('Invalid history partition id');
    }

    idRange = getHistoryIdRange(historyInfo.partitionId, queryStart);
  }

  const sourceQueries = [];
  const bindValues = [intervalMs];

  sourceQueries.push(buildHistorySourceQuery('metrics_history', currentUsesIdRange, columns));
  if (currentUsesIdRange) {
    bindValues.push(idRange.startId, idRange.endId);
  } else {
    bindValues.push(serverId, queryStart);
  }

  if (oldTableExists) {
    debug('[History] 跨周查询，合并 metrics_history 和 metrics_history_old');
    sourceQueries.push(buildHistorySourceQuery('metrics_history_old', oldUsesIdRange, columns));
    if (oldUsesIdRange) {
      bindValues.push(idRange.startId, idRange.endId);
    } else {
      bindValues.push(serverId, queryStart);
    }
  }

  const rawResult = await db.prepare(`
    WITH sampled AS (
      SELECT
        timestamp,
        ${columns},
        ROW_NUMBER() OVER (
          PARTITION BY CAST(timestamp / ? AS INTEGER)
          ORDER BY timestamp
        ) AS rn
      FROM (
        ${sourceQueries.join('\n        UNION ALL\n')}
      )
    )
    SELECT timestamp, ${columns}
    FROM sampled
    WHERE rn = 1
  `).bind(...bindValues).all();

  const result = rawResult.results.map(row => ({
    ...row,
    timestamp: Number(row.timestamp)
  }));

  result.sort((a, b) => a.timestamp - b.timestamp);

  setMetricsHistoryCache(serverId, hours, columns, result);

  debug(`[History] FINAL: ${result.length}, interval: ${intervalMs}ms`);

  return result;
}


async function weeklyCleanup(db) {
  try {
    debug('[Cleanup] 开始执行表轮换操作...');
    
    // 判断metrics_history有无索引
    const index = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='metrics_history'`
    ).first();
    if(!index){
      await saveSiteOptions(db, { history_id_optimized: 'true' });
      debug('✅ 切换到优化模式');
    }else{
      debug('✅ 继续兼容模式');
    }
    
    // 1. 删除旧的 metrics_history_old 表（如果存在）
    await db.prepare(`DROP TABLE IF EXISTS metrics_history_old`).run();
    debug('[Cleanup] 已删除旧的 metrics_history_old 表');
    
    // 2. 将 metrics_history 重命名为 metrics_history_old
    const currentTable = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history'`
    ).first();
    
    if (currentTable) {
      await db.prepare(`ALTER TABLE metrics_history RENAME TO metrics_history_old`).run();
      debug('[Cleanup] 已将 metrics_history 重命名为 metrics_history_old');
    }
  
    // 3. 重新初始化数据库以创建新的 metrics_history 表
    dbInitialized = false;
    await initDatabase(db);

    debug('[Cleanup] 已创建新的 metrics_history 表');
    
    return {
      success: true,
      message: '表轮换成功'
    };
  } catch (e) {
    console.error('[Cleanup] 表轮换失败:', e);
    return { success: false, error: e.message };
  }
}

async function saveMetricsHistory(db, serverId, historyPartitionId, metrics, regionCode = '', timestamp = null) {
  try {
    const historyId = buildHistoryId(historyPartitionId, timestamp);
    const rawTimestamp = Number(timestamp);
    const now = Number.isFinite(rawTimestamp) && rawTimestamp > 0
      ? (rawTimestamp < 10000000000 ? rawTimestamp * 1000 : rawTimestamp)
      : Date.now();
    
    const parsePing = (val) => {
      if (val === '' || val === null || val === undefined) return null;
      const num = parseInt(val);
      return (num > 0) ? num : null;
    };

    const parseLoss = (val) => {
      if (val === '' || val === null || val === undefined) return null;
      const num = parseInt(val);
      if (Number.isNaN(num)) return null;
      return Math.max(0, Math.min(100, num));
    };
    
    await db.prepare(`
      INSERT INTO metrics_history (
        id, server_id, timestamp, cpu, load_avg,
        net_in_speed, net_out_speed, net_rx, net_tx,
        processes, tcp_conn, udp_conn,
        ping_ct, ping_cu, ping_cm, ping_bd,
        loss_ct, loss_cu, loss_cm, loss_bd,
        ram_total, ram_used, swap_total, swap_used,
        disk_total, disk_used,
        cpu_cores, cpu_info, gpu, gpu_info, arch, os, region, ip_v4, ip_v6, boot_time,
        net_rx_monthly, net_tx_monthly
      ) VALUES (
        ?,?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?
      )
    `).bind(
      historyId,
      serverId,
      now,
      parseFloat(metrics.cpu) || 0,
      metrics.load || metrics.load_avg || '0 0 0',
      parseFloat(metrics.net_in_speed) || 0,
      parseFloat(metrics.net_out_speed) || 0,
      parseFloat(metrics.net_rx) || 0,
      parseFloat(metrics.net_tx) || 0,
      parseInt(metrics.processes) || 0,
      parseInt(metrics.tcp_conn) || 0,
      parseInt(metrics.udp_conn) || 0,
      parsePing(metrics.ping_ct),
      parsePing(metrics.ping_cu),
      parsePing(metrics.ping_cm),
      parsePing(metrics.ping_bd),
      parseLoss(metrics.loss_ct),
      parseLoss(metrics.loss_cu),
      parseLoss(metrics.loss_cm),
      parseLoss(metrics.loss_bd),
      parseFloat(metrics.ram_total) || 0,
      parseFloat(metrics.ram_used) || 0,
      parseFloat(metrics.swap_total) || 0,
      parseFloat(metrics.swap_used) || 0,
      parseFloat(metrics.disk_total) || 0,
      parseFloat(metrics.disk_used) || 0,
      parseInt(metrics.cpu_cores) || 0,
      metrics.cpu_info || '',
      metrics.gpu === '' || metrics.gpu === null || metrics.gpu === undefined ? null : (parseFloat(metrics.gpu) || 0),
      metrics.gpu_info || '',
      metrics.arch || '',
      metrics.os || '',
      regionCode,
      metrics.ip_v4 || '0',
      metrics.ip_v6 || '0',
      metrics.boot_time || '',
      parseFloat(metrics.net_rx_monthly) || 0,
      parseFloat(metrics.net_tx_monthly) || 0
    ).run();
  } catch (e) {
    // 检测是否是 "has no column" 错误，如果是则添加缺失字段
    if (e.message && /has no column/i.test(e.message)) {
      console.warn('检测到数据库字段缺失，尝试添加缺失字段...');
      await addHistoryColumns(db);
      return;
    }
    console.error('保存历史数据失败:', e);
  }
}

async function getLatestMetrics(db, serverId, server = null) {
  try {
    const historyInfo = await getServerHistoryInfo(db, serverId, server);
    if (!historyInfo.partitionId) {
      throw new Error('Invalid history partition id');
    }

    const useIdFilter = await isHistoryOptimized(db);

    const rangeStart = historyInfo.startTimestamp > 0 ? historyInfo.startTimestamp : null;
    const { startId, endId } = getHistoryIdRange(historyInfo.partitionId, rangeStart);
    debug(`Server ${serverId} history_id_range: ${startId} - ${endId}`);
  
    const result = useIdFilter ? await db.prepare(`
      SELECT * FROM metrics_history
      WHERE id >= ?
        AND id <= ?
      ORDER BY id DESC
      LIMIT 1
    `).bind(startId, endId).first()
    :await db.prepare(`
      SELECT * FROM metrics_history
      WHERE server_id = ?
      ORDER BY timestamp DESC
      LIMIT 1
    `).bind(serverId).first();
    return result || null;
  } catch (e) {
    console.error('获取最新指标数据失败:', e);
    return null;
  }
}

async function getLatestMetricsForAllServers(db) {
  const now = Date.now();
  const cacheInfo = getLatestMetricsCache();
  if (cacheInfo.cache && now - cacheInfo.time < cacheInfo.ttl) {
    return cacheInfo.cache;
  }

  // 确保 metrics_history 表有 idx_history_server_time 索引
  await ensureHistoryIndex(db);

  try {
    const servers = await getAllServers(db);

    const entries = await Promise.all(
      servers.map(s =>
        getLatestMetrics(db, s.id, s).then(metrics => [s.id, metrics])
      )
    );

    const result = new Map(entries.filter(([, m]) => m !== null));
    setLatestMetricsCache(result);
    return result;
  } catch (e) {
    console.error('获取所有服务器最新指标数据失败:', e);
    const cacheInfo = getLatestMetricsCache();
    return cacheInfo.cache || new Map();
  }
}

__mod.initDatabase = initDatabase;
__mod.clearHistory = clearHistory;
__mod.getMetricsHistory = getMetricsHistory;
__mod.weeklyCleanup = weeklyCleanup;
__mod.saveMetricsHistory = saveMetricsHistory;
__mod.getLatestMetrics = getLatestMetrics;
__mod.getLatestMetricsForAllServers = getLatestMetricsForAllServers;
}

// /src/utils/cache.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_cache_js"] = {};
/**
 * 缓存管理模块
 * 集中管理所有内存缓存，包括：
 * - 服务器列表缓存
 * - 服务器详情（复用服务器列表缓存）
 * - 最新指标缓存
 * - 历史指标缓存
 * - 站点设置缓存
 */


const SERVERS_LIST_TTL = 120 * 1000;
let serversListCache = null;

const LATEST_ALL_TTL = 30 * 1000;
let latestAllCache = null;
let latestAllCacheTime = 0;

const metricsHistoryCache = new Map();

const serverDetailCache = new Map();

function getCacheDuration(hours) {
  if (hours >= 120) {
    return 10 * 60 * 1000;
  } else if (hours >= 60) {
    return 5 * 60 * 1000;
  } else if (hours >= 30) {
    return 3 * 60 * 1000;
  } else {
    return 1 * 60 * 1000;
  }
}

function filterServersByHidden(servers, includeHidden) {
  if (!servers || servers.length === 0) return [];
  if (includeHidden) {
    return [...servers];
  }
  return servers.filter(s => s.is_hidden !== 1 && s.is_hidden !== '1');
}

async function getAllServers(db, includeHidden = true) {
  const now = Date.now();
  
  if (serversListCache && now - serversListCache.time < SERVERS_LIST_TTL) {
    debug('服务器列表缓存命中');
    return filterServersByHidden(serversListCache.data, includeHidden);
  }

  try {
    const { results } = await db.prepare('SELECT * FROM servers ORDER BY sort_order ASC').all();
    serversListCache = { data: results, time: now };
    debug('服务器列表缓存更新');
    return filterServersByHidden(results, includeHidden);
  } catch (e) {
    debug('获取服务器列表失败:', e);
    return filterServersByHidden(serversListCache?.data, includeHidden);
  }
}

function clearServersListCache() {
  serversListCache = null;
  serverDetailCache.clear();
}

function clearServerDetailCache() {
  serverDetailCache.clear();
}

async function getServerDetail(db, id, includeHidden = false) {
  const now = Date.now();
  const cached = serverDetailCache.get(id);
  
  if (cached) {
    if (now - cached.time < SERVERS_LIST_TTL) {
      debug('服务器详情缓存命中');
      const server = cached.data;
      
      if (!server) {
        return null;
      }
      
      if (!includeHidden && (server.is_hidden === 1 || server.is_hidden === '1')) {
        return null;
      }
      
      return { ...server };
    }
    
    serverDetailCache.delete(id);
  }
  
  const server = await db.prepare('SELECT * FROM servers WHERE id = ?').bind(id).first();

  serverDetailCache.set(id, { data: server, time: now });
  debug('服务器详情缓存更新');
  
  if (!server) {
    return null;
  }
  
  if (!includeHidden && (server.is_hidden === 1 || server.is_hidden === '1')) {
    return null;
  }
  
  return { ...server };
}

async function checkServerExists(db, id) {
  const server = await getServerDetail(db, id, true);
  return !!server;
}

/**
 * 获取最新指标缓存信息
 * @returns {object} 包含 cache、time、ttl 字段的对象
 */
function getLatestMetricsCache() {
  return { cache: latestAllCache, time: latestAllCacheTime, ttl: LATEST_ALL_TTL };
}

function setLatestMetricsCache(data) {
  latestAllCache = data;
  latestAllCacheTime = Date.now();
}

function clearLatestMetricsCache() {
  latestAllCache = null;
  latestAllCacheTime = 0;
}

function getCacheKey(serverId, hours, columns) {
  const sortedColumns = columns.split(',').sort().join(',');
  return `${serverId}:${hours}:${sortedColumns}`;
}

function getMetricsHistoryCache(serverId, hours, columns) {
  const key = getCacheKey(serverId, hours, columns);
  return metricsHistoryCache.get(key);
}

function setMetricsHistoryCache(serverId, hours, columns, data) {
  const key = getCacheKey(serverId, hours, columns);
  metricsHistoryCache.set(key, { data, timestamp: Date.now() });
}

function clearMetricsHistoryCache(serverId) {
  for (const key of metricsHistoryCache.keys()) {
    if (key.startsWith(`${serverId}:`)) {
      metricsHistoryCache.delete(key);
    }
  }
}

function clearAllCaches() {
  clearServersListCache();
  clearLatestMetricsCache();
  metricsHistoryCache.clear();
  clearSiteSettingsCache();
  clearAppearanceSettingsCache();
}

__mod.getCacheDuration = getCacheDuration;
__mod.getAllServers = getAllServers;
__mod.clearServersListCache = clearServersListCache;
__mod.clearServerDetailCache = clearServerDetailCache;
__mod.getServerDetail = getServerDetail;
__mod.checkServerExists = checkServerExists;
__mod.getLatestMetricsCache = getLatestMetricsCache;
__mod.setLatestMetricsCache = setLatestMetricsCache;
__mod.clearLatestMetricsCache = clearLatestMetricsCache;
__mod.getMetricsHistoryCache = getMetricsHistoryCache;
__mod.setMetricsHistoryCache = setMetricsHistoryCache;
__mod.clearMetricsHistoryCache = clearMetricsHistoryCache;
__mod.clearAllCaches = clearAllCaches;
}

// /src/utils/settings.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_settings_js"] = {};
const CURRENT_VERSION = 'V2.7.11 Beta';
const DEFAULT_SITE_TITLE = 'Cloudflare Server Monitor';
const APPEARANCE_FIELDS = ['site_title', 'custom_bg', 'custom_head', 'custom_script', 'csp_static', 'csp_api'];
const SITE_FIELDS = ['is_public', 'show_price', 'show_expire', 'show_tf', 'show_time', 'show_long_history', 'tg_notify', 'tg_bot_token', 'tg_chat_id', 'turnstile_enabled', 'turnstile_login_enabled', 'turnstile_site_key', 'turnstile_secret_key', 'jwt_secret', 'username', 'password', 'cloudflare_account_id', 'cloudflare_token', 'custom_ct', 'custom_cu', 'custom_cm', 'custom_bd', 'expire_reminder','history_id_optimized','servers_optimized'];

const SITE_SETTINGS_TTL = 120 * 1000;
let cachedSiteSettings = null;
let siteSettingsCacheExpiry = 0;
let cachedAppearanceOptions = null;
let appearanceOptionsCacheExpiry = 0;

const defaults = {
  site_title: DEFAULT_SITE_TITLE,
  custom_bg: '',
  custom_head: '',
  custom_script: '',
  csp_static: '',
  csp_api: '',
  is_public: 'true',
  show_price: 'true',
  show_expire: 'true',
  show_tf: 'true',
  show_time: 'true',
  show_long_history: 'false',
  tg_notify: 'false',
  tg_bot_token: '',
  tg_chat_id: '',
  turnstile_enabled: 'false',
  turnstile_login_enabled: 'false',
  turnstile_site_key: '',
  turnstile_secret_key: '',
  cloudflare_account_id: '',
  cloudflare_token: '',
  custom_ct: 'gd-ct-dualstack.ip.zstaticcdn.com',
  custom_cu: 'gd-cu-dualstack.ip.zstaticcdn.com',
  custom_cm: 'gd-cm-dualstack.ip.zstaticcdn.com',
  custom_bd: 'lf3-ips.zstaticcdn.com',
  expire_reminder: 'false',
  history_id_optimized: 'false',
  servers_optimized: 'false'
};

function tryParseJSON(str) {
  if (!str) return null;
  try {
    return JSON.parse(str);
  } catch (e) {
    return null;
  }
}

function copyFields(target, source, fields) {
  if (!source || typeof source !== 'object') return;
  for (const field of fields) {
    if (source[field] !== undefined) {
      target[field] = source[field];
    }
  }
}

function hasMissingFields(source, fields) {
  if (!source || typeof source !== 'object') return true;
  return fields.some(field => source[field] === undefined);
}

async function loadLegacySettings(db, fields) {
  const legacy = {};
  const fieldSet = new Set(fields);
  const { results } = await db.prepare('SELECT * FROM settings').all();
  if (results && results.length > 0) {
    results.forEach(r => {
      if (fieldSet.has(r.key)) {
        legacy[r.key] = r.value;
      }
    });
  }
  return legacy;
}

async function loadSiteSettings(db) {
  const now = Date.now();
  if (cachedSiteSettings && now < siteSettingsCacheExpiry) {
    debug('Settings缓存命中');
    return cachedSiteSettings;
  }
  debug('Settings缓存更新');

  const result = { ...defaults };
  let siteOptions = null;

  try {
    const siteRow = await db.prepare(
      "SELECT value FROM settings WHERE key = 'site_options'"
    ).first();
    if (siteRow) {
      const parsed = tryParseJSON(siteRow.value);
      if (parsed) {
        siteOptions = parsed;
      }
    }

    if (hasMissingFields(siteOptions, SITE_FIELDS)) {
      copyFields(result, await loadLegacySettings(db, SITE_FIELDS), SITE_FIELDS);
    }
    copyFields(result, siteOptions, SITE_FIELDS);
  } catch (e) {
    console.error('加载站点设置失败:', e);
  }

  cachedSiteSettings = result;
  siteSettingsCacheExpiry = now + SITE_SETTINGS_TTL;
  return result;
}

function clearSiteSettingsCache() {
  cachedSiteSettings = null;
  siteSettingsCacheExpiry = 0;
}

async function loadAppearanceOptions(db) {
  const now = Date.now();
  if (cachedAppearanceOptions && now < appearanceOptionsCacheExpiry) {
    debug('Appearance缓存命中');
    return cachedAppearanceOptions;
  }
  debug('Appearance缓存更新');

  const result = {};
  copyFields(result, defaults, APPEARANCE_FIELDS);
  let appearanceOptions = null;

  try {
    const appearanceRow = await db.prepare(
      "SELECT value FROM settings WHERE key = 'appearance_options'"
    ).first();
    if (appearanceRow) {
      const parsed = tryParseJSON(appearanceRow.value);
      if (parsed) {
        appearanceOptions = parsed;
      }
    }

    const needsLegacyAppearance = hasMissingFields(appearanceOptions, APPEARANCE_FIELDS);
    if (needsLegacyAppearance) {
      copyFields(result, await loadLegacySettings(db, APPEARANCE_FIELDS), APPEARANCE_FIELDS);
    }
    copyFields(result, appearanceOptions, APPEARANCE_FIELDS);
  } catch (e) {
    console.error('加载外观设置失败:', e);
  }

  cachedAppearanceOptions = result;
  appearanceOptionsCacheExpiry = now + SITE_SETTINGS_TTL;
  return result;
}

function clearAppearanceSettingsCache() {
  cachedAppearanceOptions = null;
  appearanceOptionsCacheExpiry = 0;
}

async function loadSettings(db) {
  const [siteSettings, appearanceOptions] = await Promise.all([
    loadSiteSettings(db),
    loadAppearanceOptions(db)
  ]);
  return { ...defaults, ...siteSettings, ...appearanceOptions };
}

async function saveSiteOptions(db, updates) {
  const siteRow = await db.prepare(
    "SELECT value FROM settings WHERE key = 'site_options'"
  ).first();
  
  const existingSiteOptions = siteRow && siteRow.value
    ? tryParseJSON(siteRow.value) || {}
    : {};
  const legacySiteOptions = hasMissingFields(existingSiteOptions, SITE_FIELDS)
    ? await loadLegacySettings(db, SITE_FIELDS)
    : {};
  
  const siteOptions = { ...legacySiteOptions, ...existingSiteOptions, ...updates };
  
  await db.prepare(
    'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
  ).bind('site_options', JSON.stringify(siteOptions)).run();
  
  clearSiteSettingsCache();
  return siteOptions;
}

async function getSettingByKey(db, key, returnBoolean = false) {
  const settings = await loadSiteSettings(db);
  if(returnBoolean){
    const value = String(settings[key] ?? '').trim().toLowerCase();
    if(['true', '1', 'yes', 'on'].includes(value)) return true;
    if(['false', '0', 'no', 'off', ''].includes(value)) return false;
  }
  return settings[key];
}

let isDebugEnabled = false;

function setDebug(debug) {
  isDebugEnabled = debug === 1 || debug === '1' || debug === true;
  if(isDebugEnabled) console.log('DEBUG模式:', isDebugEnabled);
}

function debug(...args) {
  if (isDebugEnabled) {
    console.debug('[DEBUG]', ...args);
  }
}

function getCurrentVersion() {
  return CURRENT_VERSION;
}

__mod.loadSiteSettings = loadSiteSettings;
__mod.DEFAULT_SITE_TITLE = DEFAULT_SITE_TITLE;
__mod.clearSiteSettingsCache = clearSiteSettingsCache;
__mod.APPEARANCE_FIELDS = APPEARANCE_FIELDS;
__mod.loadAppearanceOptions = loadAppearanceOptions;
__mod.SITE_FIELDS = SITE_FIELDS;
__mod.clearAppearanceSettingsCache = clearAppearanceSettingsCache;
__mod.loadSettings = loadSettings;
__mod.saveSiteOptions = saveSiteOptions;
__mod.getSettingByKey = getSettingByKey;
__mod.setDebug = setDebug;
__mod.debug = debug;
__mod.getCurrentVersion = getCurrentVersion;
}

// /src/database/indexOptimization.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_database_indexOptimization_js"] = {};


const HISTORY_PARTITION_MULTIPLIER = 10000000000000;
const HISTORY_AUTO_OPTIMIZED_MIN_ID = HISTORY_PARTITION_MULTIPLIER;
const HISTORY_MAX_PARTITION_ID = 900;
const HISTORY_MAX_TIME_KEY = 991231235959;

// 确保servers历史记录分区优化
async function ensureServerOptimization(db) {
  const optimized = await getSettingByKey(db, 'servers_optimized', true);
  const { results: columns = [] } = await db.prepare(`PRAGMA table_info(servers)`).all();
  const existingColumns = new Set(columns.map(column => column.name));
  let addedColumns = 0;

  if (!existingColumns.has('history_partition_id')) {
    await db.prepare(`ALTER TABLE servers ADD COLUMN history_partition_id INTEGER DEFAULT 0`).run();
    addedColumns++;
    debug('history_partition_id 字段已添加');
  }

  if (!existingColumns.has('timestamp')) {
    await db.prepare(`ALTER TABLE servers ADD COLUMN timestamp INTEGER DEFAULT 0`).run();
    addedColumns++;
    debug('timestamp 字段已添加');
  }

  if (addedColumns > 0) {
    clearServersListCache();
  }

  if (optimized && addedColumns === 0) {
    debug('服务器历史记录分区已优化');
    return { success: true, assigned: 0 };
  }

  const { results: servers = [] } = await db.prepare(`
    SELECT id, history_partition_id
    FROM servers
    ORDER BY id ASC
  `).all();
  
  if (servers.length === 0) {
    debug('没有服务器需要优化');
    await saveSiteOptions(db, { servers_optimized: 'true' });
    return { success: true, assigned: 0 };
  }

  if (servers.length > HISTORY_MAX_PARTITION_ID) {
    throw new Error(`No available history partition id; max is ${HISTORY_MAX_PARTITION_ID}`);
  }

  let updated = 0;

  for (let i = 0; i < servers.length; i++) {
    const server = servers[i];
    const partitionId = i + 1;
    if (Number(server.history_partition_id) === partitionId) {
      continue;
    }

    try {
      await db.prepare(
        `UPDATE servers SET history_partition_id = ? WHERE id = ?`
      ).bind(partitionId, server.id).run();
      updated++;
    } catch (e) {
      debug(`Failed to update server ${server.id} history_partition_id: ${e.message}`);
    }
  }

  // 清空服务器列表的缓存
  clearServersListCache();

  debug(`服务器历史记录分区优化完成，更新了 ${updated} 条记录`);
  
  // 标记为已优化
  await saveSiteOptions(db, { servers_optimized: 'true' });

  return { success: true, assigned: updated };
}

// 获取下一个可用的历史记录分区ID
async function getNextServerHistoryPartitionId(db) {
  const servers = await getAllServers(db, true);
  const usedIds = new Set(
    servers
      .map(s => Number(s.history_partition_id))
      .filter(id => Number.isInteger(id) && id > 0 && id <= HISTORY_MAX_PARTITION_ID)
  );
  
  for (let id = 1; id <= HISTORY_MAX_PARTITION_ID; id++) {
    if (!usedIds.has(id)) return id;
  }
  debug(`No available history partition id`);
  throw new Error(`No available history partition id`);
}

function padHistoryTimePart(value) {
  return String(value).padStart(2, '0');
}

// 格式化历史记录时间戳
function normalizeHistoryTimestamp(value, fallback = Date.now()) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return fallback;
  return ts < 10000000000 ? ts * 1000 : ts;
}

function formatHistoryTimeKey(timestamp) {
  const normalized = normalizeHistoryTimestamp(timestamp);

  const date = new Date(normalized);
  const year = date.getUTCFullYear();
  if (year < 2000 || year > 2099) {
    debug(`Invalid year ${year} for history time key`);
    throw new Error(`Invalid year ${year} for history time key`);
  };

  return Number([
    padHistoryTimePart(year % 100),
    padHistoryTimePart(date.getUTCMonth() + 1),
    padHistoryTimePart(date.getUTCDate()),
    padHistoryTimePart(date.getUTCHours()),
    padHistoryTimePart(date.getUTCMinutes()),
    padHistoryTimePart(date.getUTCSeconds())
  ].join(''));
}

function normalizeHistoryPartitionId(value) {
  const partitionId = Number(value);
  if (!Number.isInteger(partitionId) || partitionId <= 0 || partitionId > HISTORY_MAX_PARTITION_ID) {
    return null;
  }
  return partitionId;
}

function buildHistoryId(partitionId, timestamp) {
  const normalizedPartitionId = normalizeHistoryPartitionId(partitionId);
  if (!normalizedPartitionId) {
    throw new Error('Invalid history partition id');
  }
  return normalizedPartitionId * HISTORY_PARTITION_MULTIPLIER + formatHistoryTimeKey(timestamp);
}

async function getServerHistoryInfo(db, serverId, server = null) {
  const target = server && server.id === serverId
    ? server
    : (await getAllServers(db, true)).find(s => s.id === serverId);

  if (!target) {
    debug(`Server ${serverId} not found`);
    throw new Error(`Server ${serverId} not found`);
  }

  return {
    partitionId: normalizeHistoryPartitionId(target.history_partition_id),
    startTimestamp: normalizeHistoryTimestamp(target.timestamp, 0)
  };
}

function getHistoryIdRange(partitionId, startTimestamp = null, endTimestamp = null) {
  const normalizedPartitionId = normalizeHistoryPartitionId(partitionId);
  if (!normalizedPartitionId) {
    throw new Error('Invalid history partition id');
  }

  const prefix = normalizedPartitionId * HISTORY_PARTITION_MULTIPLIER;
  return {
    startId: prefix + (startTimestamp === null || startTimestamp === undefined
      ? 0
      : formatHistoryTimeKey(startTimestamp)),
    endId: prefix + (endTimestamp === null || endTimestamp === undefined
      ? HISTORY_MAX_TIME_KEY
      : formatHistoryTimeKey(endTimestamp))
  };
}

__mod.ensureServerOptimization = ensureServerOptimization;
__mod.HISTORY_PARTITION_MULTIPLIER = HISTORY_PARTITION_MULTIPLIER;
__mod.getNextServerHistoryPartitionId = getNextServerHistoryPartitionId;
__mod.HISTORY_AUTO_OPTIMIZED_MIN_ID = HISTORY_AUTO_OPTIMIZED_MIN_ID;
__mod.normalizeHistoryTimestamp = normalizeHistoryTimestamp;
__mod.HISTORY_MAX_PARTITION_ID = HISTORY_MAX_PARTITION_ID;
__mod.formatHistoryTimeKey = formatHistoryTimeKey;
__mod.HISTORY_MAX_TIME_KEY = HISTORY_MAX_TIME_KEY;
__mod.normalizeHistoryPartitionId = normalizeHistoryPartitionId;
__mod.buildHistoryId = buildHistoryId;
__mod.getServerHistoryInfo = getServerHistoryInfo;
__mod.getHistoryIdRange = getHistoryIdRange;
}

// /src/database/updateDatabase.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_database_updateDatabase_js"] = {};

async function updateDatabase(db) {
  debug('开始执行数据库更新...');
  const results = [];
  
  try {
    const historyIndex = await ensureHistoryIndex(db);
    results.push({ name: 'metrics_history 索引检查', ...historyIndex });
    
    const serversCols = await addServerColumns(db);
    results.push({ name: 'servers 表列更新', ...serversCols });
    
    const cleanupServers = await cleanupServerExtraColumns(db);
    results.push({ name: 'servers 表多余字段清理', ...cleanupServers });
    
    const historyCols = await addHistoryColumns(db);
    results.push({ name: 'metrics_history 表列更新', ...historyCols });

    // 无需清理metrics_history多余字段，消耗过大，不影响使用，每周执行weeklyCleanup的时候会自动清理
    
    const staleCleanup = await cleanupStaleSettings(db);
    results.push({ name: '废弃 settings key 清理', ...staleCleanup });
    
    const dropAggregated = await dropMetricsAggregatedTable(db);
    results.push({ name: '删除弃用的 metrics_aggregated 表', ...dropAggregated });
    
    debug('✅ 数据库更新完成');
    
    return {
      success: true,
      message: 'databaseUpgradeSuccess',
      results
    };
  } catch (e) {
    debug('❌ 数据库更新失败:', e);
    return {
      success: false,
      message: 'databaseUpgradeFailed',
      error: e.message,
      results
    };
  }
}

async function isHistoryOptimized(db) {
  const history_id_optimized = await getSettingByKey(db, 'history_id_optimized', true);
  if(history_id_optimized) return true;
  const minId = await db.prepare(`
    SELECT id AS min_id
    FROM metrics_history
    ORDER BY id ASC
    LIMIT 1
  `).first();
  if(!minId) return true;  // 空表，视为已优化
  return minId.min_id > 10000000000000;
}

// 确保 旧版metrics_history 表有索引
async function ensureHistoryIndex(db) {
  const history_id_optimized = await getSettingByKey(db, 'history_id_optimized', true);
  if(history_id_optimized) {
    debug('metrics_history 表已优化，无需创建索引');
    return { success: true, created: false, message: 'metrics_history 表已优化，无需创建索引'};
  }
  
  try {
    const index = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='metrics_history'`
    ).first();

    if (index) {
      debug('索引已存在无需创建');
      return { success: true, created: false, message: '索引已存在' };
    }

    // 获取最小id
     const minId = await db.prepare(`
      SELECT id AS min_id
      FROM metrics_history
      ORDER BY id ASC
      LIMIT 1
    `).first();

    if (!minId || minId.min_id > 10000000000000) {
      debug('metrics_history 表为空或已优化，无需创建索引');
      return {
        success: true,
        created: false,
        message: 'metrics_history 表为空或已优化，无需创建索引'
      };
    }

    const idxName = 'idx_history_server_time_' + Math.random().toString(36).substring(2);
    await db.prepare(`DROP INDEX IF EXISTS ${idxName}`).run();
    await db.prepare(`
      CREATE INDEX IF NOT EXISTS ${idxName} 
      ON metrics_history(server_id, timestamp)
    `).run();
    debug(`✅ 已创建索引 ${idxName}`);

    return { success: true, created: true, message: '已创建索引' };
  } catch (e) {
    debug('检查/创建 metrics_history 索引失败:', e);
    return { success: false, error: e.message };
  }
}

async function addServerColumns(db) {
  try {
    const { results: columns } = await db.prepare(`PRAGMA table_info(servers)`).all();
    const existingCols = columns.map(c => c.name);
    
    const newCols = {
      is_hidden: "TEXT DEFAULT '0'",
      offline_notify_disabled: "TEXT DEFAULT '0'",
      sort_order: "INTEGER DEFAULT 0",
      tags: "TEXT DEFAULT ''",
      note: "TEXT DEFAULT ''",
      reset_day: "INTEGER DEFAULT 1",
      collect_interval: "INTEGER DEFAULT 0",
      report_interval: "INTEGER DEFAULT 60",
      ping_mode: "TEXT DEFAULT 'tcp'",
      custom_ct: "TEXT DEFAULT ''",
      custom_cu: "TEXT DEFAULT ''",
      custom_cm: "TEXT DEFAULT ''",
      custom_bd: "TEXT DEFAULT ''",
      rx_correction: "REAL DEFAULT NULL",
      tx_correction: "REAL DEFAULT NULL",
      traffic_calc_type: "TEXT DEFAULT 'total'",
      history_partition_id: "INTEGER DEFAULT 0",
      timestamp: "INTEGER DEFAULT 0"
    };
    
    let added = 0;
    for (const [colName, colDef] of Object.entries(newCols)) {
      if (!existingCols.includes(colName)) {
        await db.prepare(`ALTER TABLE servers ADD COLUMN ${colName} ${colDef}`).run();
        added++;
      }
    }
    
    return { success: true, added };
  } catch (e) {
    debug('添加 servers 表列失败:', e);
    return { success: false, error: e.message };
  }
}

async function cleanupServerExtraColumns(db) {
  try {
    const { results: columns } = await db.prepare(`PRAGMA table_info(servers)`).all();
    const existingCols = columns.map(c => c.name);
    
    const extraCols = ['cpu', 'ram', 'disk', 'load_avg', 'uptime', 'last_updated', 'ram_total', 'net_rx', 'net_tx', 'net_in_speed', 'net_out_speed', 'os', 'cpu_info', 'cpu_cores' , 'arch' ,'boot_time', 'ram_used', 'swap_total', 'swap_used', 'disk_total', 'disk_used', 'processes', 'tcp_conn', 'udp_conn', 'country', 'ip_v4', 'ip_v6', 'ping_ct', 'ping_cu', 'ping_cm', 'ping_bd', 'monthly_rx', 'monthly_tx', 'last_rx', 'last_tx', 'reset_month', 'bandwidth'];
    const colsToDrop = extraCols.filter(col => existingCols.includes(col));
    
    if (colsToDrop.length === 0) {
      return { success: true, cleaned: 0, message: '无需清理（没有多余字段）' };
    }
    
    for (const col of colsToDrop) {
      await db.prepare(`ALTER TABLE servers DROP COLUMN ${col}`).run();
      debug(`✅ 已删除 servers 表的 ${col} 字段`);
    }
    
    return { success: true, cleaned: colsToDrop.length, message: `已删除 ${colsToDrop.join(', ')} 字段` };
  } catch (e) {
    debug('清理 servers 表多余字段失败:', e);
    return { success: false, error: e.message };
  }
}

async function addHistoryColumns(db) {
  try {
    const { results: historyColumns } = await db.prepare(`PRAGMA table_info(metrics_history)`).all();
    const existingHistoryCols = historyColumns.map(c => c.name);
    
    const newHistoryCols = {
      cpu_cores: "INTEGER DEFAULT 0",
      cpu_info: "TEXT DEFAULT ''",
      gpu: "REAL DEFAULT NULL",
      gpu_info: "TEXT DEFAULT ''",
      arch: "TEXT DEFAULT ''",
      os: "TEXT DEFAULT ''",
      region: "TEXT DEFAULT ''",
      ip_v4: "TEXT DEFAULT '0'",
      ip_v6: "TEXT DEFAULT '0'",
      boot_time: "TEXT DEFAULT ''",
      net_rx_monthly: "REAL DEFAULT 0",
      net_tx_monthly: "REAL DEFAULT 0",
      loss_ct: "INTEGER DEFAULT NULL",
      loss_cu: "INTEGER DEFAULT NULL",
      loss_cm: "INTEGER DEFAULT NULL",
      loss_bd: "INTEGER DEFAULT NULL"
    };
    
    let added = 0;
    for (const [colName, colDef] of Object.entries(newHistoryCols)) {
      if (!existingHistoryCols.includes(colName)) {
        await db.prepare(`ALTER TABLE metrics_history ADD COLUMN ${colName} ${colDef}`).run();
        added++;
      }
    }
    
    return { success: true, added };
  } catch (e) {
    debug('添加 metrics_history 表列失败:', e);
    return { success: false, error: e.message };
  }
}

async function dropMetricsAggregatedTable(db) {
  debug('开始删除弃用的 metrics_aggregated 表...');
  try {
    const { results: tables } = await db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_aggregated'`
    ).all();
    
    if (tables.length === 0) {
      return { success: true, dropped: 0, message: '无需删除（表不存在）' };
    }
    
    await db.prepare(`DROP TABLE metrics_aggregated`).run();
    debug('✅ 已删除 metrics_aggregated 表');
    return { success: true, dropped: 1, message: '已删除 metrics_aggregated 表' };
  } catch (e) {
    debug('删除 metrics_aggregated 表失败:', e);
    return { success: false, error: e.message };
  }
}

async function cleanupStaleSettings(db) {
  debug('开始清理废弃的 settings key...');
  try {
    const stalePrefixes = ['last_write_%'];
    const staleExact = [
      'theme',
      'custom_css',
      'auto_reset_traffic',
      'last_aggregated_to_120',
      'last_aggregated_to_240',
      'last_aggregated_to_480',
      'last_aggregated_to_960',
      'last_aggregated_to_1920',
      'site_title',
      'admin_title',
      'custom_head',
      'custom_script',
      'custom_bg',
      'is_public',
      'show_price',
      'show_expire',
      'show_tf',
      'show_time',
      'show_long_history',
      'tg_notify',
      'tg_bot_token',
      'tg_chat_id',
      'last_aggregated_to',
      'last_cleanup',
      'expire_reminder'
    ];
    const staleKeysWhere = stalePrefixes.map(() => `key LIKE ?`).concat(staleExact.map(() => `key = ?`)).join(' OR ');
    const staleBindings = [...stalePrefixes, ...staleExact];
    const { meta: cleanupResult } = await db.prepare(
      `DELETE FROM settings WHERE ${staleKeysWhere}`
    ).bind(...staleBindings).run();
    if (cleanupResult.changes > 0) {
      debug(`已清理 ${cleanupResult.changes} 个废弃的 settings key`);
    }
    return { success: true, cleaned: cleanupResult.changes };
  } catch (e) {
    debug('清理废弃 settings key 失败:', e);
    return { success: false, error: e.message };
  }
}

__mod.updateDatabase = updateDatabase;
__mod.isHistoryOptimized = isHistoryOptimized;
__mod.ensureHistoryIndex = ensureHistoryIndex;
__mod.addServerColumns = addServerColumns;
__mod.addHistoryColumns = addHistoryColumns;
__mod.cleanupStaleSettings = cleanupStaleSettings;
}

// /src/services/notification.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_services_notification_js"] = {};



const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

async function fetchWithRetry(url, options, retries = MAX_RETRIES) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      
      if (i < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
      }
    } catch (e) {
      if (i < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
      } else {
        throw e;
      }
    }
  }
  throw new Error('Max retries exceeded');
}


async function sendNotification(settings, msg) {
  if(!settings.tg_bot_token) return;
  const title = "💌 Cloudflare Server Monitor";
  if(settings.tg_chat_id) {
    try {
      await fetchWithRetry(`https://api.telegram.org/bot${settings.tg_bot_token}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: settings.tg_chat_id,
          text: msg,
          parse_mode: 'Markdown'
        })
      });
    } catch (e) {
      return "Telegram 通知发送失败: " + e.message;
    }
  }else if(settings.tg_bot_token.includes("open.feishu.cn")) {
    try {
      await fetchWithRetry(settings.tg_bot_token, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
        body: JSON.stringify({
          msg_type: "interactive",
          card: {
            schema: "2.0",
            header: { template: "blue",  title: { content: title, tag: "plain_text" } },
            body: { elements: [{tag: "markdown", content: msg}] }
          }
        })
      });
    } catch (e) {
      return "飞书机器人通知发送失败: " + e.message;
    }
  }else if(settings.tg_bot_token.includes("https://api.day.app/") || settings.tg_bot_token.indexOf("bark:") == 0) {
    if(settings.tg_bot_token.indexOf("bark:") == 0) {
      settings.tg_bot_token = settings.tg_bot_token.replace("bark:", "");
    }
    try {
      await fetchWithRetry(settings.tg_bot_token, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: title,
          markdown: msg,
          group: "Cloudflare Server Monitor"
        })
      });
    } catch (e) {
      return "Bark通知发送失败: " + e.message;
    }
  }else if(settings.tg_bot_token.includes("https://qyapi.weixin.qq.com")){
    try {
      await fetchWithRetry(settings.tg_bot_token, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          msgtype: "markdown",
          markdown: { content: msg }
        })
      });
    } catch (e) {
      return "企业微信通知发送失败: " + e.message;
    }
  // Server 酱（使用 sendkey）
  }else if(settings.tg_bot_token.includes("https://sctapi.ftqq.com/")) {
    try {
      await fetchWithRetry(settings.tg_bot_token, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: title,
          desp: msg
        })
      });
    } catch (e) {
      return "Server酱通知发送失败: " + e.message;
    }
  }else if(settings.tg_bot_token.includes("https://wxpusher.zjiecode.com/api/send/message/SPT_")) {
    const match = settings.tg_bot_token.match(/\/message\/([^/]+)/);
    const spt = match ? match[1] : null;
    try {
      await fetchWithRetry("https://wxpusher.zjiecode.com/api/send/message/simple-push", {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          "content": msg,
          "summary": title,
          "contentType":3,
          "spt": spt,
        })
      });
    } catch (e) {
      return "WxPusher通知发送失败: " + e.message;
    }
  }else if(settings.tg_bot_token.includes("/message?token=")) {
    try {
      await fetchWithRetry(settings.tg_bot_token, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: title,
          message: msg,
          priority: 5,
          extras: {
            "client::display": { "contentType": "text/markdown" }
          }
        })
      });
    } catch (e) {
      return "Gotify通知发送失败: " + e.message;
    }
  }else {
    return "未知的通知方式";
  }
}

async function checkOfflineNodes(db) {
  const siteSettings = await loadSiteSettings(db);

  if (siteSettings.tg_notify !== 'true'|| !siteSettings.tg_bot_token) return;

  try {
    const allServers = await getAllServers(db);
    
    const latestMetricsMap = await getLatestMetricsForAllServers(db);
    
    let alertState = {};
    const stateRes = await db.prepare(
      "SELECT value FROM settings WHERE key = 'alert_state'"
    ).first();
    
    if (stateRes) {
      try {
        alertState = JSON.parse(stateRes.value);
      } catch (e) {
        alertState = {};
      }
    }

    const now = Date.now();
    const offlineNodes = [];
    const recoveredNodes = [];

    for (const s of allServers) {
      if (s.offline_notify_disabled === '1') continue;

      const latestMetrics = latestMetricsMap.get(s.id);
      
      let isOffline = true;
      if (latestMetrics) {
        const diff = now - latestMetrics.timestamp;
        isOffline = diff > 300000;
      }

      if (isOffline && !alertState[s.id]) {
        offlineNodes.push(s);
        alertState[s.id] = true;
      } else if (!isOffline && alertState[s.id]) {
        recoveredNodes.push(s);
        delete alertState[s.id];
      }
    }

    if (offlineNodes.length > 0) {
      const nodeList = offlineNodes.map(n => `• ${n.name}`).join('\n');
      const msg = `⚠️ **节点离线告警** (${offlineNodes.length}个)\n\n${nodeList}\n\n**时间:** ${new Date().toLocaleString('zh-CN', {timeZone: 'Asia/Shanghai'})}`;
      await sendNotification(siteSettings, msg);
    }

    if (recoveredNodes.length > 0) {
      const nodeList = recoveredNodes.map(n => `• ${n.name}`).join('\n');
      const msg = `✅ **节点恢复通知** (${recoveredNodes.length}个)\n\n${nodeList}\n\n**时间:** ${new Date().toLocaleString('zh-CN', {timeZone: 'Asia/Shanghai'})}`;
      await sendNotification(siteSettings, msg);
    }

    if (offlineNodes.length > 0 || recoveredNodes.length > 0) {
      await db.prepare(
        'INSERT INTO settings (key, value) VALUES ("alert_state", ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
      ).bind(JSON.stringify(alertState)).run();
    }
  } catch (e) {
    console.error('离线检测失败:', e);
  }
}

async function checkExpiringServers(db) {
  const siteSettings = await loadSiteSettings(db);

  if (siteSettings.expire_reminder !== 'true' || !siteSettings.tg_bot_token) {
    return;
  }
  try {
    const allServers = await getAllServers(db);
    const now = Date.now();
    const REMINDER_DAYS = 7;
    const expiringServers = [];

    for (const s of allServers) {
      if (!s.expire_date) continue;

      const expTime = new Date(s.expire_date).getTime();
      if (isNaN(expTime)) continue;

      const diff = expTime - now;
      const days = Math.ceil(diff / (1000 * 3600 * 24));

      debug(`[Cron] 检测到服务器 ${s.name} 到期日期 ${s.expire_date}，剩余天数 ${days} 天`);

      if (days > 0 && days <= REMINDER_DAYS) {
        expiringServers.push({ name: s.name, expire_date: s.expire_date, days });
      }
    }

    if (expiringServers.length > 0) {
      const serverList = expiringServers.map(s => `• ${s.name} - 剩余${s.days}天 (${s.expire_date})`).join('\n');
      const msg = `⏰ **服务器到期提醒** (${expiringServers.length}个)\n\n${serverList}`;
      debug(`[Cron] 发送到期提醒通知: ${msg}`);
      await sendNotification(siteSettings, msg);
    }
  } catch (e) {
    console.error('到期检测失败:', e);
  }
}
__mod.sendNotification = sendNotification;
__mod.checkOfflineNodes = checkOfflineNodes;
__mod.checkExpiringServers = checkExpiringServers;
}

// /src/handlers/admin.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_handlers_admin_js"] = {};











function isValidUUID(id) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
}

function isValidName(name) {
  return name && typeof name === 'string' && name.trim().length > 0 && name.length <= 100;
}

function sanitizeCspDomains(input) {
  if (!input || typeof input !== 'string') return '';
  return input
    .split(',')
    .map(s => s.trim())
    .map(normalizeCspOrigin)
    .filter(Boolean)
    .filter((domain, index, arr) => arr.indexOf(domain) === index)
    .join(',');
}

function normalizeCspOrigin(value) {
  const raw = String(value || '').trim();
  if (!raw || /[\s;"']/.test(raw)) return '';
  try {
    const url = new URL(raw);
    if (url.protocol !== 'https:') return '';
    if (url.username || url.password || url.search || url.hash) return '';
    if (url.pathname && url.pathname !== '/') return '';
    return url.origin;
  } catch (_) {
    return '';
  }
}

async function deleteServer(db, id) {
  try {
    const stmt1 = db.prepare(`PRAGMA foreign_key_list(metrics_history)`);
    const result1 = await stmt1.all();
    if (result1.results.length > 0) {
      await db.prepare('DELETE FROM metrics_history WHERE server_id = ?').bind(id).run();
    }

    const stmt2 = db.prepare(`PRAGMA foreign_key_list(metrics_history_old)`);
    const result2 = await stmt2.all();
    if (result2.results.length > 0) {
      await db.prepare('DELETE FROM metrics_history_old WHERE server_id = ?').bind(id).run();
    }

    await db.prepare('DELETE FROM servers WHERE id = ?').bind(id).run();
  } catch (err) {
    throw err;
  }
}

function getUtcTodayRange() {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const end = new Date(start.getTime() + 86400000 - 1);
  return {
    date: start.toISOString().slice(0, 10),
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
    startTime: start.toISOString(),
    endTime: end.toISOString()
  };
}

function getLast24HoursRange() {
  const now = new Date();
  const end = now;
  const start = new Date(now.getTime() - 86400000);
  return {
    date: start.toISOString().slice(0, 10) + ' ~ ' + end.toISOString().slice(0, 10),
    startTime: start.toISOString(),
    endTime: end.toISOString()
  };
}

async function cloudflareGraphql(query, variables, token) {
  const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });
  const data = await response.json();
  if (!response.ok || data.errors) {
    const message = data.errors && data.errors.length > 0 ? data.errors.map(e => e.message).join('; ') : 'Cloudflare GraphQL request failed';
    throw new Error(message);
  }
  return data.data;
}

async function fetchCloudflareUsage(token, accountId, range) {
  const query = `query CloudflareUsage($accountTag: string!, $start: Date, $end: Date, $startTime: string, $endTime: string) {
    viewer {
      accounts(filter: { accountTag: $accountTag }) {
        d1AnalyticsAdaptiveGroups(
          limit: 10000
          filter: { date_geq: $start, date_leq: $end }
        ) {
          sum { rowsRead rowsWritten }
          dimensions { databaseId }
        }
        workersInvocationsAdaptive(
          limit: 10000
          filter: { datetime_geq: $startTime, datetime_leq: $endTime }
        ) {
          sum { requests }
        }
      }
    }
  }`;
  const data = await cloudflareGraphql(query, {
    accountTag: accountId,
    start: range.start || range.startTime.slice(0, 10),
    end: range.end || range.endTime.slice(0, 10),
    startTime: range.startTime,
    endTime: range.endTime
  }, token);
  const account = data.viewer?.accounts?.[0] || {};
  const groups = account.d1AnalyticsAdaptiveGroups || [];
  const usage = groups.reduce((total, group) => {
    total.rowsRead += Number(group.sum?.rowsRead || 0);
    total.rowsWritten += Number(group.sum?.rowsWritten || 0);
    return total;
  }, { rowsRead: 0, rowsWritten: 0 });
  const workersRequests = (account.workersInvocationsAdaptive || []).reduce((total, group) => {
    return total + Number(group.sum?.requests || 0);
  }, 0);
  return { rowsRead: usage.rowsRead, rowsWritten: usage.rowsWritten, workersRequests, databaseCount: groups.length };
}

async function getD1DailyUsage(token, accountId) {
  if (!token) throw new Error('cloudflareTokenRequired');
  if (!accountId) throw new Error('cloudflareAccountIdRequired');

  const todayRange = getUtcTodayRange();
  const last24Range = getLast24HoursRange();

  const [todayUsage, last24Usage] = await Promise.all([
    fetchCloudflareUsage(token, accountId, todayRange),
    fetchCloudflareUsage(token, accountId, last24Range)
  ]);

  return {
    today: {
      rowsRead: todayUsage.rowsRead,
      rowsWritten: todayUsage.rowsWritten,
      workersRequests: todayUsage.workersRequests
    },
    last24Hours: {
      rowsRead: last24Usage.rowsRead,
      rowsWritten: last24Usage.rowsWritten,
      workersRequests: last24Usage.workersRequests
    }
  };
}

async function handleAdminAPI(request, env, sys, loadFullSettings = null) {
  try {
    const data = await request.json();

    if (data.action === 'login') {
      const { username, password } = data;
      
      if (!username || !password) {
        return createBadRequestResponse('Missing username or password');
      }

      const turnstileEnabled = sys && (sys.turnstile_enabled === 'true' || sys.turnstile_enabled === true);
      const turnstileLoginEnabled = sys && (sys.turnstile_login_enabled === 'true' || sys.turnstile_login_enabled === true);
      const turnstileSecretKey = sys && sys.turnstile_secret_key || '';
      
      if (turnstileEnabled || turnstileLoginEnabled) {
        const turnstileToken = request.headers.get('X-Turnstile-Token');
        const isTurnstileVerified = await verifyTurnstileToken(turnstileToken, turnstileSecretKey);
        
        if (!isTurnstileVerified) {
          return createErrorResponse(new AppError('Turnstile verification failed', 403));
        }
      }

      const authHeader = 'Basic ' + btoa(username + ':' + password);
      const mockRequest = {
        headers: {
          get: (key) => key === 'Authorization' ? authHeader : null
        }
      };

      const credentialResult = await validateCredentials(mockRequest, env, sys);
      
      if (!credentialResult.valid) {
        return createUnauthorizedResponse('Invalid username or password');
      }

      if (credentialResult.needsPasswordUpgrade) {
        try {
          const upgradedPasswordHash = await hashPassword(password);
          await saveSiteOptions(env.DB, { password: upgradedPasswordHash });
          if (sys) {
            sys.password = upgradedPasswordHash;
          }
        } catch (e) {
          console.error('Password hash upgrade failed:', e);
        }
      }

      try {
        const token = await generateToken(env, sys);
        return createSuccessResponse({ 
          success: true, 
          token: token,
          message: 'loginSuccessful'
        });
      } catch (e) {
        return createErrorResponse(e);
      }
    }

    if (!await checkAuth(request, env, sys)) {
      return simpleAuthResponse();
    }

    if (data.action === 'get_settings') {
      const fullSettings = loadFullSettings ? await loadFullSettings() : sys;
      const { jwt_secret, ...safeSettings } = fullSettings || {};
      return createSuccessResponse({
        success: true,
        settings: safeSettings,
        api_secret: env.API_SECRET
      });
    }
    else if (data.action === 'list') {
      const servers = await getAllServers(env.DB);
      const latestMetricsMap = await getLatestMetricsForAllServers(env.DB);
      
      const now = Date.now();
      const ONLINE_THRESHOLD = 300000;
      const stats = {
        total: servers.length,
        online: 0,
        offline: 0,
        total_cpu: 0,
        total_net_in: 0,
        total_net_out: 0,
        avg_cpu: 0
      };
      
      const serversWithStatus = servers.map(server => {
        const latestMetrics = latestMetricsMap.get(server.id);
        const item = { ...server };
        let isOnline = false;
        
        if (latestMetrics) {
          isOnline = (now - latestMetrics.timestamp) < ONLINE_THRESHOLD;
          mergeMetricsIntoServer(item, latestMetrics);
        } else {
          item.last_updated = 0;
          item.is_online = false;
          item.cpu_cores = 0;
          item.cpu_info = '';
          item.arch = '';
          item.os = '';
          item.ip_v4 = '0';
          item.ip_v6 = '0';
          item.boot_time = '';
        }
        
        item.is_online = isOnline;
        if (!item.region) item.region = server.region || '';
        delete item.bandwidth;

        if (isOnline) {
          stats.online++;
          stats.total_cpu += parseFloat(item.cpu) || 0;
          stats.total_net_in += parseFloat(item.net_in_speed) || 0;
          stats.total_net_out += parseFloat(item.net_out_speed) || 0;
        } else {
          stats.offline++;
        }
        
        return item;
      });
      
      if (stats.online > 0) {
        stats.avg_cpu = (stats.total_cpu / stats.online).toFixed(2);
      }

      return createSuccessResponse({
        success: true,
        servers: serversWithStatus,
        stats
      });
    }
    else if (data.action === 'd1_usage') {
      const hasCloudflareToken = Object.prototype.hasOwnProperty.call(data, 'cloudflare_token');
      const hasCloudflareAccountId = Object.prototype.hasOwnProperty.call(data, 'cloudflare_account_id');
      const cloudflareToken = hasCloudflareToken ? data.cloudflare_token : (sys?.cloudflare_token || '');
      const cloudflareAccountId = hasCloudflareAccountId ? data.cloudflare_account_id : (sys?.cloudflare_account_id || '');

      try {
        const usage = await getD1DailyUsage(String(cloudflareToken || '').trim(), String(cloudflareAccountId || '').trim());
        return createSuccessResponse({
          success: true,
          usage,
          message: 'd1UsageQueried'
        });
      } catch (e) {
        return createBadRequestResponse(e.message);
      }
    }
    else if (data.action === 'send_test_notification') {
      const { tg_bot_token, tg_chat_id } = data;
      if (!tg_bot_token || tg_bot_token.trim().length === 0) {
        return createBadRequestResponse('tgBotTokenRequired');
      }
      try {
        const testMsg = `✅ **测试通知**\n\n这是一条来自 CF Server Monitor 的测试消息。\n\n**时间:** ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`;
        const result = await sendNotification({ tg_bot_token, tg_chat_id: tg_chat_id || '' }, testMsg);
        if(result) {
          return createBadRequestResponse(result);
        }
        return createSuccessResponse({ success: true, message: 'testNotificationSent' });
      } catch (e) {
        return createBadRequestResponse('testNotificationFailed');
      }
    }
    else if (data.action === 'save_settings') {
      const settings = data.settings || {};

      // 如果 turnstile_enabled 或 turnstile_login_enabled 开启，验证 turnstile_site_key 和 turnstile_secret_key 都不为空
      if (settings.turnstile_enabled === 'true' || settings.turnstile_enabled === true || settings.turnstile_login_enabled === 'true' || settings.turnstile_login_enabled === true) {
        if (!settings.turnstile_site_key || settings.turnstile_site_key.trim().length === 0) {
          return createBadRequestResponse('Turnstile Site Key is required when Turnstile is enabled');
        }
        if (!settings.turnstile_secret_key || settings.turnstile_secret_key.trim().length === 0) {
          return createBadRequestResponse('Turnstile Secret Key is required when Turnstile is enabled');
        }
      }

      // 如果 tg_notify 或 expire_reminder 开启，验证 tg_bot_token 不为空
      if (settings.tg_notify === 'true' || settings.expire_reminder === 'true') {
        if (!settings.tg_bot_token || settings.tg_bot_token.trim().length === 0) {
          return createBadRequestResponse('Telegram Bot Token is required when notifications are enabled');
        }
      }

      const appearanceOptions = {};
      for (const field of APPEARANCE_FIELDS) {
        if (settings[field] !== undefined) {
          // CSP 字段格式校验：只允许 https:// 开头的域名，逗号分隔
          if (field === 'csp_static' || field === 'csp_api') {
            appearanceOptions[field] = sanitizeCspDomains(settings[field]);
          } else {
            appearanceOptions[field] = settings[field];
          }
        }
      }
      await env.DB.prepare(
        'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
      ).bind('appearance_options', JSON.stringify(appearanceOptions)).run();
      clearAppearanceSettingsCache();

      const siteOptions = {};
      for (const field of SITE_FIELDS) {
        if (settings[field] !== undefined) {
          if (field === 'password') {
            if (settings[field] && settings[field].length > 0) {
              siteOptions[field] = await hashPassword(settings[field]);
            }
          } else {
            siteOptions[field] = settings[field];
          }
        }
      }
      await saveSiteOptions(env.DB, siteOptions);
      Object.assign(sys, appearanceOptions, siteOptions);
      return createSuccessResponse({
        success: true,
        message: 'updateSuccess'
      });
    } 
    else if (data.action === 'add') {
      const name = data.name || 'New Server';
      if (!isValidName(name)) {
        return createBadRequestResponse('invalidServerName');
      }
      
      const id = crypto.randomUUID();
      const group = data.server_group || 'Default';

      const { max_order } = await env.DB.prepare('SELECT COALESCE(MAX(sort_order), -1) as max_order FROM servers').first();
      const sortOrder = (max_order || 0) + 1;

      const historyPartitionId = await getNextServerHistoryPartitionId(env.DB);

      await env.DB.prepare(`
        INSERT INTO servers
        (id, name, server_group, sort_order, history_partition_id, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
      `).bind(id, name, group, sortOrder, historyPartitionId, Date.now()).run();
      
      clearServersListCache();
      
      return createSuccessResponse({ 
        success: true, 
        id: id,
        message: 'serverAdded'
      });
    } 
    else if (data.action === 'delete') {
      const { id } = data;
      if (!id || !isValidUUID(id)) {
        return createBadRequestResponse('invalidServerId');
      }
      
      await deleteServer(env.DB, id);
      
      clearServersListCache();
      
      return createSuccessResponse({ 
        success: true, 
        message: 'serverDeleted'
      });
    } 
    else if (data.action === 'save_order') {
      const { orders } = data;
      if (!orders || !Array.isArray(orders) || orders.length === 0) {
        return createBadRequestResponse('missingSortData');
      }
      
      for (let i = 0; i < orders.length; i++) {
        if (!isValidUUID(orders[i])) {
          return createBadRequestResponse('invalidSortId');
        }
        await env.DB.prepare('UPDATE servers SET sort_order = ? WHERE id = ?').bind(i, orders[i]).run();
      }
      
      clearServersListCache();
      
      return createSuccessResponse({ 
        success: true, 
        message: 'sortOrderSaved'
      });
    }
    else if (data.action === 'edit') {
      const { id, name, server_group, tags, note, price, expire_date, traffic_limit, traffic_calc_type, reset_day, collect_interval, report_interval, ping_mode, custom_ct, custom_cu, custom_cm, custom_bd, rx_correction, tx_correction, offline_notify_disabled, is_hidden } = data;
      if (!id || !isValidUUID(id)) {
        return createBadRequestResponse('invalidServerId');
      }
      const agentConfigResult = validateAgentConfigInput({
        collect_interval,
        report_interval,
        ping_mode,
        reset_day
      });
      if (!agentConfigResult.valid) {
        return createBadRequestResponse(agentConfigResult.error);
      }
      const normalizedAgentConfig = agentConfigResult.config;

      const sanitizePing = (v) => {
        if (v === null || v === undefined) return '';
        return String(v).replace(/[^a-zA-Z0-9.\-_]/g, '').slice(0, 50);
      };
      const safeCustomCt = sanitizePing(custom_ct);
      const safeCustomCu = sanitizePing(custom_cu);
      const safeCustomCm = sanitizePing(custom_cm);
      const safeCustomBd = sanitizePing(custom_bd);
      const safeTags = String(tags || '')
        .split(',')
        .map(tag => tag.trim().replace(/[^\p{L}\p{N} ._\-]/gu, '').slice(0, 32))
        .filter(Boolean)
        .slice(0, 12)
        .join(',');
      const safeNote = String(note || '').trim().slice(0, 500);

      const toNullCorrection = (v) => {
        if (v === null || v === undefined || v === '') return null;
        return isValidTrafficCorrection(v) ? Number(v) : undefined;
      };
      const safeRx = toNullCorrection(rx_correction);
      const safeTx = toNullCorrection(tx_correction);
      if (safeRx === undefined || safeTx === undefined) {
        return createBadRequestResponse('invalidTrafficCorrection');
      }
      
      try {
        await env.DB.prepare(`
          UPDATE servers
          SET name = ?, server_group = ?, tags = ?, note = ?, price = ?, expire_date = ?, traffic_limit = ?, traffic_calc_type = ?, reset_day = ?, collect_interval = ?, report_interval = ?, ping_mode = ?, custom_ct = ?, custom_cu = ?, custom_cm = ?, custom_bd = ?, rx_correction = ?, tx_correction = ?, offline_notify_disabled = ?, is_hidden = ?
          WHERE id = ?
        `).bind(
          name || '',
          server_group || 'Default',
          safeTags,
          safeNote,
          price || '',
          expire_date || '',
          traffic_limit || '',
          traffic_calc_type || 'total',
          normalizedAgentConfig.reset_day,
          normalizedAgentConfig.collect_interval,
          normalizedAgentConfig.report_interval,
          normalizedAgentConfig.ping_mode,
          safeCustomCt,
          safeCustomCu,
          safeCustomCm,
          safeCustomBd,
          safeRx,
          safeTx,
          offline_notify_disabled || '0',
          is_hidden || '0',
          id
        ).run();
      } catch (e) {
        if (e.message && /no such column/i.test(e.message)) {
          console.warn('检测到数据库字段缺失，尝试添加缺失字段...');
          await addServerColumns(env.DB);
          return createBadRequestResponse('dbColumnsAdded');
        }else{
          const errMsg = e?.message || String(e);
          return createBadRequestResponse(errMsg || 'serverUpdateFailed');
        }
      }
      
      clearServersListCache();
      
      return createSuccessResponse({ 
        success: true, 
        message: 'serverUpdated'
      });
    }
    else if (data.action === 'batch_delete') {
      const { ids } = data;
      if (!ids || !Array.isArray(ids) || ids.length === 0) {
        return createBadRequestResponse('selectServersToDelete');
      }
      
      for (const id of ids) {
        if (!isValidUUID(id)) {
          return createBadRequestResponse('invalidServerIdInList');
        }
      }
      
      for (const id of ids) {
        await deleteServer(env.DB, id);
      }
      
      clearServersListCache();
      
      return createSuccessResponse({ 
        success: true, 
        message: 'batchDeleted'
      });
    }
    
    return createBadRequestResponse('unknownAction');
    
  } catch (e) {
    console.error('Admin API 错误:', e);
    return createErrorResponse(e);
  }
}

__mod.handleAdminAPI = handleAdminAPI;
}

// /src/middleware/auth.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_middleware_auth_js"] = {};
const ALGORITHM = { name: 'HMAC', hash: 'SHA-256' };

async function generateKeyFromSecret(secret) {
  const encoder = new TextEncoder();
  const keyData = encoder.encode(secret);
  return await crypto.subtle.importKey('raw', keyData, ALGORITHM, false, ['sign', 'verify']);
}

async function signJwt(payload, secret) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const encodedHeader = btoa(JSON.stringify(header)).replace(/=/g, '');
  const encodedPayload = btoa(JSON.stringify(payload)).replace(/=/g, '');
  
  const data = `${encodedHeader}.${encodedPayload}`;
  const key = await generateKeyFromSecret(secret);
  
  const encoder = new TextEncoder();
  const dataBytes = encoder.encode(data);
  const signature = await crypto.subtle.sign(ALGORITHM, key, dataBytes);
  
  const encodedSignature = btoa(String.fromCharCode(...new Uint8Array(signature))).replace(/=/g, '');
  
  return `${encodedHeader}.${encodedPayload}.${encodedSignature}`;
}

async function verifyJwt(token, secret) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) {
      return null;
    }
    
    const [encodedHeader, encodedPayload, encodedSignature] = parts;
    
    const key = await generateKeyFromSecret(secret);
    
    const data = `${encodedHeader}.${encodedPayload}`;
    const encoder = new TextEncoder();
    const dataBytes = encoder.encode(data);
    
    const signatureBytes = new Uint8Array(atob(encodedSignature).split('').map(c => c.charCodeAt(0)));
    
    const isValid = await crypto.subtle.verify(ALGORITHM, key, signatureBytes, dataBytes);
    
    if (!isValid) {
      return null;
    }
    
    const payload = JSON.parse(atob(encodedPayload));
    
    if (payload.exp && Date.now() > payload.exp * 1000) {
      return null;
    }
    
    return payload;
  } catch (e) {
    console.error('JWT verification error:', e);
    return null;
  }
}

function getJwtSecret(env, sys) {
  if (sys && sys.jwt_secret && sys.jwt_secret.length >= 32) {
    return sys.jwt_secret;
  }
  
  const fallback = env.API_SECRET || 'default_jwt_secret_for_server_monitor';
  const padded = fallback.padEnd(32, 'x');
  
  return padded.substring(0, 64);
}

async function generateToken(env, sys) {
  const payload = {
    sub: 'admin',
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 604800
  };
  
  const secret = getJwtSecret(env, sys);
  return signJwt(payload, secret);
}

async function checkAuth(request, env, sys) {
  const authHeader = request.headers.get('Authorization');
  if (!authHeader) {
    return false;
  }

  const parts = authHeader.trim().split(/\s+/);
  const scheme = parts[0];
  const token = parts[1];

  if (scheme !== 'Bearer' || !token) {
    return false;
  }

  const secret = getJwtSecret(env, sys);
  
  try {
    const payload = await verifyJwt(token, secret);
    return payload !== null;
  } catch (e) {
    console.error('Auth check error:', e);
    return false;
  }
}

async function validateCredentials(request, env, sys) {
  try {
    const authHeader = request.headers.get('Authorization');
    if (!authHeader) {
      return { valid: false, needsPasswordUpgrade: false };
    }

    const parts = authHeader.trim().split(/\s+/);
    const scheme = parts[0];
    const encoded = parts[1];

    if (scheme !== 'Basic' || !encoded) {
      return { valid: false, needsPasswordUpgrade: false };
    }

    let decoded;
    try {
      decoded = atob(encoded);
    } catch (e) {
      return { valid: false, needsPasswordUpgrade: false };
    }

    const idx = decoded.indexOf(':');
    if (idx === -1) {
      return { valid: false, needsPasswordUpgrade: false };
    }

    const username = decoded.slice(0, idx);
    const password = decoded.slice(idx + 1);

    const validUsername = (sys && sys.username && sys.username.length > 0)
      ? sys.username
      : (typeof env.API_USER_NAME === 'string' && env.API_USER_NAME.length > 0)
        ? env.API_USER_NAME
        : 'admin';

    if (sys && sys.password && sys.password.length > 0) {
      if (username !== validUsername) {
        return { valid: false, needsPasswordUpgrade: false };
      }

      const result = await verifyPasswordHash(password, sys.password);
      return {
        valid: result.valid,
        needsPasswordUpgrade: result.needsRehash === true
      };
    }

    const valid = (
      typeof env.API_SECRET === 'string' &&
      env.API_SECRET.length > 0 &&
      username === validUsername &&
      password === env.API_SECRET
    );
    return { valid, needsPasswordUpgrade: false };
  } catch (e) {
    console.error('Credential validation error:', e);
    return { valid: false, needsPasswordUpgrade: false };
  }
}

function simpleAuthResponse() {
  return new Response(JSON.stringify({ error: 'Unauthorized', code: 401 }), {
    status: 401,
    headers: { 'Content-Type': 'application/json' }
  });
}

__mod.generateToken = generateToken;
__mod.checkAuth = checkAuth;
__mod.validateCredentials = validateCredentials;
__mod.simpleAuthResponse = simpleAuthResponse;
}

// /src/utils/common.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_common_js"] = {};
/**
 * 公共工具函数模块
 * 统一存放各处重复定义的函数
 */

/**
 * 验证 Turnstile token
 * @param {string} token - Turnstile token
 * @param {string} secretKey - Turnstile secret key
 * @returns {Promise<boolean>} 验证结果
 */
async function verifyTurnstileToken(token, secretKey) {
  if (!token || !secretKey) {
    return false;
  }
  
  try {
    const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        secret: secretKey,
        response: token
      })
    });
    
    const data = await response.json();
    return data.success === true;
  } catch (e) {
    console.error('Turnstile verification error:', e);
    return false;
  }
}

/**
 * 管理后台密码哈希参数
 */
const PASSWORD_HASH_ALGORITHM = 'pbkdf2_sha256';
const PASSWORD_HASH_ITERATIONS = 50000;
const PASSWORD_SALT_BYTES = 16;
const PASSWORD_HASH_BYTES = 32;

function bytesToHex(bytes) {
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

function hexToBytes(hex) {
  if (!hex || hex.length % 2 !== 0 || !/^[a-f0-9]+$/i.test(hex)) {
    return null;
  }

  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.slice(i, i + 2), 16);
  }
  return bytes;
}

function timingSafeEqualBytes(left, right) {
  if (!(left instanceof Uint8Array) || !(right instanceof Uint8Array)) {
    return false;
  }

  if (left.length === right.length && crypto.subtle && typeof crypto.subtle.timingSafeEqual === 'function') {
    return crypto.subtle.timingSafeEqual(left, right);
  }

  let diff = left.length ^ right.length;
  const maxLength = Math.max(left.length, right.length);
  for (let i = 0; i < maxLength; i++) {
    diff |= (left[i] || 0) ^ (right[i] || 0);
  }
  return diff === 0;
}

async function derivePbkdf2Hash(password, salt, iterations) {
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    encoder.encode(password),
    'PBKDF2',
    false,
    ['deriveBits']
  );

  const bits = await crypto.subtle.deriveBits(
    {
      name: 'PBKDF2',
      hash: 'SHA-256',
      salt,
      iterations
    },
    keyMaterial,
    PASSWORD_HASH_BYTES * 8
  );

  return new Uint8Array(bits);
}

function parsePbkdf2Hash(storedHash) {
  if (typeof storedHash !== 'string') {
    return null;
  }

  const parts = storedHash.trim().split('$');
  if (parts.length !== 4 || parts[0] !== PASSWORD_HASH_ALGORITHM) {
    return null;
  }

  const iterations = Number(parts[1]);
  const salt = hexToBytes(parts[2]);
  const hash = hexToBytes(parts[3]);

  if (
    !Number.isInteger(iterations) ||
    iterations <= 0 ||
    !salt ||
    salt.length !== PASSWORD_SALT_BYTES ||
    !hash ||
    hash.length !== PASSWORD_HASH_BYTES
  ) {
    return null;
  }

  return { iterations, salt, hash };
}

function isLegacyMd5Hash(storedHash) {
  return typeof storedHash === 'string' && /^[a-f0-9]{32}$/i.test(storedHash.trim());
}

async function hashPassword(password) {
  const salt = crypto.getRandomValues(new Uint8Array(PASSWORD_SALT_BYTES));
  const hash = await derivePbkdf2Hash(password, salt, PASSWORD_HASH_ITERATIONS);
  return `${PASSWORD_HASH_ALGORITHM}$${PASSWORD_HASH_ITERATIONS}$${bytesToHex(salt)}$${bytesToHex(hash)}`;
}

async function verifyPasswordHash(password, storedHash) {
  const parsed = parsePbkdf2Hash(storedHash);
  if (parsed) {
    const hash = await derivePbkdf2Hash(password, parsed.salt, parsed.iterations);
    return {
      valid: timingSafeEqualBytes(hash, parsed.hash),
      needsRehash: false,
      algorithm: PASSWORD_HASH_ALGORITHM
    };
  }

  if (isLegacyMd5Hash(storedHash)) {
    const hashedPassword = await md5Hash(password);
    const actual = hexToBytes(hashedPassword);
    const expected = hexToBytes(storedHash.trim().toLowerCase());
    const valid = timingSafeEqualBytes(actual, expected);
    return {
      valid,
      needsRehash: valid,
      algorithm: 'md5'
    };
  }

  return {
    valid: false,
    needsRehash: false,
    algorithm: 'unknown'
  };
}

/**
 * 计算 MD5 哈希值，仅用于兼容旧版密码
 * @param {string} input - 输入字符串
 * @returns {Promise<string>} MD5 哈希值
 */
const MD5_SHIFTS = [
  7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
  5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
  4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
  6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21
];
const MD5_CONSTANTS = Array.from({ length: 64 }, (_, i) =>
  Math.floor(Math.abs(Math.sin(i + 1)) * 0x100000000) >>> 0
);

async function md5Hash(input) {
  const bytes = Array.from(new TextEncoder().encode(input));
  const originalBitLength = bytes.length * 8;
  bytes.push(0x80);
  while ((bytes.length % 64) !== 56) bytes.push(0);

  const lowBits = originalBitLength >>> 0;
  const highBits = Math.floor(originalBitLength / 0x100000000) >>> 0;
  for (let i = 0; i < 4; i++) bytes.push((lowBits >>> (i * 8)) & 0xff);
  for (let i = 0; i < 4; i++) bytes.push((highBits >>> (i * 8)) & 0xff);

  const rotateLeft = (value, amount) => ((value << amount) | (value >>> (32 - amount))) >>> 0;

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;

  for (let offset = 0; offset < bytes.length; offset += 64) {
    const words = new Array(16);
    for (let i = 0; i < 16; i++) {
      const base = offset + i * 4;
      words[i] = (
        bytes[base] |
        (bytes[base + 1] << 8) |
        (bytes[base + 2] << 16) |
        (bytes[base + 3] << 24)
      ) >>> 0;
    }

    let a = a0;
    let b = b0;
    let c = c0;
    let d = d0;

    for (let i = 0; i < 64; i++) {
      let f;
      let g;
      if (i < 16) {
        f = (b & c) | (~b & d);
        g = i;
      } else if (i < 32) {
        f = (d & b) | (~d & c);
        g = (5 * i + 1) % 16;
      } else if (i < 48) {
        f = b ^ c ^ d;
        g = (3 * i + 5) % 16;
      } else {
        f = c ^ (b | ~d);
        g = (7 * i) % 16;
      }

      const previousD = d;
      d = c;
      c = b;
      const sum = (a + (f >>> 0) + MD5_CONSTANTS[i] + words[g]) >>> 0;
      b = (b + rotateLeft(sum, MD5_SHIFTS[i])) >>> 0;
      a = previousD;
    }

    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }

  return [a0, b0, c0, d0].map(word => {
    let hex = '';
    for (let i = 0; i < 4; i++) hex += ((word >>> (i * 8)) & 0xff).toString(16).padStart(2, '0');
    return hex;
  }).join('');
}

__mod.verifyTurnstileToken = verifyTurnstileToken;
__mod.PASSWORD_HASH_ALGORITHM = PASSWORD_HASH_ALGORITHM;
__mod.isLegacyMd5Hash = isLegacyMd5Hash;
__mod.PASSWORD_HASH_ITERATIONS = PASSWORD_HASH_ITERATIONS;
__mod.hashPassword = hashPassword;
__mod.verifyPasswordHash = verifyPasswordHash;
__mod.md5Hash = md5Hash;
}

// /src/utils/metrics.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_metrics_js"] = {};
function mergeMetricsIntoServer(server, metrics) {
  if (!metrics) return;

  server.cpu = metrics.cpu || 0;
  server.load_avg = metrics.load ?? metrics.load_avg ?? '0 0 0';
  server.net_in_speed = metrics.net_in_speed || 0;
  server.net_out_speed = metrics.net_out_speed || 0;
  server.net_rx = metrics.net_rx || 0;
  server.net_tx = metrics.net_tx || 0;
  server.net_rx_monthly = metrics.net_rx_monthly || 0;
  server.net_tx_monthly = metrics.net_tx_monthly || 0;
  server.processes = metrics.processes || 0;
  server.tcp_conn = metrics.tcp_conn || 0;
  server.udp_conn = metrics.udp_conn || 0;
  server.ping_ct = metrics.ping_ct;
  server.ping_cu = metrics.ping_cu;
  server.ping_cm = metrics.ping_cm;
  server.ping_bd = metrics.ping_bd;
  server.loss_ct = metrics.loss_ct;
  server.loss_cu = metrics.loss_cu;
  server.loss_cm = metrics.loss_cm;
  server.loss_bd = metrics.loss_bd;
  server.ram_total = metrics.ram_total || 0;
  server.ram_used = metrics.ram_used || 0;
  server.swap_total = metrics.swap_total || 0;
  server.swap_used = metrics.swap_used || 0;
  server.disk_total = metrics.disk_total || 0;
  server.disk_used = metrics.disk_used || 0;
  server.cpu_cores = metrics.cpu_cores || 0;
  server.cpu_info = metrics.cpu_info || '';
  server.gpu = metrics.gpu;
  server.gpu_info = metrics.gpu_info || '';
  server.arch = metrics.arch || '';
  server.os = metrics.os || '';
  server.region = metrics.region || '';
  server.ip_v4 = metrics.ip_v4 || '0';
  server.ip_v6 = metrics.ip_v6 || '0';
  server.boot_time = metrics.boot_time || '';
  server.last_updated = metrics.timestamp || 0;
}
__mod.mergeMetricsIntoServer = mergeMetricsIntoServer;
}

// /src/utils/errors.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_errors_js"] = {};
class AppError extends Error {
  constructor(message, code = 500, details = null) {
    super(message);
    this.name = 'AppError';
    this.code = code;
    this.details = details;
  }
}

function createErrorResponse(error, logError = true) {
  if (logError) {
    if (error instanceof AppError) {
      console.error(`[Error] ${error.code}: ${error.message}`, error.details || '');
    } else {
      console.error('[Error] Unexpected:', error.message, error.stack);
    }
  }

  const code = error instanceof AppError ? error.code : 500;
  const message = error instanceof AppError 
    ? error.message 
    : 'Internal Server Error';

  return new Response(JSON.stringify({ 
    error: message, 
    code: code 
  }), {
    status: code,
    headers: { 'Content-Type': 'application/json' }
  });
}

function createSuccessResponse(data, headers = {}) {
  const defaultHeaders = { 'Content-Type': 'application/json' };
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { ...defaultHeaders, ...headers }
  });
}

function createUnauthorizedResponse(message = 'Unauthorized') {
  return new Response(JSON.stringify({ 
    error: message, 
    code: 401 
  }), {
    status: 401,
    headers: { 'Content-Type': 'application/json' }
  });
}

function createBadRequestResponse(message = 'Bad Request') {
  return new Response(JSON.stringify({ 
    error: message, 
    code: 400 
  }), {
    status: 400,
    headers: { 'Content-Type': 'application/json' }
  });
}

function createNotFoundResponse(message = 'Not Found') {
  return new Response(JSON.stringify({ 
    error: message, 
    code: 404 
  }), {
    status: 404,
    headers: { 'Content-Type': 'application/json' }
  });
}
__mod.createErrorResponse = createErrorResponse;
__mod.AppError = AppError;
__mod.createSuccessResponse = createSuccessResponse;
__mod.createUnauthorizedResponse = createUnauthorizedResponse;
__mod.createBadRequestResponse = createBadRequestResponse;
__mod.createNotFoundResponse = createNotFoundResponse;
}

// /src/utils/agentConfig.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_agentConfig_js"] = {};

const AGENT_CONFIG_SCHEMA_VERSION = 1;
const AGENT_CONFIG_SCHEMA_HEADER = 'X-Agent-Config-Schema';
const AGENT_CONFIG_MD5_HEADER = 'X-Agent-Config-Md5';
const MAX_TRAFFIC_CORRECTION_GB = 1000000;

const ALLOWED_COLLECT_INTERVALS = new Set([0, 1, 2, 5, 10]);
const ALLOWED_REPORT_INTERVALS = new Set([30, 60, 120, 180]);
const ALLOWED_PING_MODES = new Set(['http', 'tcp']);

function validateInteger(name, value, allowedValues = null, min = null, max = null) {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    return `${name} must be an integer`;
  }
  if (allowedValues && !allowedValues.has(value)) {
    return `${name} is not allowed`;
  }
  if (min !== null && value < min) return `${name} is below the minimum`;
  if (max !== null && value > max) return `${name} is above the maximum`;
  return null;
}

function validateAgentConfigInput(input) {
  const collectError = validateInteger(
    'collect_interval',
    input.collect_interval,
    ALLOWED_COLLECT_INTERVALS
  );
  if (collectError) return { valid: false, error: collectError };

  const reportError = validateInteger(
    'report_interval',
    input.report_interval,
    ALLOWED_REPORT_INTERVALS
  );
  if (reportError) return { valid: false, error: reportError };

  const resetError = validateInteger('reset_day', input.reset_day, null, 0, 31);
  if (resetError) return { valid: false, error: resetError };

  if (typeof input.ping_mode !== 'string' || !ALLOWED_PING_MODES.has(input.ping_mode)) {
    return { valid: false, error: 'ping_mode must be http or tcp' };
  }

  if (input.collect_interval > 0 && input.report_interval < input.collect_interval) {
    return { valid: false, error: 'report_interval must be greater than or equal to collect_interval' };
  }

  if (
    input.collect_interval > 0 &&
    Math.ceil(input.report_interval / input.collect_interval) > 300
  ) {
    return { valid: false, error: 'configuration would create more than 300 samples per report' };
  }

  return {
    valid: true,
    config: {
      collect_interval: input.collect_interval,
      ping_mode: input.ping_mode,
      report_interval: input.report_interval,
      reset_day: input.reset_day,
      schema_version: AGENT_CONFIG_SCHEMA_VERSION
    }
  };
}

function storedInteger(value, allowedValues, fallback) {
  const number = typeof value === 'number' ? value : Number(value);
  return Number.isInteger(number) && allowedValues.has(number) ? number : fallback;
}

function sanitizePingNode(value) {
  if (typeof value !== 'string') return '';
  return value.replace(/[^a-zA-Z0-9.\-_]/g, '').slice(0, 50);
}

function isValidTrafficCorrection(value) {
  let number;
  if (typeof value === 'number') {
    number = value;
  } else if (typeof value === 'string' && /^[0-9]+(?:\.[0-9]+)?$/.test(value)) {
    number = Number(value);
  } else {
    return false;
  }
  return Number.isFinite(number) && number >= 0 && number <= MAX_TRAFFIC_CORRECTION_GB;
}

function normalizeTrafficCorrection(value) {
  return isValidTrafficCorrection(value) ? Number(value) : 0;
}

function buildAgentConfig(server, settings = null) {
  const collectInterval = storedInteger(server?.collect_interval, ALLOWED_COLLECT_INTERVALS, 0);
  let reportInterval = storedInteger(server?.report_interval, ALLOWED_REPORT_INTERVALS, 60);
  if (collectInterval > 0 && reportInterval < collectInterval) reportInterval = 60;

  const resetNumber = typeof server?.reset_day === 'number'
    ? server.reset_day
    : Number(server?.reset_day);
  const resetDay = Number.isInteger(resetNumber) && resetNumber >= 0 && resetNumber <= 31
    ? resetNumber
    : 1;

  const pingMode = ALLOWED_PING_MODES.has(server?.ping_mode) ? server.ping_mode : 'http';

  const customCt = sanitizePingNode(server?.custom_ct || settings?.custom_ct || '');
  const customCu = sanitizePingNode(server?.custom_cu || settings?.custom_cu || '');
  const customCm = sanitizePingNode(server?.custom_cm || settings?.custom_cm || '');
  const customBd = sanitizePingNode(server?.custom_bd || settings?.custom_bd || '');

  return {
    collect_interval: collectInterval,
    ping_mode: pingMode,
    report_interval: reportInterval,
    reset_day: resetDay,
    custom_ct: customCt,
    custom_cu: customCu,
    custom_cm: customCm,
    custom_bd: customBd,
    schema_version: AGENT_CONFIG_SCHEMA_VERSION
  };
}

function serializeAgentConfig(config) {
  return `collect_interval=${config.collect_interval}` +
    `&ping_mode=${config.ping_mode}` +
    `&report_interval=${config.report_interval}` +
    `&reset_day=${config.reset_day}` +
    `&schema_version=${config.schema_version}` +
    `&custom_ct=${config.custom_ct}` +
    `&custom_cu=${config.custom_cu}` +
    `&custom_cm=${config.custom_cm}` +
    `&custom_bd=${config.custom_bd}`;
}

function serializeCorrection(correction) {
  if (correction === null || correction === undefined) return '';
  return `&rx_correction=${correction.rx_correction}` +
    `&tx_correction=${correction.tx_correction}`;
}

async function describeAgentConfig(server, settings = null) {
  const config = buildAgentConfig(server, settings);
  const serialized = serializeAgentConfig(config);
  const md5 = await md5Hash(serialized);

  const hasCorrection = server?.rx_correction != null || server?.tx_correction != null;
  let correction = null;
  if (hasCorrection) {
    correction = {
      rx_correction: normalizeTrafficCorrection(server.rx_correction),
      tx_correction: normalizeTrafficCorrection(server.tx_correction)
    };
  }

  return { config, serialized, md5, correction };
}

__mod.validateAgentConfigInput = validateAgentConfigInput;
__mod.AGENT_CONFIG_SCHEMA_VERSION = AGENT_CONFIG_SCHEMA_VERSION;
__mod.isValidTrafficCorrection = isValidTrafficCorrection;
__mod.AGENT_CONFIG_SCHEMA_HEADER = AGENT_CONFIG_SCHEMA_HEADER;
__mod.normalizeTrafficCorrection = normalizeTrafficCorrection;
__mod.AGENT_CONFIG_MD5_HEADER = AGENT_CONFIG_MD5_HEADER;
__mod.buildAgentConfig = buildAgentConfig;
__mod.MAX_TRAFFIC_CORRECTION_GB = MAX_TRAFFIC_CORRECTION_GB;
__mod.serializeAgentConfig = serializeAgentConfig;
__mod.serializeCorrection = serializeCorrection;
__mod.describeAgentConfig = describeAgentConfig;
}

// /src/handlers/frontend.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_handlers_frontend_js"] = {};

let filesCache = null;

async function loadFrontendFiles(env) {
  if (filesCache) return filesCache;

  try {
    const files = {};
    
    // 尝试从 Cloudflare Pages/Asset 绑定读取
    if (env.ASSETS) {
      try {
        // 主要文件
        const mainFiles = ['dashboard.html', 'style.css'];
        for (const filename of mainFiles) {
          try {
            const res = await env.ASSETS.fetch(new Request(`http://static/${filename}`));
            if (res.ok) {
              files[filename] = await res.text();
            }
          } catch (e) {
            // 忽略错误
          }
        }
      } catch (e) {
        console.log('[INFO] No ASSETS binding');
      }
    }

    filesCache = files;
    return filesCache;
  } catch (e) {
    console.error('[ERROR] Failed to load frontend files:', e);
    return {};
  }
}

function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function escapeCssString(str) {
  return String(str).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
}

function normalizeCspOrigin(value) {
  const raw = String(value || '').trim();
  if (!raw || /[\s;"']/.test(raw)) return '';
  try {
    const url = new URL(raw);
    if (url.protocol !== 'https:') return '';
    if (url.username || url.password || url.search || url.hash) return '';
    if (url.pathname && url.pathname !== '/') return '';
    return url.origin;
  } catch (_) {
    return '';
  }
}

function parseCspOrigins(value) {
  return [...new Set(String(value || '')
    .split(',')
    .map(normalizeCspOrigin)
    .filter(Boolean))];
}

function injectAppearanceSettings(html, settings) {
  let modifiedHtml = html;

  // 1. 更新页面标题
  const siteTitle = escapeHtml(settings.site_title || DEFAULT_SITE_TITLE);
  modifiedHtml = modifiedHtml.replace(/<title>.*<\/title>/, `<title>${siteTitle}</title>`);

  

  // 2. 追加 CSP 白名单域名
  const cspStatic = settings.csp_static || '';
  const cspApi = settings.csp_api || '';
  const staticDomains = parseCspOrigins(cspStatic);
  const rawApiDomains = parseCspOrigins(cspApi);

  // API 域名需要同时支持 https 和 wss（WebSocket）
  const apiDomains = [];
  for (const domain of rawApiDomains) {
    apiDomains.push(domain);
    if (domain.startsWith('https://')) {
      apiDomains.push(domain.replace('https://', 'wss://'));
    }
  }

  if (staticDomains.length > 0 || apiDomains.length > 0) {
    const turnstileDomain = 'https://challenges.cloudflare.com';
    const insightsDomain = 'https://static.cloudflareinsights.com';
    const fontsApiDomain = 'https://fonts.googleapis.com';
    const fontsStaticDomain = 'https://fonts.gstatic.com';

    // 从现有 CSP 中提取已有域名
    const cspMatch = modifiedHtml.match(/<meta http-equiv="Content-Security-Policy" content="([^"]+)">/);
    if (cspMatch) {
      const existingCsp = cspMatch[1];
      const domainRegex = /https?:\/\/[^\s';]+|wss?:\/\/[^\s';]+/g;
      const existingDomains = existingCsp.match(domainRegex) || [];

      // 按指令分类域名
      const scriptSrcDomains = [...new Set([
        ...existingDomains.filter(d => [turnstileDomain, insightsDomain].includes(d)),
        ...staticDomains
      ])].join(' ');

      const styleSrcDomains = [...new Set([
        ...existingDomains.filter(d => [turnstileDomain, fontsApiDomain].includes(d)),
        ...staticDomains
      ])].join(' ');

      const imgSrcDomains = [...new Set([
        ...existingDomains.filter(d => [turnstileDomain].includes(d)),
        ...staticDomains
      ])].join(' ');

      const fontSrcDomains = [...new Set([
        ...existingDomains.filter(d => [turnstileDomain, fontsStaticDomain].includes(d)),
        ...staticDomains
      ])].join(' ');

      const connectSrcDomains = [...new Set([
        ...existingDomains.filter(d => [turnstileDomain, insightsDomain].includes(d)),
        ...apiDomains
      ])].join(' ');

      // 构建新的 CSP
      const newCsp = [
        `default-src 'self'`,
        `script-src 'self' 'unsafe-inline' ${scriptSrcDomains}`,
        `style-src 'self' 'unsafe-inline' ${styleSrcDomains}`,
        `img-src 'self' ${imgSrcDomains} data:`,
        `font-src 'self' ${fontSrcDomains}`,
        `connect-src 'self' ${connectSrcDomains}`,
        `frame-src ${turnstileDomain}`,
        `form-action 'self'`,
        `object-src 'none'`,
        `base-uri 'self'`
      ].join(';');

      // 替换 CSP meta 标签（CSP 值不需要转义，它已经在双引号内）
      modifiedHtml = modifiedHtml.replace(
        cspMatch[0],
        `<meta http-equiv="Content-Security-Policy" content="${newCsp}">`
      );
    }
  }

  // 3. 注入 custom_head (在 </head> 标签前)
  if (settings.custom_head) {
    modifiedHtml = modifiedHtml.replace('</head>', `${settings.custom_head}\n</head>`);
  }

  // 4. 注入 custom_script (在 </body> 标签前)
  if (settings.custom_script) {
    modifiedHtml = modifiedHtml.replace('</body>', `<script>${settings.custom_script}</script>\n</body>`);
  }

  // 5. 注入 custom_bg (添加背景样式到 body)
  if (settings.custom_bg) {
    const safeBg = escapeCssString(settings.custom_bg);
    const bgStyle = `\n<style>\n  body { background-image: url('${safeBg}'); background-size: cover; background-attachment: fixed; background-position: center; }\n</style>\n`;
    modifiedHtml = modifiedHtml.replace('</head>', `${bgStyle}\n</head>`);
  }

  return modifiedHtml;
}

async function serveFrontend(request, env, settings = null) {
  const url = new URL(request.url);
  const path = url.pathname;

  const files = await loadFrontendFiles(env);
  
  // Vue SPA - 所有路由都返回 dashboard.html
  let html = files['dashboard.html'];

  if (html) {
    if (!settings) {
      settings = await loadAppearanceOptions(env.DB);
    }
    html = injectAppearanceSettings(html, settings);

    return new Response(html, {
      headers: {
        'Content-Type': 'text/html;charset=UTF-8',
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0',
        'CDN-Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
      }
    });
  }

  return new Response('Frontend not available. Please build the frontend first with `npm run build:frontend`.', {
    status: 503,
    headers: { 'Content-Type': 'text/plain' }
  });
}

__mod.serveFrontend = serveFrontend;
}

// /src/handlers/update.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_handlers_update_js"] = {};







// 将最新一次上报打包成前端可直接消费的 "当前状态" 对象
// 与 /api/server 和 /api/servers 返回的字段保持一致，便于页面直接合并
function buildPayloadForBroadcast(id, metrics, extra = {}) {
  const payload = {};
  mergeMetricsIntoServer(payload, metrics);
  payload.id = id;
  payload.region = extra.region || '';
  payload.last_updated = extra.timestamp || metrics.timestamp || Date.now();
  payload.timestamp = payload.last_updated;
  return payload;
}

// 批量推送：5秒窗口内合并向 DO 推送一次，减少请求次数
const BATCH_WINDOW = 5000;
const MAX_BATCH_SAMPLES = 300;
let batchQueue = new Map();
let flushingPromise = null;

// 用于过滤不需要实时更新的字段
const BROADCAST_DELETE_FIELDS = ['id', 'name', 'region', 'arch', 'os', 'cpu_info', 'cpu_cores', 'gpu_info', 'expire_date', 'server_group', 'traffic_limit', 'net_rx_monthly', 'net_tx_monthly', 'boot_time', 'timestamp', 'ip_v4', 'ip_v6'];

function normalizeTimestamp(value, fallback = Date.now()) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return fallback;
  return ts < 10000000000 ? ts * 1000 : ts;
}

function logUpdateBadRequest(reason, details = {}) {
  console.warn('[Update] 400 Bad Request:', reason, details);
}

function normalizeCorrectionValue(value) {
  if (value === null || value === undefined || value === '') return 0;
  return isValidTrafficCorrection(value) ? Number(value) : null;
}

function normalizeMetricSamples(data) {
  const now = Date.now();
  const rawSamples = Array.isArray(data.samples)
    ? data.samples
    : (Array.isArray(data.batch) ? data.batch : []);

  const samples = rawSamples.map(item => {
    if (!item || typeof item !== 'object') return null;
    const metrics = item.metrics || item.data || item.payload || item;
    if (!metrics || typeof metrics !== 'object') return null;
    const ts = normalizeTimestamp(item.ts ?? item.timestamp ?? metrics.timestamp, now);
    return { ts, metrics };
  }).filter(Boolean);

  if (samples.length === 0 && data.metrics && typeof data.metrics === 'object') {
    samples.push({
      ts: normalizeTimestamp(data.metrics.timestamp, now),
      metrics: data.metrics
    });
  }

  samples.sort((a, b) => a.ts - b.ts);
  return samples.slice(-MAX_BATCH_SAMPLES);
}

function toBroadcastSamples(id, samples, regionCode) {
  return samples.map(sample => {
    const payload = buildPayloadForBroadcast(id, sample.metrics || {}, {
      region: regionCode,
      timestamp: sample.ts
    });
    const filtered = Object.assign({}, payload);
    BROADCAST_DELETE_FIELDS.forEach(field => delete filtered[field]);
    return { ts: sample.ts, payload: filtered };
  });
}

function queueBroadcastSamples(serverId, samples) {
  if (!serverId || !Array.isArray(samples) || samples.length === 0) return;
  const existing = batchQueue.get(serverId);
  const merged = existing && Array.isArray(existing.samples)
    ? existing.samples.concat(samples)
    : samples;
  batchQueue.set(serverId, { samples: merged.slice(-MAX_BATCH_SAMPLES) });
}

async function _flushBatch(env) {
  flushingPromise = null;

  if (batchQueue.size === 0) return;

  // 原子性地取出当前队列，避免并发写入干扰
  const queue = batchQueue;
  batchQueue = new Map();

  const updates = [];
  for (const [serverId, item] of queue) {
    if (item && Array.isArray(item.samples) && item.samples.length > 0) {
      updates.push({ serverId, samples: item.samples });
    } else if (item) {
      const filtered = Object.assign({}, item);
      BROADCAST_DELETE_FIELDS.forEach(field => delete filtered[field]);
      updates.push({ serverId, payload: filtered });
    }
  }

  if (updates.length === 0) return;

  try {
    const id = env.METRICS_BROADCASTER.idFromName('global');
    const stub = env.METRICS_BROADCASTER.get(id);
    await stub.fetch('http://internal/batch-push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ updates })
    });
  } catch (e) {
    console.warn('[broadcast] batch push failed:', e.message || e);
  }
}

function _ensureBatchFlush(env) {
  if (flushingPromise) return flushingPromise;

  flushingPromise = new Promise(resolve => setTimeout(resolve, BATCH_WINDOW))
    .then(() => _flushBatch(env));

  return flushingPromise;
}

async function handleUpdate(request, env, ctx) {
  try {
    const data = await request.json();
    const { id, secret } = data;

    if (secret !== env.API_SECRET) {
      return createUnauthorizedResponse('Invalid secret');
    }

    let regionCode = request.cf?.country || request.headers?.get('cf-ipcountry') || '';

    const serverDetail = await getServerDetail(env.DB, id, true);

    if (!serverDetail) {
      return createNotFoundResponse('Server not found');
    }

    if (
      Object.prototype.hasOwnProperty.call(data, 'rx_correction') ||
      Object.prototype.hasOwnProperty.call(data, 'tx_correction')
    ) {
      const ackRx = normalizeCorrectionValue(data.rx_correction);
      const ackTx = normalizeCorrectionValue(data.tx_correction);
      if (ackRx === null || ackTx === null) {
        return createBadRequestResponse('Invalid correction');
      }

      await env.DB.prepare(`
        UPDATE servers
        SET rx_correction = NULL, tx_correction = NULL
        WHERE id = ?
          AND (rx_correction IS NOT NULL OR tx_correction IS NOT NULL)
          AND ABS(COALESCE(rx_correction, 0) - ?) < 0.000001
          AND ABS(COALESCE(tx_correction, 0) - ?) < 0.000001
      `).bind(id, ackRx, ackTx).run();
      clearServerDetailCache();

      return new Response('OK', {
        status: 200,
        headers: { 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    // 从缓存中获取历史记录分区 ID
    const historyPartitionId = serverDetail.history_partition_id;
    if(!historyPartitionId) {
      await ensureServerOptimization(env.DB, id);
      logUpdateBadRequest('Missing history_partition_id', {
        id,
        history_partition_id: serverDetail.history_partition_id
      });
      return createBadRequestResponse('Missing history_partition_id');
    }

    const samples = normalizeMetricSamples(data);
    if (samples.length === 0) {
      logUpdateBadRequest('Missing metrics', {
        id,
        has_metrics: !!data.metrics,
        has_samples: Array.isArray(data.samples),
        has_batch: Array.isArray(data.batch)
      });
      return createBadRequestResponse('Missing metrics');
    }

    // 获取最后一条插入（如果是批量数据，取最后一个样本）
    const latestSample = samples[samples.length - 1];
    await saveMetricsHistory(env.DB, id, historyPartitionId, latestSample.metrics, regionCode, latestSample.ts);

    const broadcastSamples = toBroadcastSamples(id, samples, regionCode);
    // 加入批量队列，由后台定时任务统一推送到 DO
    queueBroadcastSamples(id, broadcastSamples);
    ctx.waitUntil(_ensureBatchFlush(env));

    const clientConfigSchema = request.headers.get(AGENT_CONFIG_SCHEMA_HEADER);
    if (clientConfigSchema !== String(AGENT_CONFIG_SCHEMA_VERSION)) {
      return new Response('OK', {
        status: 200,
        headers: { 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    try {
      const settings = await loadSiteSettings(env.DB);
      const descriptor = await describeAgentConfig(serverDetail, settings);
      const clientConfigMd5 = (request.headers.get(AGENT_CONFIG_MD5_HEADER) || '').trim().toLowerCase();
      const hasCorrection = descriptor.correction !== null;
      const md5Changed = clientConfigMd5 !== descriptor.md5;
      const responseHeaders = {
        'Cache-Control': 'no-store',
        [AGENT_CONFIG_SCHEMA_HEADER]: String(AGENT_CONFIG_SCHEMA_VERSION),
        [AGENT_CONFIG_MD5_HEADER]: descriptor.md5
      };

      if (!md5Changed && !hasCorrection) {
        return new Response(null, { status: 204, headers: responseHeaders });
      }

      let body = descriptor.serialized;
      if (hasCorrection) {
        body += serializeCorrection(descriptor.correction);
      }

      return new Response(body, {
        status: 200,
        headers: {
          ...responseHeaders,
          'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'
        }
      });
    } catch (configError) {
      console.warn('[Update] Failed to build agent configuration:', configError?.message || configError);
      return new Response('OK', {
        status: 200,
        headers: { 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }
  } catch (e) {
    return createErrorResponse(e);
  }
}

// 暴露给 index.js 路由使用的 WebSocket 接入函数
async function handleWebSocketUpgrade(request, env) {
  if (!env || !env.METRICS_BROADCASTER) {
    return new Response(JSON.stringify({ error: 'WebSocket not enabled', code: 503 }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const url = new URL(request.url);
  const qs = url.search || '';
  try {
    const id = env.METRICS_BROADCASTER.idFromName('global');
    const stub = env.METRICS_BROADCASTER.get(id);
    const realOrigin = new URL(request.url).origin;
    const headers = new Headers(request.headers);
    headers.set('X-Real-Origin', realOrigin);
    return await stub.fetch(new Request(`http://internal/ws${qs}`, {
      method: request.method,
      headers,
      body: request.body,
      redirect: request.redirect
    }));
  } catch (e) {
    console.error('[ws] DO upgrade failed:', e);
    return new Response(JSON.stringify({ error: 'WebSocket error', code: 500 }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

__mod.handleUpdate = handleUpdate;
__mod.handleWebSocketUpgrade = handleWebSocketUpgrade;
}

// /src/handlers/dashboard.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_handlers_dashboard_js"] = {};





function withoutPrivateServerFields(server) {
  const item = { ...server };
  delete item.bandwidth;
  delete item.note;
  return item;
}

async function handleServerAPI(request, env, sys) {
  const isLoggedIn = await checkAuth(request, env, sys);
  
  if (sys.is_public !== 'true' && !isLoggedIn) {
    return simpleAuthResponse();
  }
  
  const url = new URL(request.url);
  const id = url.searchParams.get('id');
  
  if (!id) return createBadRequestResponse('Missing ID');
  
  const server = await getServerDetail(env.DB, id, isLoggedIn);
  if (!server) return createNotFoundResponse('Server not found');
  
  const latestMetrics = await getLatestMetrics(env.DB, id, server);
  mergeMetricsIntoServer(server, latestMetrics);
  server.sysConfig = {
    show_long_history: sys.show_long_history === 'true'
  };
  
  return createSuccessResponse(withoutPrivateServerFields(server));
}

async function handleServersAPI(request, env, sys) {
  const isLoggedIn = await checkAuth(request, env, sys);
  
  if (sys.is_public !== 'true' && !isLoggedIn) {
    return simpleAuthResponse();
  }
  
  const results = (await getAllServers(env.DB, isLoggedIn)).map(withoutPrivateServerFields);
  
  const latestMetricsMap = await getLatestMetricsForAllServers(env.DB);
  
  const now = Date.now();
  let globalOnline = 0;
  let globalSpeedIn = 0, globalSpeedOut = 0, globalNetTx = 0, globalNetRx = 0;
  const regionStats = {};
  
  for (const server of results) {
    const latestMetrics = latestMetricsMap.get(server.id);
    
    let isOnline = false;
    
    if (latestMetrics) {
      isOnline = (now - latestMetrics.timestamp) < 300000;
      mergeMetricsIntoServer(server, latestMetrics);
    }
    
    if (isOnline) {
      globalOnline++;
      globalSpeedIn += parseFloat(server.net_in_speed) || 0;
      globalSpeedOut += parseFloat(server.net_out_speed) || 0;
    }
    
    globalNetRx += parseFloat(server.net_rx || 0);
    globalNetTx += parseFloat(server.net_tx || 0);
    
    let cCode = (server.region || '').toUpperCase();
    if (cCode !== '') {
      regionStats[cCode] = (regionStats[cCode] || 0) + 1;
    }
  }
  
  const globalOffline = results.length - globalOnline;

  const data = {
    servers: results,
    stats: {
      total: results.length,
      online: globalOnline,
      offline: globalOffline,
      globalSpeedIn,
      globalSpeedOut,
      globalNetTx,
      globalNetRx
    },
    regionStats,
    sysConfig: {
      show_price: sys.show_price === 'true',
      show_expire: sys.show_expire === 'true',
      show_tf: sys.show_tf === 'true',
      show_time: sys.show_time === 'true'
    }
  };

  return createSuccessResponse(data);
}

__mod.handleServerAPI = handleServerAPI;
__mod.handleServersAPI = handleServersAPI;
}

// /src/utils/cors.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_utils_cors_js"] = {};
function parseAllowedOrigins(corsAllowedOrigins) {
  if (!corsAllowedOrigins || corsAllowedOrigins.trim() === '') {
    return [];
  }
  return corsAllowedOrigins
    .split(',')
    .map(o => o.trim())
    .filter(o => o !== '');
}

function getCorsAllowedOrigins(env) {
  return parseAllowedOrigins(env.CORS_ALLOWED_ORIGINS);
}

function isOriginAllowed(origin, allowedOrigins) {
  if (!origin || allowedOrigins.length === 0) {
    return false;
  }
  return allowedOrigins.includes(origin);
}

function createCorsHeaders(origin, allowedOrigins) {
  const headers = new Headers();
  
  if (isOriginAllowed(origin, allowedOrigins)) {
    headers.set('Access-Control-Allow-Origin', origin);
    headers.set('Access-Control-Allow-Credentials', 'true');
    headers.set('Vary', 'Origin');
  }
  
  return headers;
}

function createOptionsResponse(request, allowedOrigins) {
  const origin = request.headers.get('Origin');
  const headers = createCorsHeaders(origin, allowedOrigins);
  
  const requestMethod = request.headers.get('Access-Control-Request-Method');
  if (requestMethod) {
    headers.set('Access-Control-Allow-Methods', requestMethod);
  }
  
  const requestHeaders = request.headers.get('Access-Control-Request-Headers');
  if (requestHeaders) {
    headers.set('Access-Control-Allow-Headers', requestHeaders);
  }
  
  headers.set('Access-Control-Max-Age', '86400');
  headers.set('Content-Length', '0');
  
  return new Response(null, {
    status: 204,
    headers
  });
}

function applyCors(response, request, allowedOrigins) {
  const origin = request.headers.get('Origin');
  if (!isOriginAllowed(origin, allowedOrigins)) {
    return response;
  }
  
  const newHeaders = new Headers(response.headers);
  newHeaders.set('Access-Control-Allow-Origin', origin);
  newHeaders.set('Access-Control-Allow-Credentials', 'true');
  
  const vary = newHeaders.get('Vary') || '';
  if (!vary.includes('Origin')) {
    newHeaders.set('Vary', vary ? `${vary}, Origin` : 'Origin');
  }
  
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: newHeaders
  });
}
__mod.getCorsAllowedOrigins = getCorsAllowedOrigins;
__mod.isOriginAllowed = isOriginAllowed;
__mod.createCorsHeaders = createCorsHeaders;
__mod.createOptionsResponse = createOptionsResponse;
__mod.applyCors = applyCors;
}

// /src/durable/MetricsBroadcaster.js
{ const __mod = __mods["_tmp_CF_Server_Monitor_src_durable_MetricsBroadcaster_js"] = {};
// Durable Object: 服务器监控指标广播中心
// 负责维护 WebSocket 连接并在收到新指标时向订阅者实时推送
//
// - 连接通过 /api/ws?subscribe=<scope> 建立
//   scope = 'all'        -> 订阅所有服务器更新（首页）
//   scope = <serverId>   -> 只订阅某台服务器的更新（详情页）
//
// - 后端 /update 处理器在成功写入 DB 后，调用 /__do_push/<id>
//   由本 DO 向所有订阅者广播刚收到的指标。
//
// - 使用 DO WebSocket Hibernation API，闲置时休眠以节省资源。
//   通过 setWebSocketAutoResponse 自动响应 ping，无需唤醒 DO。

const MAX_SUBSCRIBE_IDS = 500;
const MAX_SERVER_ID_LENGTH = 64;
const SERVER_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;
const WS_POLICY_VIOLATION = 1008;

function parseAllowedOrigins(corsAllowedOrigins) {
  if (!corsAllowedOrigins || corsAllowedOrigins.trim() === '') {
    return [];
  }
  return corsAllowedOrigins
    .split(',')
    .map(o => o.trim())
    .filter(o => o !== '');
}

class _MetricsBroadcasterBase {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // 自动响应 ping 心跳，DO 无需被唤醒
    // @ts-ignore - Cloudflare Workers 运行时提供 WebSocketRequestResponsePair
    this.state.setWebSocketAutoResponse(
      // @ts-ignore
      new WebSocketRequestResponsePair(
        JSON.stringify({ type: 'ping' }),
        JSON.stringify({ type: 'pong' })
      )
    );
  }

  _isValidServerId(id) {
    return (
      typeof id === 'string' &&
      id.length > 0 &&
      id.length <= MAX_SERVER_ID_LENGTH &&
      SERVER_ID_PATTERN.test(id)
    );
  }

  _isValidScope(scope) {
    return scope === 'all' || this._isValidServerId(scope);
  }

  _normalizeServerIds(ids) {
    if (ids === undefined) return { ok: true, ids: [] };
    if (!Array.isArray(ids) || ids.length > MAX_SUBSCRIBE_IDS) {
      return { ok: false, ids: [] };
    }

    const seen = new Set();
    const normalized = [];
    for (const id of ids) {
      if (typeof id !== 'string') {
        return { ok: false, ids: [] };
      }

      const value = id.trim();
      if (!this._isValidServerId(value)) {
        return { ok: false, ids: [] };
      }

      if (seen.has(value)) continue;
      seen.add(value);
      normalized.push(value);
    }
    return { ok: true, ids: normalized };
  }

  _closeInvalidSubscription(ws) {
    try {
      ws.close(WS_POLICY_VIOLATION, 'invalid subscription');
    } catch (_) {}
  }

  _getSubscribeScope(msg, current) {
    if (!Object.prototype.hasOwnProperty.call(msg, 'scope') || msg.scope === undefined) {
      return current.scope || 'all';
    }
    return typeof msg.scope === 'string' ? msg.scope : null;
  }

  // 根据 scope 和 serverIds 判断是否需要接收某台服务器的更新
  _shouldDeliver(sessionScope, serverId, serverIds) {
    if (!sessionScope) return false;
    if (sessionScope === 'all') {
      if (!serverIds || serverIds.length === 0) return false;
      return serverIds.includes(serverId);
    }
    return sessionScope === serverId;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    // ── 1) WebSocket 接入 ──────────────────────────────
    if (path === '/ws' || path.endsWith('/ws')) {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        return new Response('Expected WebSocket upgrade request', { status: 426 });
      }

      const origin = request.headers.get('Origin');
      const allowedOrigins = parseAllowedOrigins(this.env.CORS_ALLOWED_ORIGINS);

      // Worker 转发时通过 X-Real-Origin 传递真实 origin，替代 DO 内部的 http://internal
      const realOrigin = request.headers.get('X-Real-Origin') || `${url.protocol}//${url.host}`;
      if (origin && allowedOrigins.length > 0 && !allowedOrigins.includes(origin) && origin !== realOrigin) {
        return new Response('Forbidden', { status: 403 });
      }

      const raw = url.searchParams.get('subscribe') || 'all';
      const scope = raw.trim().toLowerCase();
      if (!this._isValidScope(scope)) {
        return new Response('Invalid subscription scope', { status: 400 });
      }

      // @ts-ignore - Cloudflare Workers 运行时提供 WebSocketPair
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      // 使用 DO WebSocket Hibernation API 接管连接
      this.state.acceptWebSocket(server);

      // 将订阅 scope 和空 serverIds 附加到 WebSocket（休眠后仍保留）
      server.serializeAttachment({ scope, serverIds: [] });

      // 立即发送 hello 让客户端确认连接成功
      try {
        server.send(JSON.stringify({
          type: 'hello',
          ts: Date.now(),
          subscribed: scope
        }));
      } catch (_) {
      }

      const responseHeaders = new Headers();
      if (origin && allowedOrigins.length > 0) {
        responseHeaders.set('Access-Control-Allow-Origin', origin);
        responseHeaders.set('Access-Control-Allow-Credentials', 'true');
      } else if (allowedOrigins.length === 0) {
        responseHeaders.set('Access-Control-Allow-Origin', '*');
      }

      return new Response(null, {
        status: 101,
        webSocket: client,
        headers: responseHeaders
      });
    }

    // ── 2) 广播入口：/update 成功后由 Worker 内部转发 ──
    //     path: /push/<serverId>   body: { metrics } JSON
    if (method === 'POST' && (path.startsWith('/push/') || path.includes('/push/'))) {
      const parts = path.split('/push/');
      const serverId = decodeURIComponent((parts[1] || '').split('/')[0] || '');
      if (!serverId) {
        return new Response(JSON.stringify({ error: 'missing serverId' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      let payload = null;
      try {
        payload = await request.json();
      } catch (_) {
        return new Response(JSON.stringify({ error: 'invalid JSON' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      this._broadcast(serverId, payload);
      const count = this.state.getWebSockets().length;
      return new Response(JSON.stringify({ ok: true, subscribers: count }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ── 2b) 批量推送入口 ──────────────────────────────
    //     body: { updates: [{ serverId, payload }, ...] }
    if (method === 'POST' && path === '/batch-push') {
      let body = null;
      try {
        body = await request.json();
      } catch (_) {
        return new Response(JSON.stringify({ error: 'invalid JSON' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      const updates = body && body.updates;
      if (!Array.isArray(updates) || updates.length === 0) {
        return new Response(JSON.stringify({ error: 'missing or empty updates array' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      const normalizedUpdates = this._normalizeBatchUpdates(updates);
      if (normalizedUpdates.length === 0) {
        return new Response(JSON.stringify({ error: 'missing valid updates' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      this._broadcastBatch(normalizedUpdates);

      const count = this.state.getWebSockets().length;
      return new Response(JSON.stringify({ ok: true, count: normalizedUpdates.length, subscribers: count }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ── 3) 健康检查 ────────────────────────────────────
    if (method === 'GET' && (path === '/health' || path.endsWith('/health'))) {
      const count = this.state.getWebSockets().length;
      return new Response(JSON.stringify({ ok: true, subscribers: count }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response('Not found', { status: 404 });
  }

  // 向所有匹配 scope 的 WebSocket 广播推送
  _broadcast(serverId, payload) {
    const ts = Date.now();
    const updates = [{
      serverId,
      samples: [{ ts, data: payload }]
    }];
    this._broadcastBatch(updates);
  }

  // WebSocket 收到消息（ping 已被自动响应拦截，不会到达此处）
  _normalizeBatchUpdates(updates) {
    const now = Date.now();
    return updates.map(item => {
      if (!item || !item.serverId) return null;
      const serverId = String(item.serverId);
      const rawSamples = Array.isArray(item.samples)
        ? item.samples
        : (item.payload ? [{ ts: now, payload: item.payload }] : []);

      const samples = rawSamples.map(sample => {
        if (!sample || typeof sample !== 'object') return null;
        const data = sample.data || sample.payload || sample.metrics;
        if (!data || typeof data !== 'object') return null;
        const ts = Number(sample.ts || sample.timestamp || data.last_updated || now) || now;
        return { ts, data };
      }).filter(Boolean);

      if (samples.length === 0) return null;
      samples.sort((a, b) => a.ts - b.ts);
      return { serverId, samples };
    }).filter(Boolean);
  }

  _broadcastBatch(updates) {
    const ts = Date.now();
    const websockets = this.state.getWebSockets();

    for (const ws of websockets) {
      const attachment = ws.deserializeAttachment();
      if (!attachment) continue;

      const scopedUpdates = updates.filter(item => this._shouldDeliver(attachment.scope, item.serverId, attachment.serverIds));
      if (scopedUpdates.length === 0) continue;

      const message = JSON.stringify({
        type: 'batchUpdate',
        ts,
        updates: scopedUpdates
      });

      try {
        ws.send(message);
      } catch (_) {
        // WebSocket 已异常关闭，DO 会自动清理
      }
    }
  }

  webSocketMessage(ws, message) {
    // 保留处理扩展消息的入口
    try {
      const msg = JSON.parse(message || '{}');
      if (msg && msg.type === 'subscribe') {
        const current = ws.deserializeAttachment() || {};
        const rawScope = this._getSubscribeScope(msg, current);
        if (rawScope === null) {
          this._closeInvalidSubscription(ws);
          return;
        }

        const scope = rawScope.trim().toLowerCase();
        if (!this._isValidScope(scope)) {
          this._closeInvalidSubscription(ws);
          return;
        }

        const normalizedServerIds = this._normalizeServerIds(msg.ids);
        if (!normalizedServerIds.ok) {
          this._closeInvalidSubscription(ws);
          return;
        }

        const serverIds = normalizedServerIds.ids;
        ws.serializeAttachment({ scope, serverIds });
        try {
          ws.send(JSON.stringify({
            type: 'subscribed',
            ts: Date.now(),
            subscribed: scope,
            count: serverIds.length
          }));
        } catch (_) {}
        return;
      }
      if (msg && msg.type === 'pong') return;
    } catch (_) {}
  }

  // WebSocket 关闭 — DO 自动清理，无需手动移除
  webSocketClose(ws, code, reason) {}

  // WebSocket 错误 — DO 自动处理
  webSocketError(ws, error) {}
}

// default: _MetricsBroadcasterBase
__mod.default = _MetricsBroadcasterBase;
__mod._MetricsBroadcasterBase = _MetricsBroadcasterBase;
}

// /src/index.js
const { initDatabase, weeklyCleanup, getMetricsHistory, clearHistory } = __mods["_tmp_CF_Server_Monitor_src_database_schema_js"];

const { checkOfflineNodes, checkExpiringServers } = __mods["_tmp_CF_Server_Monitor_src_services_notification_js"];

const { updateDatabase } = __mods["_tmp_CF_Server_Monitor_src_database_updateDatabase_js"];

const { handleAdminAPI } = __mods["_tmp_CF_Server_Monitor_src_handlers_admin_js"];

const { serveFrontend } = __mods["_tmp_CF_Server_Monitor_src_handlers_frontend_js"];

const { handleUpdate, handleWebSocketUpgrade } = __mods["_tmp_CF_Server_Monitor_src_handlers_update_js"];

const { handleServerAPI, handleServersAPI } = __mods["_tmp_CF_Server_Monitor_src_handlers_dashboard_js"];

const { loadSettings, loadSiteSettings, loadAppearanceOptions, setDebug, debug, getCurrentVersion } = __mods["_tmp_CF_Server_Monitor_src_utils_settings_js"];

const { checkAuth, simpleAuthResponse } = __mods["_tmp_CF_Server_Monitor_src_middleware_auth_js"];

const { getServerDetail, getMetricsHistoryCache, setMetricsHistoryCache, getCacheDuration } = __mods["_tmp_CF_Server_Monitor_src_utils_cache_js"];

const { AppError, createSuccessResponse, createUnauthorizedResponse, createBadRequestResponse, createNotFoundResponse, createErrorResponse } = __mods["_tmp_CF_Server_Monitor_src_utils_errors_js"];

const { verifyTurnstileToken } = __mods["_tmp_CF_Server_Monitor_src_utils_common_js"];

const { getCorsAllowedOrigins, createOptionsResponse, applyCors } = __mods["_tmp_CF_Server_Monitor_src_utils_cors_js"];

// Durable Objects: 实时指标广播
// 显式 import + extends，确保 wrangler 静态分析器能在入口文件直接识别此 DO 类
const _MetricsBroadcaster = __mods["_tmp_CF_Server_Monitor_src_durable_MetricsBroadcaster_js"].MetricsBroadcaster;

export class MetricsBroadcaster extends _MetricsBroadcaster {}

async function getEncryptionKey(env, sys) {
  let secret = (sys && sys.jwt_secret) || env.TURNSTILE_SECRET_KEY || env.API_SECRET || 'default_secret_key_for_turnstile_encryption';
  secret += '_turnstile';
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(secret));
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    new Uint8Array(hash).slice(0, 32),
    { name: 'AES-GCM' },
    false,
    ['encrypt', 'decrypt']
  );
  return keyMaterial;
}

async function encryptTurnstileData(data, env, sys) {
  const key = await getEncryptionKey(env, sys);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoder = new TextEncoder();
  const encodedData = encoder.encode(JSON.stringify(data));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv: iv },
    key,
    encodedData
  );
  const combined = new Uint8Array(iv.length + ciphertext.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(ciphertext), iv.length);
  return btoa(String.fromCharCode(...combined));
}

async function decryptTurnstileData(encoded, env, sys) {
  try {
    const key = await getEncryptionKey(env, sys);
    const decoded = new Uint8Array(atob(encoded).split('').map(c => c.charCodeAt(0)));
    const iv = decoded.slice(0, 12);
    const ciphertext = decoded.slice(12);
    const decrypted = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv: iv },
      key,
      ciphertext
    );
    const encoder = new TextDecoder();
    return JSON.parse(encoder.decode(decrypted));
  } catch (e) {
    debug('Cookie decryption error:', e);
    return null;
  }
}

async function isTurnstileVerified(request, env, sys) {
  const verifiedHeader = request.headers.get('X-Turnstile-Verified');
  
  if (!verifiedHeader) return false;
  
  try {
    const decrypted = await decryptTurnstileData(verifiedHeader, env, sys);
    return decrypted && decrypted.expires && Date.now() < decrypted.expires * 1000;
  } catch {
    return false;
  }
}

async function fetchHistoryData(env, request, id, hours, columns, sys = null) {
  if (!id) return createBadRequestResponse('Missing ID');

  const ALLOWED_HOURS = [0.167, 0.5, 1, 6, 12, 24, 48, 96, 168];
  if (!ALLOWED_HOURS.includes(hours)) {
    return createBadRequestResponse('Invalid hours parameter');
  }
  
  if (!sys) {
    sys = await loadSiteSettings(env.DB);
  }
  const isLoggedIn = await checkAuth(request, env, sys);
  
  if (sys.is_public !== 'true' && !isLoggedIn) {
    return simpleAuthResponse();
  }
  
  if (hours > 1 && !isLoggedIn) {
    return createUnauthorizedResponse();
  }
  
  const server = await getServerDetail(env.DB, id, isLoggedIn);
  if (!server) return createNotFoundResponse();
  
  // 最多查询7天数据
  const clampedHours = Math.min(hours, 168);
  const cacheDuration = getCacheDuration(clampedHours);

  const cached = getMetricsHistoryCache(id, clampedHours, columns);
  if (cached && Date.now() - cached.timestamp < cacheDuration) {
    return createSuccessResponse(cached.data, { 'X-Cache': 'HIT' });
  }
  
  let data;
  try {
    data = await getMetricsHistory(env.DB, id, clampedHours, columns, server);
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    if (/no such column/i.test(message)) {
      debug('[History] 数据库字段缺失，可能尚未升级数据库:', message);
      return new Response(JSON.stringify({
        message: 'databaseUpgradeRequired'
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    throw e;
  }
  
  setMetricsHistoryCache(id, clampedHours, columns, data);
  
  return createSuccessResponse(data, { 'X-Cache': 'MISS' });
}

export default {
  async fetch(request, env, ctx) {
    setDebug(env.DEBUG);

    const url = new URL(request.url);
    const method = request.method;
    const path = url.pathname;

    const corsAllowedOrigins = getCorsAllowedOrigins(env);
    
    if (!env.API_SECRET || env.API_SECRET.length === 0) {
      const response = createBadRequestResponse('API_SECRET is required');
      return applyCors(response, request, corsAllowedOrigins);
    }
    
    if (method === 'OPTIONS') {
      return createOptionsResponse(request, corsAllowedOrigins);
    }

    if (env.ASSETS && method === 'GET') {
      try {
        const res = await env.ASSETS.fetch(new Request(`http://static${path}`, request));
        if (res.ok) {
          return applyCors(res, request, corsAllowedOrigins);
        }
      } catch (e) {
      }
    }

    const bypassTurnstilePaths = [
      '/admin/api',
      '/api/ws',
    ];

    const isApiRequest = path.startsWith('/api/') || path.startsWith('/admin/api');
    if (path === '/api/config' || path === '/clearHistory') {
      await initDatabase(env.DB);
    }

    // /api/config 在不带 X-Turnstile-Token 且不带 X-Turnstile-Verified 时仍然 bypass（用于初始化判断是否需要验证），
    // 带 token 或 verified header 时则走完整验证流程，以便复用 verified 字段返回验证结果
    const isTurnstileBypassed = (reqPath) => {
      if (bypassTurnstilePaths.includes(reqPath)) return true;
      if (reqPath === '/api/config' && !request.headers.get('X-Turnstile-Token') && !request.headers.get('X-Turnstile-Verified')) return true;
      return false;
    };

    let setTurnstileVerified = false;
    let sys = null;

    if (isApiRequest && !isTurnstileBypassed(path)) {
      sys = await loadSiteSettings(env.DB);
      const turnstileEnabled = sys.turnstile_enabled === 'true';
      const turnstileSecretKey = sys.turnstile_secret_key || '';
      
      // 全局 Turnstile 验证：仅 turnstile_enabled 开启时拦截所有 API 请求
      // turnstile_login_enabled 仅在登录时验证，不在此处拦截
      if (turnstileEnabled) {
        const hasValidCookie = await isTurnstileVerified(request, env, sys);
        
        if (!hasValidCookie) {
          const turnstileToken = request.headers.get('X-Turnstile-Token');
          const isVerified = await verifyTurnstileToken(turnstileToken, turnstileSecretKey);
          
          if (!isVerified) {
            const response = createErrorResponse(new AppError('Turnstile verification failed', 403));
            return applyCors(response, request, corsAllowedOrigins);
          }
          
          setTurnstileVerified = true;
        }
      }
    }

    async function ensureSiteSettings() {
      if (!sys) {
        sys = await loadSiteSettings(env.DB);
      }
      return sys;
    }

    async function ensureFullSettings() {
      sys = await loadSettings(env.DB);
      return sys;
    }

    const routes = [
      { method: 'POST', path: '/update', handler: () => handleUpdate(request, env, ctx) },
      { method: 'GET', path: '/__do/health', handler: async () => {
        if (!env.METRICS_BROADCASTER) {
          return createSuccessResponse({ ok: false, reason: 'DO not bound' });
        }
        try {
          const id = env.METRICS_BROADCASTER.idFromName('global');
          const stub = env.METRICS_BROADCASTER.get(id);
          return await stub.fetch('http://internal/health');
        } catch (e) {
          return createSuccessResponse({ ok: false, reason: e.message });
        }
      }},
      { method: 'GET', path: '/api/config', handler: async () => {
        await ensureSiteSettings();
        const appearanceOptions = await loadAppearanceOptions(env.DB);
        const turnstileEnabled = sys.turnstile_enabled === 'true';
        const turnstileLoginEnabled = sys.turnstile_login_enabled === 'true';
        let verified = false;
        let turnstileVerified = null;

        if (turnstileEnabled) {
          verified = await isTurnstileVerified(request, env, sys);
          if (setTurnstileVerified) {
            verified = true;
            const expires = Math.floor(Date.now() / 1000) + 3600;
            const cookieData = { expires, verified: true, timestamp: Date.now() };
            turnstileVerified = await encryptTurnstileData(cookieData, env, sys);
          }
        }

        const isLoggedIn = await checkAuth(request, env, sys);

        return createSuccessResponse({
          version: getCurrentVersion(),
          is_public: sys.is_public === 'true',
          authorization: isLoggedIn,
          turnstile_enabled: turnstileEnabled,
          turnstile_login_enabled: turnstileEnabled || turnstileLoginEnabled,
          turnstile_site_key: sys.turnstile_site_key || '',
          site_title: appearanceOptions.site_title || '',
          csp_static: appearanceOptions.csp_static || '',
          csp_api: appearanceOptions.csp_api || '',
          verified: verified,
          turnstile_verified: turnstileVerified,
          show_long_history: sys.show_long_history === 'true'
        });
      }},
      { method: 'GET', path: '/api/server', handler: async () => {
        await ensureSiteSettings();
        return handleServerAPI(request, env, sys);
      }},
      { method: 'GET', path: '/api/servers', handler: async () => {
        await ensureSiteSettings();
        return handleServersAPI(request, env, sys);
      }},
      { method: 'GET', path: '/api/ws', handler: async () => handleWebSocketUpgrade(request, env) },

      { method: 'GET', path: '/api/history/all', handler: async () => {
        await ensureSiteSettings();
        const id = url.searchParams.get('id');
        const hours = parseFloat(url.searchParams.get('hours') || '24');
        const allColumns = 'cpu, gpu, gpu_info, ram_total, ram_used, disk_total, disk_used, processes, net_in_speed, net_out_speed, tcp_conn, udp_conn, ping_ct, ping_cu, ping_cm, ping_bd, loss_ct, loss_cu, loss_cm, loss_bd, swap_total, swap_used, load_avg, region';
        // 后续版本可以删掉region 字段，用于升级数据库提示
        return fetchHistoryData(env, request, id, hours, allColumns, sys);
      }},
      { method: 'POST', path: '/admin/api', handler: async () => {
        await ensureSiteSettings();
        return handleAdminAPI(request, env, sys, ensureFullSettings);
      }},
      { method: 'POST', path: '/updateDatabase', handler: async () => {
        await ensureSiteSettings();
        if (!await checkAuth(request, env, sys)) {
          return simpleAuthResponse();
        }
        const result = await updateDatabase(env.DB);
        return createSuccessResponse(result);
      }},
      { method: 'POST', path: '/clearHistory', handler: async () => {
        await ensureSiteSettings();
        if (!await checkAuth(request, env, sys)) {
          return simpleAuthResponse();
        }
        const result = await clearHistory(env.DB);
        return createSuccessResponse(result);
      }}
    ];

    for (const route of routes) {
      if (route.method === method && route.path === path) {
        const response = await route.handler();

        // WebSocket 升级响应直接原样返回，不能修改 response 对象
        if (response.status === 101) {
          return response;
        }

        if (setTurnstileVerified) {
          const expires = Math.floor(Date.now() / 1000) + 3600;
          const cookieData = { expires, verified: true, timestamp: Date.now() };
          const encryptedData = await encryptTurnstileData(cookieData, env, sys);

          const finalHeaders = new Headers(response.headers);
          finalHeaders.set('Access-Control-Allow-Origin', request.headers.get('Origin') || '');
          finalHeaders.set('Access-Control-Allow-Credentials', 'true');
          finalHeaders.set('Vary', 'Origin');

          return new Response(response.body, {
            status: response.status,
            statusText: response.statusText,
            headers: finalHeaders
          });
        }

        return applyCors(response, request, corsAllowedOrigins);
      }
    }

    const appearanceOptions = await loadAppearanceOptions(env.DB);
    const frontendResponse = await serveFrontend(request, env, appearanceOptions);
    return applyCors(frontendResponse, request, corsAllowedOrigins);
  },

  async scheduled(event, env, ctx) {
    const cron = event.cron;
    debug(`[Cron] 定时任务触发: ${cron}`);

    const now = new Date();
    const day = now.getUTCDay();
    const hour = now.getUTCHours();
    const minute = now.getUTCMinutes();
    
    if (cron === '*/1 * * * *') {
      if (day === 0 && hour === 0 && minute < 5) {
        debug('[Cron] 每周日0:00-0:05表轮换期间，跳过离线节点检测');
      } else {
        debug('[Cron] 开始执行离线节点检测');
        await checkOfflineNodes(env.DB);
        debug('[Cron] 离线节点检测完成');
      }
    } else if (cron === '0 * * * *') {
      if (day === 0 && hour === 0) {
        debug('[Cron] 开始执行每周数据清理任务（表轮换）');
        await weeklyCleanup(env.DB);
        debug('[Cron] 每周数据清理任务完成');
      }
      
      if (hour === 12) {
        debug('[Cron] 开始执行服务器到期检测');
        await checkExpiringServers(env.DB);
        debug('[Cron] 服务器到期检测完成');
      }
    }else if(env.DEBUG == 1){
      if (cron === '0 0 * * 0') {
        debug('[Cron DEBUG] 开始执行每周数据清理任务（表轮换）');
        await weeklyCleanup(env.DB);
        debug('[Cron DEBUG] 每周数据清理任务完成');
      } else if (cron === '0 12 * * *') {
        debug('[Cron DEBUG] 开始执行服务器到期检测');
        await checkExpiringServers(env.DB);
        debug('[Cron DEBUG] 服务器到期检测完成');
      }
    }
  }
};
