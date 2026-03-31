import express from "express";
import bodyParser from "body-parser";
import crypto from "crypto";
import { google } from "googleapis";

const {
  PORT = 10000,
  APP_TZ = "America/Mexico_City",
  SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON,
  TELEGRAM_BOT_TOKEN,
  MINIAPP_BASE_URL,
  PUBLIC_BASE_URL,
} = process.env;

if (!SHEET_ID) throw new Error("Missing SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");

const app = express();
app.use(bodyParser.json({ limit: "35mb" }));
app.use(bodyParser.urlencoded({ extended: false, limit: "35mb" }));

const TELEGRAM_API = TELEGRAM_BOT_TOKEN ? `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}` : "";
const EVPLUS_VERSION = "EVPLUS_V1";
const EVPLUS_MIN_DIMENSION = 420;
const EVPLUS_MIN_ESTIMATED_BYTES = 9000;
const PHOTO_CELL_LIMIT = 48000;
const pendingEvidenceAnalysisTimers = new Map();

const ALLOWED_ORIGINS = [MINIAPP_BASE_URL, PUBLIC_BASE_URL, "http://localhost:5173", "https://localhost:5173"].filter(Boolean);

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin);
  }
  res.header("Vary", "Origin");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-telegram-init-data");
  res.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

function norm(value) {
  return (value ?? "").toString().trim();
}

function upper(value) {
  return norm(value).toUpperCase();
}

function safeNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function safeInt(value, fallback = 0) {
  const n = parseInt(String(value), 10);
  return Number.isFinite(n) ? n : fallback;
}

function isTrue(value) {
  return ["TRUE", "1", "SI", "SÍ", "YES", "VERDADERO"].includes(upper(value));
}

function unique(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function fitCell(value, limit = PHOTO_CELL_LIMIT) {
  const str = value == null ? "" : String(value);
  return str.length > limit ? str.slice(0, limit) : str;
}

function ymdInTZ(date = new Date(), tz = APP_TZ) {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function hmInTZ(date = new Date(), tz = APP_TZ) {
  return new Intl.DateTimeFormat("es-MX", {
    timeZone: tz,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function fmtDateTimeTZ(iso, tz = APP_TZ) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${ymdInTZ(d, tz)} ${hmInTZ(d, tz)}`;
}

function todayISO() {
  return ymdInTZ(new Date(), APP_TZ);
}

function nowISO() {
  return new Date().toISOString();
}

function buildExternalIdFromTelegramUser(userId) {
  return `telegram:${userId}`;
}

function buildBaseUrl(req) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL.replace(/\/+$/, "");
  const proto = String(req.headers["x-forwarded-proto"] || "https");
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "");
  return `${proto}://${host}`.replace(/\/+$/, "");
}

function headerRangeEnd(headerLength) {
  let dividend = headerLength;
  let columnName = "";
  while (dividend > 0) {
    const modulo = (dividend - 1) % 26;
    columnName = String.fromCharCode(65 + modulo) + columnName;
    dividend = Math.floor((dividend - modulo) / 26);
  }
  return columnName || "A";
}

function headerIndexMap(header) {
  const map = {};
  header.forEach((h, idx) => {
    if (norm(h)) map[norm(h)] = idx;
  });
  return map;
}

function toRadians(deg) {
  return (deg * Math.PI) / 180;
}

function haversineDistanceMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Math.round(R * c);
}

function classifyGeofence(distanceM, radiusM, accuracyM) {
  if (!Number.isFinite(distanceM)) return { result: "SIN_DATOS_GPS", severity: "MEDIA" };
  if (distanceM <= radiusM) return { result: "OK_EN_GEOCERCA", severity: "BAJA" };
  if (distanceM <= radiusM + (accuracyM || 0)) return { result: "OK_CON_TOLERANCIA_GPS", severity: "MEDIA" };
  return { result: "FUERA_DE_GEOCERCA", severity: "ALTA" };
}

function serializeGeoFallback(baseNotes, payload) {
  const parts = [norm(baseNotes)];
  parts.push(`GEO_RESULT:${payload.resultado || ""}`);
  parts.push(`GEO_DIST:${payload.distancia_m ?? ""}`);
  parts.push(`GEO_ACC:${payload.accuracy_m ?? ""}`);
  parts.push(`GEO_RAD:${payload.radio_tienda_m ?? ""}`);
  return parts.filter(Boolean).join(" | ");
}

function normalizePhotoForSheet(value, fallbackTag = "[IMAGE_TOO_LARGE_FOR_SHEETS]") {
  const str = norm(value);
  if (!str) return { value: "", overflow: false, originalLength: 0 };
  if (!str.startsWith("data:")) {
    return { value: fitCell(str), overflow: str.length > PHOTO_CELL_LIMIT, originalLength: str.length };
  }
  if (str.length <= PHOTO_CELL_LIMIT) {
    return { value: str, overflow: false, originalLength: str.length };
  }
  return { value: fallbackTag, overflow: true, originalLength: str.length };
}

function mergeOverflowNote(base, info) {
  const note = norm(base);
  if (!info?.overflow) return fitCell(note);
  const msg = `FOTO_EXCEDIDA_EN_SHEETS:${info.originalLength}`;
  return fitCell(note ? `${note} | ${msg}` : msg);
}

function parseDataUrl(dataUrl) {
  const raw = norm(dataUrl);
  const match = raw.match(/^data:([^;]+);base64,(.+)$/);
  if (!match) return null;
  try {
    return { mime: match[1], buffer: Buffer.from(match[2], "base64") };
  } catch {
    return null;
  }
}

function extractJpegDimensions(buffer) {
  if (!buffer || buffer.length < 4 || buffer[0] !== 0xff || buffer[1] !== 0xd8) return null;
  let offset = 2;
  while (offset < buffer.length) {
    while (offset < buffer.length && buffer[offset] !== 0xff) offset += 1;
    if (offset + 9 >= buffer.length) break;
    const marker = buffer[offset + 1];
    if ([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker)) {
      const height = buffer.readUInt16BE(offset + 5);
      const width = buffer.readUInt16BE(offset + 7);
      return { width, height };
    }
    if (marker === 0xd9 || marker === 0xda) break;
    const length = buffer.readUInt16BE(offset + 2);
    if (!length) break;
    offset += 2 + length;
  }
  return null;
}

function extractPngDimensions(buffer) {
  if (!buffer || buffer.length < 24) return null;
  const sig = "89504e470d0a1a0a";
  if (buffer.slice(0, 8).toString("hex") !== sig) return null;
  return { width: buffer.readUInt32BE(16), height: buffer.readUInt32BE(20) };
}

function extractWebpDimensions(buffer) {
  if (!buffer || buffer.length < 30) return null;
  if (buffer.slice(0, 4).toString() !== "RIFF" || buffer.slice(8, 12).toString() !== "WEBP") return null;
  const chunk = buffer.slice(12, 16).toString();
  if (chunk === "VP8X") {
    const width = 1 + buffer.readUIntLE(24, 3);
    const height = 1 + buffer.readUIntLE(27, 3);
    return { width, height };
  }
  if (chunk === "VP8 ") {
    const width = buffer.readUInt16LE(26) & 0x3fff;
    const height = buffer.readUInt16LE(28) & 0x3fff;
    return { width, height };
  }
  if (chunk === "VP8L") {
    const b1 = buffer[21];
    const b2 = buffer[22];
    const b3 = buffer[23];
    const b4 = buffer[24];
    const width = 1 + (((b2 & 0x3f) << 8) | b1);
    const height = 1 + (((b4 & 0x0f) << 10) | (b3 << 2) | ((b2 & 0xc0) >> 6));
    return { width, height };
  }
  return null;
}

function getImageMeta(photoValue) {
  const parsed = parseDataUrl(photoValue);
  if (!parsed) return { mime: "", width: 0, height: 0, estimated_bytes: 0, isDataUrl: false };
  let dims = null;
  if (parsed.mime === "image/png") dims = extractPngDimensions(parsed.buffer);
  else if (parsed.mime === "image/jpeg" || parsed.mime === "image/jpg") dims = extractJpegDimensions(parsed.buffer);
  else if (parsed.mime === "image/webp") dims = extractWebpDimensions(parsed.buffer);
  return {
    mime: parsed.mime,
    width: safeInt(dims?.width, 0),
    height: safeInt(dims?.height, 0),
    estimated_bytes: parsed.buffer.length,
    isDataUrl: true,
  };
}

function buildEvidencePhotoHash(photoValue, photoName = "") {
  const normalized = norm(photoValue);
  const digestInput = normalized.startsWith("data:") ? normalized : `${photoName}|${normalized.slice(0, 500)}`;
  return crypto.createHash("sha256").update(digestInput).digest("hex");
}

function normalizeTextKey(value) {
  return norm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function canonicalEvidenceTypeLabel(value) {
  const raw = norm(value);
  if (!raw) return "";
  const key = normalizeTextKey(raw);
  if (key.includes("ANAQUEL") && key.includes("ACERCAMIENTO") && key.includes("PRECIO")) {
    return "Anaquel/acercamiento/inventario/precios";
  }
  if (key === "COMPETENCIA") return "Competencia";
  if (key === "FACHADA TIENDA") return "Fachada tienda";
  return raw;
}

function evidenceTypeKey(value) {
  return normalizeTextKey(canonicalEvidenceTypeLabel(value));
}

function buildEvidenceGroupKey(visitaId, marcaId, tipoEvidencia, fase) {
  return [
    norm(visitaId),
    norm(marcaId),
    evidenceTypeKey(tipoEvidencia),
    upper(fase || "NA"),
  ].join("::");
}

function scheduleEvidenceGroupAnalysis({ visitaId, marcaId, tipoEvidencia, fase }) {
  const key = buildEvidenceGroupKey(visitaId, marcaId, tipoEvidencia, fase);
  const existing = pendingEvidenceAnalysisTimers.get(key);
  if (existing) clearTimeout(existing);
  const timer = setTimeout(async () => {
    pendingEvidenceAnalysisTimers.delete(key);
    try {
      await rerunEvidencePlusForGroup(visitaId, marcaId, tipoEvidencia, fase);
    } catch (error) {
      console.warn("scheduleEvidenceGroupAnalysis error", {
        key,
        message: error?.message || error,
      });
    }
  }, 2500);
  pendingEvidenceAnalysisTimers.set(key, timer);
}

function verifyTelegramInitData(initData) {
  if (!TELEGRAM_BOT_TOKEN) throw new Error("TELEGRAM_BOT_TOKEN no configurado para validar initData");
  const params = new URLSearchParams(initData);
  const hash = params.get("hash");
  if (!hash) throw new Error("initData inválido: falta hash");
  params.delete("hash");
  const dataCheckString = [...params.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
  const secret = crypto.createHmac("sha256", "WebAppData").update(TELEGRAM_BOT_TOKEN).digest();
  const calc = crypto.createHmac("sha256", secret).update(dataCheckString).digest("hex");
  if (calc !== hash) throw new Error("initData invalid");
  const authDate = safeInt(params.get("auth_date"), 0);
  if (authDate) {
    const ageSec = Math.floor(Date.now() / 1000) - authDate;
    if (ageSec > 60 * 60 * 24 * 3) throw new Error("initData expirado");
  }
  let user = null;
  try {
    user = JSON.parse(params.get("user") || "null");
  } catch {
    user = null;
  }
  if (!user?.id) throw new Error("initData sin usuario");
  return {
    telegramUser: user,
    external_id: buildExternalIdFromTelegramUser(user.id),
    raw: Object.fromEntries(params.entries()),
  };
}

async function getActorFromRequest(req) {
  const initData = norm(req.body?.initData || req.headers["x-telegram-init-data"] || "");
  if (!initData) throw new Error("initData requerido");
  const validated = verifyTelegramInitData(initData);
  const actor = await resolveActor(validated.external_id);
  return { validated, actor };
}

let sheetsClient = null;

async function getSheetsClient() {
  if (sheetsClient) return sheetsClient;
  const credentials = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const client = await auth.getClient();
  sheetsClient = google.sheets({ version: "v4", auth: client });
  return sheetsClient;
}

async function getSheetValues(range) {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
  return res.data.values || [];
}

async function appendSheetValues(range, values) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "USER_ENTERED",
    requestBody: { values },
  });
}

async function updateSheetValues(range, values) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "USER_ENTERED",
    requestBody: { values },
  });
}

async function getSheetHeader(sheetName) {
  const values = await getSheetValues(`${sheetName}!1:1`);
  return (values[0] || []).map((v) => norm(v));
}

async function getPolicyValidation() {
  try {
    const rows = await getSheetValues("POLITICA_VALIDACION!A2:D");
    const row = rows[0] || [];
    return {
      radio_estandar: safeNum(row[0], 100),
      sin_gps: upper(row[1] || "RECHAZAR"),
      tiempo_revision: safeNum(row[2], 30),
    };
  } catch {
    return { radio_estandar: 100, sin_gps: "RECHAZAR", tiempo_revision: 30 };
  }
}

async function getPromotorByExternalId(externalId) {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  for (const row of rows) {
    const primary = norm(row[0]);
    const promotorId = norm(row[1]);
    const nombre = norm(row[2]);
    const region = norm(row[3]);
    const cadena = norm(row[4]);
    const activo = row.length >= 6 ? isTrue(row[5]) : true;
    const supervisorExternal = norm(row[6]);
    const alternateTelegram = norm(row[7]);
    if ((primary === externalId || alternateTelegram === externalId) && activo) {
      return {
        external_id: primary || alternateTelegram,
        promotor_id: promotorId,
        nombre,
        region,
        cadena_principal: cadena,
        activo,
        supervisor_external_id: supervisorExternal,
      };
    }
  }
  return null;
}

async function getSupervisorByExternalId(externalId) {
  const rows = await getSheetValues("SUPERVISORES!A2:I");
  for (const row of rows) {
    const primary = norm(row[0]);
    const supervisorId = norm(row[1]);
    const nombre = norm(row[2]);
    const region = norm(row[3]);
    const nivel = norm(row[4]);
    const activo = row.length >= 6 ? isTrue(row[5]) : true;
    const telegramAlt = norm(row[7] || row[8]);
    if ((primary === externalId || telegramAlt === externalId) && activo) {
      return {
        external_id: primary || telegramAlt,
        supervisor_id: supervisorId,
        nombre,
        region,
        nivel,
        activo,
      };
    }
  }
  return null;
}

async function getPromotoresDeSupervisor(supervisorExternalId) {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  const raw = rows
    .filter((row) => {
      const activo = row.length >= 6 ? isTrue(row[5]) : true;
      return activo && norm(row[6]) === supervisorExternalId;
    })
    .map((row) => ({
      external_id: norm(row[0]) || norm(row[7]),
      promotor_id: norm(row[1]),
      nombre: norm(row[2]),
      region: norm(row[3]),
      cadena_principal: norm(row[4]),
      supervisor_external_id: norm(row[6]),
    }));
  const seen = new Set();
  return raw.filter((item) => {
    const key = `${item.promotor_id}::${item.external_id}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return !!(item.promotor_id || item.external_id);
  });
}

async function getClienteAccessByExternalId(externalId) {
  try {
    const rows = await getSheetValues("ACCESOS_CLIENTE!A2:G");
    for (const row of rows) {
      const activo = row.length >= 6 ? isTrue(row[5]) : true;
      if (norm(row[2]) === externalId && activo) {
        return {
          access_id: norm(row[0]),
          cliente_id: norm(row[1]),
          external_id: norm(row[2]),
          nombre_contacto: norm(row[3]),
          correo: norm(row[4]),
          activo,
          rol_cliente: norm(row[6] || "LECTURA"),
        };
      }
    }
    return null;
  } catch {
    return null;
  }
}

async function getClienteById(clienteId) {
  try {
    const rows = await getSheetValues("CLIENTES!A2:G");
    for (const row of rows) {
      const activo = row.length >= 3 ? isTrue(row[2]) : true;
      if (norm(row[0]) === clienteId && activo) {
        return {
          cliente_id: norm(row[0]),
          cliente_nombre: norm(row[1]),
          activo,
          logo_url: norm(row[3]),
          color_primario: norm(row[4]),
          correo_entregables: norm(row[5]),
          observaciones: norm(row[6]),
        };
      }
    }
    return null;
  } catch {
    return null;
  }
}

async function getClienteContextByExternalId(externalId) {
  const access = await getClienteAccessByExternalId(externalId);
  if (!access) return null;
  const cliente = await getClienteById(access.cliente_id);
  if (!cliente) return null;
  return { access, cliente };
}

async function resolveActor(externalId) {
  const supervisor = await getSupervisorByExternalId(externalId);
  if (supervisor) return { role: "supervisor", profile: supervisor };
  const promotor = await getPromotorByExternalId(externalId);
  if (promotor) return { role: "promotor", profile: promotor };
  const clienteCtx = await getClienteContextByExternalId(externalId);
  if (clienteCtx) {
    return {
      role: "cliente",
      profile: {
        external_id: externalId,
        nombre: clienteCtx.access.nombre_contacto || clienteCtx.cliente.cliente_nombre || "Cliente",
        cliente_id: clienteCtx.cliente.cliente_id,
        cliente_nombre: clienteCtx.cliente.cliente_nombre,
        rol_cliente: clienteCtx.access.rol_cliente,
      },
    };
  }
  return { role: "cliente", profile: { external_id: externalId, nombre: "Cliente" } };
}

async function findSessionRow(externalId) {
  try {
    const rows = await getSheetValues("SESIONES!A2:C");
    for (let i = 0; i < rows.length; i += 1) {
      if (norm(rows[i][0]) === externalId) {
        let data_json = {};
        try {
          data_json = rows[i][2] ? JSON.parse(rows[i][2]) : {};
        } catch {
          data_json = {};
        }
        return { rowIndex: i + 2, estado_actual: norm(rows[i][1]) || "MENU", data_json };
      }
    }
    return null;
  } catch {
    return null;
  }
}

async function upsertSessionData(externalId, patch = {}) {
  try {
    const found = await findSessionRow(externalId);
    const nextData = { ...(found?.data_json || {}), ...patch };
    const payload = [externalId, found?.estado_actual || "MENU", JSON.stringify(nextData)];
    if (!found) {
      await appendSheetValues("SESIONES!A2:C", [payload]);
      return true;
    }
    await updateSheetValues(`SESIONES!A${found.rowIndex}:C${found.rowIndex}`, [payload]);
    return true;
  } catch (error) {
    console.warn("upsertSessionData error", error?.message || error);
    return false;
  }
}

async function getChatIdByExternalId(externalId) {
  const found = await findSessionRow(externalId);
  const raw = found?.data_json?.chat_id;
  return raw ? String(raw) : "";
}

async function getTiendaMap() {
  const rows = await getSheetValues("TIENDAS!A2:L");
  const map = {};
  for (const row of rows) {
    const tienda_id = norm(row[0]);
    if (!tienda_id) continue;
    map[tienda_id] = {
      tienda_id,
      nombre_tienda: norm(row[1]),
      cadena: norm(row[2]),
      ciudad: norm(row[3]),
      region: norm(row[4]),
      activa: row.length >= 6 ? isTrue(row[5]) : true,
      direccion: norm(row[6]),
      lat: safeNum(row[7], NaN),
      lon: safeNum(row[8], NaN),
      radio_m: safeNum(row[9], 0),
      supervisor_id: norm(row[10]),
      cliente_id: norm(row[11]),
    };
  }
  return map;
}

async function getTiendasAsignadas(promotorId) {
  const rows = await getSheetValues("ASIGNACIONES!A2:E");
  return unique(
    rows
      .filter((row) => norm(row[0]) === promotorId && (row.length < 4 || isTrue(row[3] ?? "TRUE")))
      .map((row) => norm(row[1]))
  );
}

async function getVisitsHeader() {
  return getSheetHeader("VISITAS");
}

function parseVisitRow(row, idx, header) {
  const map = headerIndexMap(header);
  return {
    rowIndex: idx + 2,
    visita_id: norm(row[map.visita_id]),
    promotor_id: norm(row[map.promotor_id]),
    tienda_id: norm(row[map.tienda_id]),
    fecha: norm(row[map.fecha]),
    hora_inicio: norm(row[map.hora_inicio]),
    hora_fin: norm(row[map.hora_fin]),
    notas: norm(row[map.notas]),
    estado_visita: norm(row[map.estado_visita]),
    resultado_geocerca_entrada: norm(row[map.resultado_geocerca_entrada]),
    distancia_entrada_m: norm(row[map.distancia_entrada_m]),
    accuracy_entrada_m: norm(row[map.accuracy_entrada_m]),
    resultado_geocerca_salida: norm(row[map.resultado_geocerca_salida]),
    distancia_salida_m: norm(row[map.distancia_salida_m]),
    accuracy_salida_m: norm(row[map.accuracy_salida_m]),
    lat_tienda: norm(row[map.lat_tienda]),
    lon_tienda: norm(row[map.lon_tienda]),
    radio_tienda_m: norm(row[map.radio_tienda_m]),
  };
}

function buildVisitRowFromHeader(header, payload) {
  const row = new Array(header.length).fill("");
  header.forEach((name, idx) => {
    switch (name) {
      case "visita_id": row[idx] = payload.visita_id; break;
      case "promotor_id": row[idx] = payload.promotor_id; break;
      case "tienda_id": row[idx] = payload.tienda_id; break;
      case "fecha": row[idx] = payload.fecha; break;
      case "hora_inicio": row[idx] = payload.hora_inicio || ""; break;
      case "hora_fin": row[idx] = payload.hora_fin || ""; break;
      case "notas": row[idx] = payload.notas || ""; break;
      case "estado_visita": row[idx] = payload.estado_visita || ""; break;
      case "resultado_geocerca_entrada": row[idx] = payload.resultado_geocerca_entrada || ""; break;
      case "distancia_entrada_m": row[idx] = payload.distancia_entrada_m || ""; break;
      case "accuracy_entrada_m": row[idx] = payload.accuracy_entrada_m || ""; break;
      case "resultado_geocerca_salida": row[idx] = payload.resultado_geocerca_salida || ""; break;
      case "distancia_salida_m": row[idx] = payload.distancia_salida_m || ""; break;
      case "accuracy_salida_m": row[idx] = payload.accuracy_salida_m || ""; break;
      case "lat_tienda": row[idx] = payload.lat_tienda || ""; break;
      case "lon_tienda": row[idx] = payload.lon_tienda || ""; break;
      case "radio_tienda_m": row[idx] = payload.radio_tienda_m || ""; break;
      default: break;
    }
  });
  return row;
}

async function getVisitasAllByPromotor(promotorId) {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${headerRangeEnd(header.length)}`);
  return rows.filter((row) => norm(row[1]) === promotorId).map((row, idx) => parseVisitRow(row, idx, header));
}

async function getVisitasToday(promotorId) {
  const visits = await getVisitasAllByPromotor(promotorId);
  return visits.filter((visit) => visit.fecha === todayISO());
}

async function getOpenVisitsToday(promotorId) {
  const visits = await getVisitasToday(promotorId);
  return visits.filter((visit) => !visit.hora_fin);
}

async function getVisitById(visitaId) {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${headerRangeEnd(header.length)}`);
  for (let i = 0; i < rows.length; i += 1) {
    const visit = parseVisitRow(rows[i], i, header);
    if (visit.visita_id === visitaId) return visit;
  }
  return null;
}

async function createVisitWithGeofence(promotorId, tiendaId, baseNotes, geofence) {
  const header = await getVisitsHeader();
  const visitId = `V-${Date.now()}`;
  const payload = {
    visita_id: visitId,
    promotor_id: promotorId,
    tienda_id: tiendaId,
    fecha: todayISO(),
    hora_inicio: nowISO(),
    hora_fin: "",
    notas: baseNotes,
    estado_visita: geofence.resultado === "FUERA_DE_GEOCERCA" ? "ABIERTA_CON_ALERTA" : "ABIERTA",
    resultado_geocerca_entrada: geofence.resultado,
    distancia_entrada_m: geofence.distancia_m,
    accuracy_entrada_m: geofence.accuracy_m,
    resultado_geocerca_salida: "",
    distancia_salida_m: "",
    accuracy_salida_m: "",
    lat_tienda: geofence.lat_tienda,
    lon_tienda: geofence.lon_tienda,
    radio_tienda_m: geofence.radio_tienda_m,
  };
  if (!header.includes("resultado_geocerca_entrada")) {
    payload.notas = serializeGeoFallback(baseNotes, {
      resultado: geofence.resultado,
      distancia_m: geofence.distancia_m,
      accuracy_m: geofence.accuracy_m,
      radio_tienda_m: geofence.radio_tienda_m,
    });
  }
  const row = buildVisitRowFromHeader(header, payload);
  await appendSheetValues(`VISITAS!A2:${headerRangeEnd(header.length)}`, [row]);
  return visitId;
}

async function closeVisitWithGeofence(visit, closeNotes, geofence) {
  const header = await getVisitsHeader();
  const payload = {
    ...visit,
    hora_fin: nowISO(),
    notas: closeNotes || visit.notas,
    estado_visita: geofence.resultado === "FUERA_DE_GEOCERCA" || upper(visit.resultado_geocerca_entrada) === "FUERA_DE_GEOCERCA" ? "CERRADA_CON_ALERTA" : "CERRADA",
    resultado_geocerca_salida: geofence.resultado,
    distancia_salida_m: geofence.distancia_m,
    accuracy_salida_m: geofence.accuracy_m,
    lat_tienda: geofence.lat_tienda,
    lon_tienda: geofence.lon_tienda,
    radio_tienda_m: geofence.radio_tienda_m,
  };
  if (!header.includes("resultado_geocerca_salida")) {
    payload.notas = serializeGeoFallback(closeNotes || visit.notas, {
      resultado: geofence.resultado,
      distancia_m: geofence.distancia_m,
      accuracy_m: geofence.accuracy_m,
      radio_tienda_m: geofence.radio_tienda_m,
    });
  }
  const row = buildVisitRowFromHeader(header, payload);
  await updateSheetValues(`VISITAS!A${visit.rowIndex}:${headerRangeEnd(header.length)}${visit.rowIndex}`, [row]);
}

async function getAlertsHeader() {
  return getSheetHeader("ALERTAS");
}

function parseAlertRow(row, idx, header) {
  const map = headerIndexMap(header);
  return {
    rowIndex: idx + 2,
    alerta_id: norm(row[map.alerta_id]),
    fecha_hora: norm(row[map.fecha_hora]),
    promotor_id: norm(row[map.promotor_id]),
    visita_id: norm(row[map.visita_id]),
    evidencia_id: norm(row[map.evidencia_id]),
    tipo_alerta: norm(row[map.tipo_alerta]),
    severidad: upper(row[map.severidad] || "MEDIA"),
    descripcion: norm(row[map.descripcion]),
    status: upper(row[map.status] || "ABIERTA"),
    supervisor_id: norm(row[map.supervisor_id]),
    tienda_id: norm(row[map.tienda_id]),
    atendida_por: norm(row[map.atendida_por]),
    fecha_atencion: norm(row[map.fecha_atencion] ?? row[map.atendida_at]),
    canal_notificacion: norm(row[map.canal_notificacion]),
    comentario_cierre: norm(row[map.comentario_cierre]),
    origen_cierre: norm(row[map.origen_cierre]),
  };
}

function buildAlertRowFromHeader(header, payload) {
  const row = new Array(header.length).fill("");
  header.forEach((name, idx) => {
    switch (name) {
      case "alerta_id": row[idx] = payload.alerta_id || ""; break;
      case "fecha_hora": row[idx] = payload.fecha_hora || ""; break;
      case "promotor_id": row[idx] = payload.promotor_id || ""; break;
      case "visita_id": row[idx] = payload.visita_id || ""; break;
      case "evidencia_id": row[idx] = payload.evidencia_id || ""; break;
      case "tipo_alerta": row[idx] = payload.tipo_alerta || ""; break;
      case "severidad": row[idx] = payload.severidad || ""; break;
      case "descripcion": row[idx] = payload.descripcion || ""; break;
      case "status": row[idx] = payload.status || ""; break;
      case "supervisor_id": row[idx] = payload.supervisor_id || ""; break;
      case "tienda_id": row[idx] = payload.tienda_id || ""; break;
      case "atendida_por": row[idx] = payload.atendida_por || ""; break;
      case "fecha_atencion": row[idx] = payload.fecha_atencion || payload.atendida_at || ""; break;
      case "atendida_at": row[idx] = payload.atendida_at || payload.fecha_atencion || ""; break;
      case "canal_notificacion": row[idx] = payload.canal_notificacion || ""; break;
      case "comentario_cierre": row[idx] = payload.comentario_cierre || ""; break;
      case "origen_cierre": row[idx] = payload.origen_cierre || ""; break;
      default: break;
    }
  });
  return row;
}

async function notifySupervisorAlert(payload) {
  try {
    const supervisorExternalId = norm(payload.supervisor_id);
    if (!supervisorExternalId || !TELEGRAM_API) return false;
    const chatId = await getChatIdByExternalId(supervisorExternalId);
    if (!chatId) return false;
    const tiendaMap = await getTiendaMap();
    const tiendaNombre = tiendaMap[payload.tienda_id]?.nombre_tienda || payload.tienda_id || "Tienda";
    const text = [
      "🚨 *Nueva alerta*",
      "",
      `Tipo: *${payload.tipo_alerta || "ALERTA"}*`,
      `Severidad: *${payload.severidad || "MEDIA"}*`,
      `Tienda: *${tiendaNombre}*`,
      payload.descripcion ? `Detalle: ${payload.descripcion}` : "",
    ].filter(Boolean).join("\n");
    await sendTelegramText(chatId, text, { inline_keyboard: [[{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }]] });
    return true;
  } catch (error) {
    console.warn("notifySupervisorAlert error", error?.message || error);
    return false;
  }
}

async function createAlert(payload) {
  try {
    const header = await getAlertsHeader();
    if (!header.length) return false;
    const row = buildAlertRowFromHeader(header, payload);
    await appendSheetValues(`ALERTAS!A2:${headerRangeEnd(header.length)}`, [row]);
    await notifySupervisorAlert(payload);
    return true;
  } catch (error) {
    console.warn("createAlert error", error?.message || error);
    return false;
  }
}

async function getAlertsAll() {
  const header = await getAlertsHeader();
  const rows = await getSheetValues(`ALERTAS!A2:${headerRangeEnd(header.length)}`);
  return rows.map((row, idx) => parseAlertRow(row, idx, header));
}

async function getAlertById(alertaId) {
  const header = await getAlertsHeader();
  const rows = await getSheetValues(`ALERTAS!A2:${headerRangeEnd(header.length)}`);
  for (let i = 0; i < rows.length; i += 1) {
    const alert = parseAlertRow(rows[i], i, header);
    if (alert.alerta_id === alertaId) return { header, alert };
  }
  return null;
}

async function updateAlertRow(header, alert, patch = {}) {
  const row = buildAlertRowFromHeader(header, { ...alert, ...patch });
  await updateSheetValues(`ALERTAS!A${alert.rowIndex}:${headerRangeEnd(header.length)}${alert.rowIndex}`, [row]);
}

async function getEvidenciasHeader() {
  return getSheetHeader("EVIDENCIAS");
}

function parseEvidenceRow(row, idx, header) {
  const map = headerIndexMap(header);
  return {
    rowIndex: idx + 2,
    evidencia_id: norm(row[map.evidencia_id]),
    external_id: norm(row[map.external_id]),
    fecha_hora: norm(row[map.fecha_hora]),
    tipo_evento: norm(row[map.tipo_evento]),
    origen: norm(row[map.origen]),
    jornada_id: norm(row[map.jornada_id]),
    visita_id: norm(row[map.visita_id]),
    url_foto: norm(row[map.url_foto]),
    lat: norm(row[map.lat]),
    lon: norm(row[map.lon]),
    resultado_ai: norm(row[map.resultado_ai]),
    score_confianza: norm(row[map.score_confianza]),
    riesgo: upper(row[map.riesgo] || "BAJO"),
    marca_id: norm(row[map.marca_id]),
    producto_id: norm(row[map.producto_id]),
    tipo_evidencia: canonicalEvidenceTypeLabel(row[map.tipo_evidencia]),
    descripcion: norm(row[map.descripcion]),
    status: upper(row[map.status] || "ACTIVA"),
    note: norm(row[map.note]),
    fase: norm(row[map.fase]),
    foto_nombre: norm(row[map.foto_nombre]),
    accuracy: norm(row[map.accuracy]),
    requiere_revision_supervisor: norm(row[map.requiere_revision_supervisor]),
    revisado_por: norm(row[map.revisado_por]),
    fecha_revision: norm(row[map.fecha_revision]),
    decision_supervisor: norm(row[map.decision_supervisor]),
    motivo_revision: norm(row[map.motivo_revision]),
    hallazgos_ai: norm(row[map.hallazgos_ai]),
    reglas_disparadas: norm(row[map.reglas_disparadas]),
    analizado_at: norm(row[map.analizado_at]),
    version_motor_ai: norm(row[map.version_motor_ai]),
    hash_foto: norm(row[map.hash_foto]),
  };
}

function buildEvidenceRowFromHeader(header, payload) {
  const row = new Array(header.length).fill("");
  header.forEach((name, idx) => {
    const key = norm(name);
    row[idx] = payload[key] != null ? payload[key] : "";
  });
  return row;
}

async function getEvidenceById(evidenciaId) {
  const header = await getEvidenciasHeader();
  const rows = await getSheetValues(`EVIDENCIAS!A2:${headerRangeEnd(header.length)}`);
  for (let i = 0; i < rows.length; i += 1) {
    const evidence = parseEvidenceRow(rows[i], i, header);
    if (evidence.evidencia_id === evidenciaId) return { header, evidence };
  }
  return null;
}

async function getEvidenciasAll() {
  const header = await getEvidenciasHeader();
  const rows = await getSheetValues(`EVIDENCIAS!A2:${headerRangeEnd(header.length)}`);
  return rows.map((row, idx) => parseEvidenceRow(row, idx, header));
}

async function getEvidenciasTodayByExternalId(externalId) {
  const all = await getEvidenciasAll();
  return all.filter((row) => row.external_id === externalId && row.fecha_hora && ymdInTZ(new Date(row.fecha_hora), APP_TZ) === todayISO());
}

async function getEvidenciasByVisitId(visitaId) {
  const all = await getEvidenciasAll();
  return all.filter((row) => row.visita_id === visitaId);
}

async function registrarEvidencia(payload) {
  const result = await registrarEvidenciasBatch([payload]);
  return { photoOverflow: result.photoOverflow, originalPhotoLength: result.maxOriginalPhotoLength || 0 };
}

async function registrarEvidenciasBatch(payloads = []) {
  if (!Array.isArray(payloads) || !payloads.length) {
    return { photoOverflow: false, maxOriginalPhotoLength: 0 };
  }
  const header = await getEvidenciasHeader();
  const rows = [];
  let photoOverflow = false;
  let maxOriginalPhotoLength = 0;

  for (const payload of payloads) {
    const photoInfo = normalizePhotoForSheet(payload.url_foto);
    const safeNote = mergeOverflowNote(payload.note || "", photoInfo);
    photoOverflow = photoOverflow || photoInfo.overflow;
    maxOriginalPhotoLength = Math.max(maxOriginalPhotoLength, photoInfo.originalLength || 0);
    const rowPayload = {
      evidencia_id: fitCell(payload.evidencia_id),
      external_id: fitCell(payload.external_id || ""),
      fecha_hora: fitCell(payload.fecha_hora || nowISO()),
      tipo_evento: fitCell(payload.tipo_evento || ""),
      origen: fitCell(payload.origen || ""),
      jornada_id: fitCell(payload.jornada_id || ""),
      visita_id: fitCell(payload.visita_id || ""),
      url_foto: photoInfo.value,
      lat: fitCell(payload.lat || ""),
      lon: fitCell(payload.lon || ""),
      resultado_ai: fitCell(payload.resultado_ai || ""),
      score_confianza: fitCell(payload.score_confianza || ""),
      riesgo: fitCell(payload.riesgo || "BAJO"),
      marca_id: fitCell(payload.marca_id || ""),
      producto_id: fitCell(payload.producto_id || ""),
      tipo_evidencia: fitCell(canonicalEvidenceTypeLabel(payload.tipo_evidencia || "")),
      descripcion: fitCell(payload.descripcion || ""),
      status: fitCell(payload.status || "ACTIVA"),
      note: safeNote,
      fase: fitCell(payload.fase || ""),
      foto_nombre: fitCell(payload.foto_nombre || ""),
      accuracy: fitCell(payload.accuracy || ""),
      requiere_revision_supervisor: fitCell(payload.requiere_revision_supervisor || "FALSE"),
      revisado_por: fitCell(payload.revisado_por || ""),
      fecha_revision: fitCell(payload.fecha_revision || ""),
      decision_supervisor: fitCell(payload.decision_supervisor || ""),
      motivo_revision: fitCell(payload.motivo_revision || ""),
      hallazgos_ai: fitCell(payload.hallazgos_ai || ""),
      reglas_disparadas: fitCell(payload.reglas_disparadas || ""),
      analizado_at: fitCell(payload.analizado_at || ""),
      version_motor_ai: fitCell(payload.version_motor_ai || ""),
      hash_foto: fitCell(payload.hash_foto || ""),
    };
    rows.push(buildEvidenceRowFromHeader(header, rowPayload));
  }

  await appendSheetValues(`EVIDENCIAS!A2:${headerRangeEnd(header.length)}`, rows);
  return { photoOverflow, maxOriginalPhotoLength };
}

async function updateEvidenceRow(header, evidence, patch = {}) {
  const next = { ...evidence, ...patch };
  next.tipo_evidencia = canonicalEvidenceTypeLabel(next.tipo_evidencia);
  const photoInfo = normalizePhotoForSheet(next.url_foto ?? evidence.url_foto);
  next.url_foto = photoInfo.value;
  next.note = mergeOverflowNote(next.note ?? evidence.note, photoInfo);
  const row = buildEvidenceRowFromHeader(header, next);
  await updateSheetValues(`EVIDENCIAS!A${evidence.rowIndex}:${headerRangeEnd(header.length)}${evidence.rowIndex}`, [row]);
  return { photoOverflow: photoInfo.overflow, originalPhotoLength: photoInfo.originalLength };
}

async function getMarcasActivas() {
  const rows = await getSheetValues("MARCAS!A2:D");
  return rows
    .filter((row) => norm(row[0]) && (row.length < 3 || isTrue(row[2])))
    .map((row) => ({ marca_id: norm(row[0]), marca_nombre: norm(row[1]), cliente_id: norm(row[3]) }))
    .sort((a, b) => a.marca_nombre.localeCompare(b.marca_nombre));
}

async function getMarcaMap() {
  const rows = await getSheetValues("MARCAS!A2:D");
  const map = {};
  rows.forEach((row) => {
    const id = norm(row[0]);
    if (!id) return;
    map[id] = { marca_id: id, marca_nombre: norm(row[1]), activa: row.length < 3 || isTrue(row[2]), cliente_id: norm(row[3]) };
  });
  return map;
}

async function resolveMarcaIdByName(marcaNombre) {
  const target = upper(marcaNombre);
  if (!target) return "";
  const rows = await getSheetValues("MARCAS!A2:D");
  for (const row of rows) {
    if (upper(row[1]) === target) return norm(row[0]);
  }
  return "";
}

async function getTiposEvidenciaCatalog() {
  try {
    const rows = await getSheetValues("TIPOS_EVIDENCIA!A2:C");
    const seen = new Set();
    const items = [];
    for (const row of rows) {
      const tipo = canonicalEvidenceTypeLabel(row[0]);
      if (!tipo) continue;
      const key = evidenceTypeKey(tipo);
      if (seen.has(key)) continue;
      seen.add(key);
      items.push({
        tipo_evidencia: tipo,
        descripcion_corta: norm(row[1]),
        fotos_requeridas: safeInt(row[2], 1),
      });
    }
    return items;
  } catch {
    return [];
  }
}

async function getReglasPorMarca(marcaId) {
  let rules = [];
  try {
    const rows = await getSheetValues("REGLAS_EVIDENCIA!A2:E");
    rules = rows
      .filter((row) => norm(row[0]) === marcaId && (row.length < 5 || isTrue(row[4] ?? "TRUE")))
      .map((row) => ({
        marca_id: marcaId,
        tipo_evidencia: canonicalEvidenceTypeLabel(row[1]),
        fotos_requeridas: safeInt(row[2], 1),
        requiere_antes_despues: isTrue(row[3]),
        origen: "REGLAS_EVIDENCIA",
      }));
  } catch {
    rules = [];
  }
  const catalog = await getTiposEvidenciaCatalog();
  const merged = [];
  const byType = new Map();

  const pushUniqueRule = (rule) => {
    const key = evidenceTypeKey(rule.tipo_evidencia);
    if (!key) return;
    if (byType.has(key)) {
      const existing = byType.get(key);
      if (existing.origen === "TIPOS_EVIDENCIA" && rule.origen === "REGLAS_EVIDENCIA") {
        const idx = merged.findIndex((item) => evidenceTypeKey(item.tipo_evidencia) === key);
        if (idx >= 0) merged[idx] = rule;
        byType.set(key, rule);
      }
      return;
    }
    byType.set(key, rule);
    merged.push(rule);
  };

  rules.forEach((rule) => pushUniqueRule(rule));
  catalog.forEach((item) => {
    pushUniqueRule({
      marca_id: marcaId,
      tipo_evidencia: canonicalEvidenceTypeLabel(item.tipo_evidencia),
      fotos_requeridas: item.fotos_requeridas || 1,
      requiere_antes_despues: false,
      descripcion_corta: item.descripcion_corta,
      origen: "TIPOS_EVIDENCIA",
    });
  });

  return merged;
}

async function getAllVisitsMap() {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${headerRangeEnd(header.length)}`);
  const map = {};
  rows.forEach((row, idx) => {
    const visit = parseVisitRow(row, idx, header);
    if (visit.visita_id) map[visit.visita_id] = visit;
  });
  return map;
}

async function getPromotorMap() {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  const map = {};
  rows.forEach((row) => {
    const promotorId = norm(row[1]);
    if (!promotorId) return;
    map[promotorId] = {
      external_id: norm(row[0]) || norm(row[7]),
      promotor_id: promotorId,
      nombre: norm(row[2]),
      region: norm(row[3]),
      cadena_principal: norm(row[4]),
      supervisor_external_id: norm(row[6]),
    };
  });
  return map;
}

function buildEvidenceView(item, marcaMap, visitMap, tiendaMap, promotorMap) {
  const visit = visitMap[item.visita_id];
  const tienda = visit ? tiendaMap[visit.tienda_id] : null;
  const promotor = visit ? promotorMap[visit.promotor_id] : null;
  return {
    ...item,
    marca_nombre: marcaMap[item.marca_id]?.marca_nombre || item.marca_id,
    fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
    tienda_id: visit?.tienda_id || "",
    tienda_nombre: tienda?.nombre_tienda || visit?.tienda_id || "",
    promotor_id: visit?.promotor_id || "",
    promotor_nombre: promotor?.nombre || "",
  };
}

function evidenceStatusForAnalysis(evidence, requiresReview) {
  const current = upper(evidence.status || "RECIBIDA");
  if (["APROBADA", "RECHAZADA"].includes(current)) return current;
  if (requiresReview) return "PENDIENTE_REVISION";
  return current || "RECIBIDA";
}

function computeEvidencePlusAnalysis(evidence, context = {}) {
  const visit = context.visit || null;
  const sameVisit = Array.isArray(context.sameVisit) ? context.sameVisit : [];
  const reglas = Array.isArray(context.reglas) ? context.reglas : [];
  const activeSameVisit = sameVisit.filter((item) => upper(item.status) !== "ANULADA");
  const activeSameGroup = activeSameVisit.filter((item) =>
    upper(item.marca_id) === upper(evidence.marca_id) &&
    evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(evidence.tipo_evidencia) &&
    upper(item.fase || "NA") === upper(evidence.fase || "NA")
  );
  const regla = reglas.find((rule) => evidenceTypeKey(rule.tipo_evidencia) === evidenceTypeKey(evidence.tipo_evidencia));
  const expectedPhotos = safeInt(regla?.fotos_requeridas, 1);
  const requiereFase = Boolean(regla?.requiere_antes_despues);
  const hash = buildEvidencePhotoHash(evidence.url_foto, evidence.foto_nombre);
  const imageMeta = getImageMeta(evidence.url_foto);

  let score = 1.0;
  const hallazgos = [];
  const rules = [];
  let forcedRisk = "";
  let alertType = "";
  let alertSeverity = "";

  if (!visit) {
    score -= 0.25;
    hallazgos.push("Evidencia sin visita válida.");
    rules.push("VISIT_NOT_FOUND");
    forcedRisk = "ALTO";
    alertType = "EVIDENCIA_RIESGO_ALTO";
    alertSeverity = "ALTA";
  }

  if (expectedPhotos > 0 && activeSameGroup.length < expectedPhotos) {
    score -= 0.25;
    hallazgos.push(`Faltan ${expectedPhotos - activeSameGroup.length} foto(s) requeridas para este tipo.`);
    rules.push("MISSING_REQUIRED_PHOTOS");
    if (!forcedRisk) forcedRisk = "MEDIO";
    if (!alertType) {
      alertType = "EVIDENCIA_INCOMPLETA";
      alertSeverity = "MEDIA";
    }
  }

  if (requiereFase) {
    const phase = upper(evidence.fase || "NA");
    if (phase === "NA") {
      score -= 0.15;
      hallazgos.push("La regla de evidencia requiere fase y se recibió como NA.");
      rules.push("PHASE_REQUIRED_BUT_NA");
      if (!forcedRisk) forcedRisk = "MEDIO";
    }
    if (phase === "DESPUES") {
      const hasAntes = activeSameVisit.some((item) =>
        item.evidencia_id !== evidence.evidencia_id &&
        upper(item.marca_id) === upper(evidence.marca_id) &&
        evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(evidence.tipo_evidencia) &&
        upper(item.fase) === "ANTES" &&
        upper(item.status) !== "ANULADA"
      );
      if (!hasAntes) {
        score -= 0.15;
        hallazgos.push("Se detectó fase DESPUES sin evidencia previa ANTES.");
        rules.push("PHASE_INCONSISTENT");
        if (!forcedRisk) forcedRisk = "MEDIO";
        if (!alertType) {
          alertType = "EVIDENCIA_FASE_INCONSISTENTE";
          alertSeverity = "MEDIA";
        }
      }
    }
  }

  const duplicateStrong = activeSameVisit.some((item) => item.evidencia_id !== evidence.evidencia_id && norm(item.hash_foto) && norm(item.hash_foto) === hash);
  if (duplicateStrong) {
    score -= 0.2;
    hallazgos.push("Posible duplicado fuerte de foto dentro de la misma visita.");
    rules.push("POSSIBLE_DUPLICATE");
    forcedRisk = "ALTO";
    alertType = "EVIDENCIA_POSIBLE_DUPLICADA";
    alertSeverity = "ALTA";
  }

  if (imageMeta.isDataUrl) {
    if ((imageMeta.width && imageMeta.width < EVPLUS_MIN_DIMENSION) || (imageMeta.height && imageMeta.height < EVPLUS_MIN_DIMENSION)) {
      score -= 0.15;
      hallazgos.push(`Resolución baja: ${imageMeta.width || 0}x${imageMeta.height || 0}.`);
      rules.push("LOW_RESOLUTION");
      if (!forcedRisk) forcedRisk = "MEDIO";
      if (!alertType) {
        alertType = "EVIDENCIA_CALIDAD_BAJA";
        alertSeverity = "MEDIA";
      }
    }
    if (imageMeta.estimated_bytes && imageMeta.estimated_bytes < EVPLUS_MIN_ESTIMATED_BYTES) {
      score -= 0.1;
      hallazgos.push("El archivo luce demasiado liviano para una revisión confiable.");
      rules.push("LOW_PAYLOAD_SIZE");
      if (!forcedRisk) forcedRisk = "MEDIO";
      if (!alertType) {
        alertType = "EVIDENCIA_CALIDAD_BAJA";
        alertSeverity = "MEDIA";
      }
    }
  } else {
    hallazgos.push("No fue posible leer metadatos técnicos de la imagen en V1.");
    rules.push("IMAGE_META_NOT_AVAILABLE");
  }

  if (rules.length >= 2 && !forcedRisk) forcedRisk = "MEDIO";

  score = Math.max(0.05, Number(score.toFixed(2)));
  let riesgo = "BAJO";
  if (score < 0.65) riesgo = "ALTO";
  else if (score < 0.85) riesgo = "MEDIO";
  if (forcedRisk === "ALTO") riesgo = "ALTO";
  else if (forcedRisk === "MEDIO" && riesgo === "BAJO") riesgo = "MEDIO";

  const requiresReview = riesgo !== "BAJO" || rules.includes("MISSING_REQUIRED_PHOTOS") || rules.includes("POSSIBLE_DUPLICATE");
  const resultadoAi = riesgo === "ALTO" ? "Evidencia con riesgo alto" : rules.length ? "Evidencia analizada con observaciones" : "Evidencia analizada";

  return {
    resultado_ai: resultadoAi,
    score_confianza: score,
    riesgo,
    hallazgos_ai: hallazgos.join(" | "),
    reglas_disparadas: rules.join(","),
    requiere_revision_supervisor: requiresReview ? "TRUE" : "FALSE",
    analizado_at: nowISO(),
    version_motor_ai: EVPLUS_VERSION,
    hash_foto: hash,
    status: evidenceStatusForAnalysis(evidence, requiresReview),
    shouldCreateAlert: riesgo === "ALTO" || rules.includes("MISSING_REQUIRED_PHOTOS") || rules.includes("POSSIBLE_DUPLICATE") || rules.includes("PHASE_INCONSISTENT"),
    alertPayload: {
      tipo_alerta: alertType || (riesgo === "ALTO" ? "EVIDENCIA_RIESGO_ALTO" : "EVIDENCIA_CALIDAD_BAJA"),
      severidad: alertSeverity || (riesgo === "ALTO" ? "ALTA" : "MEDIA"),
      descripcion: hallazgos.join(" | ") || "Evidencia con observaciones detectadas por Evidencia+ V1.",
    },
  };
}

async function analyzeEvidencePlusV1(evidence) {
  const visit = evidence.visita_id ? await getVisitById(evidence.visita_id) : null;
  const sameVisit = evidence.visita_id ? await getEvidenciasByVisitId(evidence.visita_id) : [];
  const reglas = evidence.marca_id ? await getReglasPorMarca(evidence.marca_id) : [];
  return computeEvidencePlusAnalysis(evidence, { visit, sameVisit, reglas });
}

async function applyEvidencePlusResult(evidenciaId, analysis) {
  const found = await getEvidenceById(evidenciaId);
  if (!found) return null;
  await updateEvidenceRow(found.header, found.evidence, {
    resultado_ai: analysis.resultado_ai,
    score_confianza: String(analysis.score_confianza),
    riesgo: analysis.riesgo,
    hallazgos_ai: analysis.hallazgos_ai,
    reglas_disparadas: analysis.reglas_disparadas,
    analizado_at: analysis.analizado_at,
    version_motor_ai: analysis.version_motor_ai,
    hash_foto: analysis.hash_foto,
    requiere_revision_supervisor: analysis.requiere_revision_supervisor,
    status: analysis.status,
  });
  return true;
}

async function maybeCreateEvidencePlusAlert(evidence, analysis, visit) {
  if (!analysis.shouldCreateAlert || !visit) return false;
  const alerts = await getAlertsAll();
  const alreadyOpen = alerts.some((item) => upper(item.status || "ABIERTA") === "ABIERTA" && item.evidencia_id === evidence.evidencia_id && upper(item.tipo_alerta) === upper(analysis.alertPayload.tipo_alerta));
  if (alreadyOpen) return false;
  const promotorMap = await getPromotorMap();
  const promotor = promotorMap[visit.promotor_id];
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: visit.promotor_id,
    visita_id: visit.visita_id,
    evidencia_id: evidence.evidencia_id,
    tipo_alerta: analysis.alertPayload.tipo_alerta,
    severidad: analysis.alertPayload.severidad,
    descripcion: `Evidencia+ V1: ${analysis.alertPayload.descripcion}`,
    status: "ABIERTA",
    supervisor_id: promotor?.supervisor_external_id || "",
    tienda_id: visit.tienda_id,
    canal_notificacion: "EVIDENCIA_PLUS",
  });
}

async function runEvidencePlusForEvidenceId(evidenciaId) {
  const found = await getEvidenceById(evidenciaId);
  if (!found) return null;
  const evidence = found.evidence;
  if (upper(evidence.tipo_evidencia) === "ASISTENCIA") return null;
  const analysis = await analyzeEvidencePlusV1(evidence);
  await applyEvidencePlusResult(evidence.evidencia_id, analysis);
  const visit = evidence.visita_id ? await getVisitById(evidence.visita_id) : null;
  await maybeCreateEvidencePlusAlert({ ...evidence, hash_foto: analysis.hash_foto }, analysis, visit);
  return analysis;
}

async function rerunEvidencePlusForGroup(visitaId, marcaId, tipoEvidencia, fase) {
  const evidences = await getEvidenciasByVisitId(visitaId);
  const visit = await getVisitById(visitaId);
  const reglas = marcaId ? await getReglasPorMarca(marcaId) : [];
  const header = await getEvidenciasHeader();
  const group = evidences.filter((item) =>
    upper(item.status) !== "ANULADA" &&
    upper(item.tipo_evidencia) !== "ASISTENCIA" &&
    upper(item.marca_id) === upper(marcaId) &&
    evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(tipoEvidencia) &&
    upper(item.fase || "NA") === upper(fase || "NA")
  );

  for (const item of group) {
    const analysis = computeEvidencePlusAnalysis(item, { visit, sameVisit: evidences, reglas });
    await updateEvidenceRow(header, item, {
      resultado_ai: analysis.resultado_ai,
      score_confianza: String(analysis.score_confianza),
      riesgo: analysis.riesgo,
      hallazgos_ai: analysis.hallazgos_ai,
      reglas_disparadas: analysis.reglas_disparadas,
      analizado_at: analysis.analizado_at,
      version_motor_ai: analysis.version_motor_ai,
      hash_foto: analysis.hash_foto,
      requiere_revision_supervisor: analysis.requiere_revision_supervisor,
      status: analysis.status,
    });
    await maybeCreateEvidencePlusAlert({ ...item, hash_foto: analysis.hash_foto }, analysis, visit);
  }
}

async function buildGeofenceContext(tiendaId, lat, lon, accuracy) {
  const tiendaMap = await getTiendaMap();
  const policy = await getPolicyValidation();
  const tienda = tiendaMap[tiendaId];
  const radio = safeNum(tienda?.radio_m, policy.radio_estandar || 100) || 100;
  const latT = safeNum(tienda?.lat, NaN);
  const lonT = safeNum(tienda?.lon, NaN);
  let distance = NaN;
  if (Number.isFinite(latT) && Number.isFinite(lonT) && Number.isFinite(lat) && Number.isFinite(lon)) {
    distance = haversineDistanceMeters(lat, lon, latT, lonT);
  }
  const classification = classifyGeofence(distance, radio, safeNum(accuracy, 0));
  return {
    tienda,
    resultado: classification.result,
    severidad: classification.severity,
    distancia_m: Number.isFinite(distance) ? distance : "",
    accuracy_m: safeNum(accuracy, 0),
    radio_tienda_m: radio,
    lat_tienda: Number.isFinite(latT) ? latT : "",
    lon_tienda: Number.isFinite(lonT) ? lonT : "",
  };
}

async function createGeofenceAlertIfNeeded(visitId, promotor, tiendaId, tipo, geofence) {
  if (upper(geofence.resultado) !== "FUERA_DE_GEOCERCA") return false;
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: promotor.promotor_id,
    visita_id: visitId,
    evidencia_id: "",
    tipo_alerta: tipo,
    severidad: "ALTA",
    descripcion: `Registro ${tipo === "GEOCERCA_ENTRADA" ? "de entrada" : "de salida"} fuera de geocerca. Distancia ${geofence.distancia_m || "N/D"}m.`,
    status: "ABIERTA",
    supervisor_id: promotor.supervisor_external_id || "",
    tienda_id: tiendaId,
    canal_notificacion: "MINIAPP",
  });
}

function getMiniAppUrl() {
  return MINIAPP_BASE_URL || PUBLIC_BASE_URL || "https://example.com";
}

async function telegramApi(method, payload) {
  if (!TELEGRAM_API) throw new Error("TELEGRAM_BOT_TOKEN no configurado");
  const res = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Telegram API ${method} failed: ${res.status} ${text}`);
  }
  return res.json();
}

async function sendTelegramText(chatId, text, reply_markup) {
  return telegramApi("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: true,
    reply_markup,
  });
}

async function answerCallbackQuery(callbackQueryId, text = "") {
  return telegramApi("answerCallbackQuery", { callback_query_id: callbackQueryId, text });
}

function buildPromotorKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Asistencia", callback_data: "PROMO:ASIS" }, { text: "Evidencias", callback_data: "PROMO:EVID" }],
      [{ text: "Mis evidencias", callback_data: "PROMO:MY_EVID" }, { text: "Resumen", callback_data: "PROMO:SUMMARY" }],
      [{ text: "Abrir Mini App", web_app: { url: getMiniAppUrl() } }],
    ],
  };
}

function buildSupervisorKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Equipo", callback_data: "SUP:TEAM" }, { text: "Alertas", callback_data: "SUP:ALERTS" }],
      [{ text: "Evidencias", callback_data: "SUP:EVID" }, { text: "Resumen", callback_data: "SUP:SUMMARY" }],
      [{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }],
    ],
  };
}

function buildClienteKeyboard() {
  return { inline_keyboard: [[{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }]] };
}

function parseTelegramUpdate(update) {
  const callback = update.callback_query || null;
  if (callback) {
    return {
      type: "callback",
      callbackQueryId: callback.id,
      chatId: callback.message?.chat?.id,
      fromId: callback.from?.id,
      text: norm(callback.data),
    };
  }
  const message = update.message || update.edited_message || null;
  if (!message) return { type: "unknown", chatId: null, fromId: null, text: "" };
  return {
    type: "message",
    chatId: message.chat?.id,
    fromId: message.from?.id,
    text: norm(message.text || message.caption || ""),
  };
}

async function buildPromotorMenu(actor) {
  return {
    text: `👋 *Promobolsillo+*\n\nHola, *${actor.profile.nombre}*.\n\nDesde aquí puedes abrir tu operación o ir directo a la Mini App.`,
    reply_markup: buildPromotorKeyboard(),
  };
}

async function buildSupervisorMenu(actor) {
  return {
    text: `👋 *Promobolsillo+*\n\nHola, *${actor.profile.nombre}* (Supervisor).\n\nAbre el panel o usa los accesos rápidos.`,
    reply_markup: buildSupervisorKeyboard(),
  };
}

async function buildClienteMenu() {
  return {
    text: "👋 *Promobolsillo+*\n\nTu acceso disponible es el panel web.",
    reply_markup: buildClienteKeyboard(),
  };
}

async function respondPromotorShortcut(actor, key) {
  if (key === "PROMO:ASIS") {
    const visits = await getVisitasToday(actor.profile.promotor_id);
    const open = visits.filter((v) => !v.hora_fin).length;
    return {
      text: `🕒 *Asistencia*\n\nVisitas hoy: *${visits.length}*\nAbiertas: *${open}*\n\nAbre la Mini App para registrar entrada/salida con geocerca, ubicación y foto.`,
      reply_markup: { inline_keyboard: [[{ text: "Abrir asistencia", web_app: { url: getMiniAppUrl() } }]] },
    };
  }
  if (key === "PROMO:EVID") {
    return {
      text: "📸 *Evidencias*\n\nCaptura fotos estructuradas por marca, tipo y fase desde la Mini App.",
      reply_markup: { inline_keyboard: [[{ text: "Abrir evidencias", web_app: { url: getMiniAppUrl() } }]] },
    };
  }
  if (key === "PROMO:MY_EVID") {
    const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
    return {
      text: `📚 *Mis evidencias de hoy*: *${evidencias.length}*\n\nRevisa filtros, notas y reemplazos desde la Mini App.`,
      reply_markup: { inline_keyboard: [[{ text: "Abrir galería", web_app: { url: getMiniAppUrl() } }]] },
    };
  }
  if (key === "PROMO:SUMMARY") {
    const visits = await getVisitasToday(actor.profile.promotor_id);
    const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
    const geofenceAlerts = visits.filter((v) => upper(v.resultado_geocerca_entrada) === "FUERA_DE_GEOCERCA" || upper(v.resultado_geocerca_salida) === "FUERA_DE_GEOCERCA").length;
    return {
      text: `📊 *Resumen del día*\n\n🏬 Visitas: *${visits.length}*\n📸 Evidencias: *${evidencias.length}*\n🚨 Con alerta geocerca: *${geofenceAlerts}*`,
      reply_markup: buildPromotorKeyboard(),
    };
  }
  return buildPromotorMenu(actor);
}

async function respondSupervisorShortcut(actor, key) {
  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const promotorIds = team.map((t) => t.promotor_id);
  if (key === "SUP:TEAM") {
    return {
      text: `👥 *Equipo*\n\nPromotores ligados: *${team.length}*\n\nAbre el panel para revisar visitas, evidencias y alertas por promotor.`,
      reply_markup: buildSupervisorKeyboard(),
    };
  }
  if (key === "SUP:ALERTS") {
    const alerts = (await getAlertsAll()).filter((a) => promotorIds.includes(a.promotor_id) && upper(a.status || "ABIERTA") === "ABIERTA");
    return {
      text: `🚨 *Alertas abiertas*: *${alerts.length}*\n\nAbre el panel para revisar y cerrar alertas.`,
      reply_markup: buildSupervisorKeyboard(),
    };
  }
  if (key === "SUP:EVID") {
    const allEvidences = await getEvidenciasAll();
    const visitMap = await getAllVisitsMap();
    const visible = allEvidences.filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA" && promotorIds.includes(visitMap[e.visita_id]?.promotor_id));
    return {
      text: `📸 *Evidencias operativas*: *${visible.length}*\n\nAbre el panel para revisar una o varias evidencias a la vez.`,
      reply_markup: buildSupervisorKeyboard(),
    };
  }
  if (key === "SUP:SUMMARY") {
    let visitasHoy = 0;
    let abiertas = 0;
    for (const teamMember of team) {
      const visits = await getVisitasToday(teamMember.promotor_id);
      visitasHoy += visits.length;
      abiertas += visits.filter((v) => !v.hora_fin).length;
    }
    return {
      text: `📊 *Resumen supervisor*\n\n👥 Promotores: *${team.length}*\n🏬 Visitas hoy: *${visitasHoy}*\n🟢 Abiertas: *${abiertas}*`,
      reply_markup: buildSupervisorKeyboard(),
    };
  }
  return buildSupervisorMenu(actor);
}


function parseClientDateFilters(filters = {}) {
  const start = norm(filters.fecha_inicio) || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, "0")}-01`;
  const end = norm(filters.fecha_fin) || todayISO();
  return { fecha_inicio: start, fecha_fin: end };
}

function inRangeYmd(isoLike, startYmd, endYmd) {
  if (!isoLike) return false;
  const ymd = /^\d{4}-\d{2}-\d{2}$/.test(String(isoLike)) ? String(isoLike) : ymdInTZ(new Date(isoLike), APP_TZ);
  return ymd >= startYmd && ymd <= endYmd;
}

function clientOk(res, data, meta = {}) {
  return res.json({ ok: true, data, meta, error: null });
}

function clientFail(res, status, message) {
  return res.status(status).json({ ok: false, data: null, meta: {}, error: message });
}

async function getClientScopeFromRequest(req) {
  const { validated } = await getActorFromRequest(req);
  const ctx = await getClienteContextByExternalId(validated.external_id);
  if (!ctx) throw new Error("Solo cliente");
  return ctx;
}

async function getClientBrandIds(clienteId) {
  const marcaMap = await getMarcaMap();
  return Object.values(marcaMap)
    .filter((item) => item.cliente_id === clienteId && item.activa !== false)
    .map((item) => item.marca_id);
}

async function getClientDataset(clienteId, filters = {}) {
  const { fecha_inicio, fecha_fin } = parseClientDateFilters(filters);
  const cadena = norm(filters.cadena);
  const region = norm(filters.region);
  const tiendaId = norm(filters.tienda_id);
  const marcaId = norm(filters.marca_id);

  const clientBrandIds = new Set(await getClientBrandIds(clienteId));
  const visitMap = await getAllVisitsMap();
  const marcaMap = await getMarcaMap();
  const tiendaMap = await getTiendaMap();
  const promotorMap = await getPromotorMap();

  let evidences = (await getEvidenciasAll()).filter((item) =>
    upper(item.tipo_evidencia) !== "ASISTENCIA" &&
    clientBrandIds.has(item.marca_id) &&
    inRangeYmd(item.fecha_hora, fecha_inicio, fecha_fin)
  );
  if (marcaId) evidences = evidences.filter((item) => item.marca_id === marcaId);

  let visits = Object.values(visitMap).filter((visit) =>
    evidences.some((e) => e.visita_id === visit.visita_id)
  );

  let stores = unique(visits.map((visit) => visit.tienda_id)).map((id) => tiendaMap[id]).filter(Boolean);
  if (cadena) stores = stores.filter((store) => norm(store.cadena) === cadena);
  if (region) stores = stores.filter((store) => norm(store.region) === region);
  if (tiendaId) stores = stores.filter((store) => norm(store.tienda_id) === tiendaId);
  const scopedStoreIds = new Set(stores.map((store) => store.tienda_id));
  visits = visits.filter((visit) => scopedStoreIds.has(visit.tienda_id));
  const scopedVisitIds = new Set(visits.map((visit) => visit.visita_id));
  evidences = evidences.filter((item) => scopedVisitIds.has(item.visita_id));
  const scopedEvidenceIds = new Set(evidences.map((item) => item.evidencia_id));

  const alerts = (await getAlertsAll()).filter((alert) =>
    inRangeYmd(alert.fecha_hora, fecha_inicio, fecha_fin) &&
    ((alert.evidencia_id && scopedEvidenceIds.has(alert.evidencia_id)) || (alert.visita_id && scopedVisitIds.has(alert.visita_id)))
  );

  return {
    fecha_inicio,
    fecha_fin,
    brandIds: Array.from(clientBrandIds),
    stores,
    visits,
    evidences,
    alerts,
    visitMap,
    promotorMap,
    marcaMap,
    tiendaMap,
  };
}

function paginateRows(rows, pagination = {}) {
  const page = Math.max(1, safeInt(pagination.page, 1));
  const pageSize = Math.max(1, safeInt(pagination.page_size, 25));
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const start = (page - 1) * pageSize;
  return {
    rows: rows.slice(start, start + pageSize),
    meta: { page, page_size: pageSize, total_rows: totalRows, total_pages: totalPages },
  };
}

app.post("/miniapp/cliente/bootstrap", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    return clientOk(res, {
      role: "cliente",
      cliente: ctx.cliente,
      access: ctx.access,
    });
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 401, error.message || "auth failed");
  }
});

app.post("/miniapp/cliente/filter-options", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const data = await getClientDataset(ctx.cliente.cliente_id, req.body || {});
    const marcas = Array.from(new Set(data.evidences.map((item) => item.marca_id).filter(Boolean))).map((id) => ({
      id,
      label: data.marcaMap[id]?.marca_nombre || id,
    }));
    const tipos = Array.from(new Set(data.evidences.map((item) => item.tipo_evidencia).filter(Boolean))).sort();
    return clientOk(res, {
      cadenas: Array.from(new Set(data.stores.map((item) => item.cadena).filter(Boolean))).map((x) => ({ id: x, label: x })),
      regiones: Array.from(new Set(data.stores.map((item) => item.region).filter(Boolean))).map((x) => ({ id: x, label: x })),
      tiendas: data.stores.map((item) => ({ id: item.tienda_id, label: item.nombre_tienda || item.tienda_id })),
      marcas,
      tipos_evidencia: tipos.map((x) => ({ id: x, label: x })),
      riesgos: ["BAJO", "MEDIO", "ALTO"].map((x) => ({ id: x, label: x })),
      decisiones: ["APROBADA", "OBSERVADA", "RECHAZADA"].map((x) => ({ id: x, label: x })),
      severidades: ["ALTA", "MEDIA", "BAJA"].map((x) => ({ id: x, label: x })),
      estatus_alerta: ["ABIERTA", "RESUELTA", "DESCARTADA"].map((x) => ({ id: x, label: x })),
    });
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente filter-options error");
  }
});

app.post("/miniapp/cliente/dashboard", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    const uniqueStores = new Set(data.visits.map((visit) => visit.tienda_id));
    const aprobadas = data.evidences.filter((item) => upper(item.decision_supervisor || item.status) === "APROBADA").length;
    const observadas = data.evidences.filter((item) => upper(item.decision_supervisor || item.status) === "OBSERVADA").length;
    const rechazadas = data.evidences.filter((item) => upper(item.decision_supervisor || item.status) === "RECHAZADA").length;
    const geocercaOk = data.visits.filter((visit) =>
      ["OK_EN_GEOCERCA", "OK_CON_TOLERANCIA_GPS"].includes(upper(visit.resultado_geocerca_entrada)) ||
      ["OK_EN_GEOCERCA", "OK_CON_TOLERANCIA_GPS"].includes(upper(visit.resultado_geocerca_salida))
    ).length;
    const cumplimiento = data.stores.length ? Number(((uniqueStores.size / data.stores.length) * 100).toFixed(2)) : 0;
    return clientOk(res, {
      period: { fecha_inicio: data.fecha_inicio, fecha_fin: data.fecha_fin, label: `${data.fecha_inicio} a ${data.fecha_fin}` },
      cliente: ctx.cliente,
      kpis: {
        tiendas_visibles: data.stores.length,
        tiendas_visitadas: uniqueStores.size,
        visitas: data.visits.length,
        cumplimiento_pct: cumplimiento,
        evidencias: data.evidences.length,
        aprobadas,
        observadas,
        rechazadas,
        alertas: data.alerts.length,
        geocerca_ok_pct: data.visits.length ? Number(((geocercaOk / data.visits.length) * 100).toFixed(2)) : 0,
      },
      top_alerts: Array.from(data.alerts.reduce((acc, item) => {
        acc.set(item.tipo_alerta, (acc.get(item.tipo_alerta) || 0) + 1);
        return acc;
      }, new Map()).entries()).map(([tipo_alerta, total]) => ({ tipo_alerta, total })).sort((a, b) => b.total - a.total).slice(0, 6),
    });
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente dashboard error");
  }
});

app.post("/miniapp/cliente/stores", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    const rows = data.stores.map((store) => {
      const visits = data.visits.filter((visit) => visit.tienda_id === store.tienda_id);
      const evidences = data.evidences.filter((item) => data.visitMap[item.visita_id]?.tienda_id === store.tienda_id);
      const alerts = data.alerts.filter((item) => data.visitMap[item.visita_id]?.tienda_id === store.tienda_id);
      const lastVisit = visits.slice().sort((a, b) => String(b.hora_inicio || b.fecha).localeCompare(String(a.hora_inicio || a.fecha)))[0] || null;
      return {
        tienda_id: store.tienda_id,
        tienda_nombre: store.nombre_tienda || store.tienda_id,
        cadena: store.cadena || "",
        region: store.region || "",
        ciudad: store.ciudad || "",
        visitas: visits.length,
        ultima_visita: lastVisit?.hora_inicio || lastVisit?.fecha || "",
        ultima_visita_fmt: lastVisit?.hora_inicio ? fmtDateTimeTZ(lastVisit.hora_inicio) : lastVisit?.fecha || "-",
        evidencias: evidences.length,
        aprobadas: evidences.filter((item) => upper(item.decision_supervisor || item.status) === "APROBADA").length,
        observadas: evidences.filter((item) => upper(item.decision_supervisor || item.status) === "OBSERVADA").length,
        alertas: alerts.length,
        estatus: alerts.length ? "CON_INCIDENCIAS" : visits.length ? "CON_VISITA" : "SIN_VISITA",
      };
    }).sort((a, b) => a.tienda_nombre.localeCompare(b.tienda_nombre));
    const paged = paginateRows(rows, req.body?.pagination || {});
    return clientOk(res, { rows: paged.rows }, paged.meta);
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente stores error");
  }
});

app.post("/miniapp/cliente/store-detail", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const tiendaId = norm(req.body?.tienda_id);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, { ...filters, tienda_id: tiendaId });
    const store = data.stores.find((item) => item.tienda_id === tiendaId);
    if (!store) return clientFail(res, 404, "Tienda no encontrada");
    const visits = data.visits.filter((visit) => visit.tienda_id === tiendaId);
    const evidences = data.evidences.filter((item) => data.visitMap[item.visita_id]?.tienda_id === tiendaId).map((item) => buildEvidenceView(item, data.marcaMap, data.visitMap, data.tiendaMap, data.promotorMap));
    const alerts = data.alerts.filter((item) => data.visitMap[item.visita_id]?.tienda_id === tiendaId).map((item) => ({
      ...item,
      fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
      promotor_nombre: data.promotorMap[item.promotor_id]?.nombre || item.promotor_id,
    }));
    return clientOk(res, {
      store,
      summary: {
        visitas: visits.length,
        evidencias: evidences.length,
        aprobadas: evidences.filter((item) => upper(item.decision_supervisor || item.status) === "APROBADA").length,
        observadas: evidences.filter((item) => upper(item.decision_supervisor || item.status) === "OBSERVADA").length,
        alertas: alerts.length,
      },
      visits,
      evidences: evidences.slice(0, 60),
      alerts,
    });
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente store-detail error");
  }
});

app.post("/miniapp/cliente/evidences", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    let rows = data.evidences.map((item) => buildEvidenceView(item, data.marcaMap, data.visitMap, data.tiendaMap, data.promotorMap));
    if (norm(filters.tienda_id)) rows = rows.filter((item) => item.tienda_id === norm(filters.tienda_id));
    if (norm(filters.tipo_evidencia)) rows = rows.filter((item) => evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(filters.tipo_evidencia));
    if (norm(filters.fase)) rows = rows.filter((item) => norm(item.fase) === norm(filters.fase));
    if (upper(filters.riesgo)) rows = rows.filter((item) => upper(item.riesgo) === upper(filters.riesgo));
    if (upper(filters.decision_supervisor)) rows = rows.filter((item) => upper(item.decision_supervisor || item.status) === upper(filters.decision_supervisor));
    else rows = rows.filter((item) => ["APROBADA", "OBSERVADA"].includes(upper(item.decision_supervisor || item.status)));
    rows.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const paged = paginateRows(rows, req.body?.pagination || { page_size: 40 });
    return clientOk(res, { rows: paged.rows }, paged.meta);
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente evidences error");
  }
});

app.post("/miniapp/cliente/incidents", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    let rows = data.alerts.map((item) => ({
      ...item,
      fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
      promotor_nombre: data.promotorMap[item.promotor_id]?.nombre || item.promotor_id,
      tienda_nombre: data.tiendaMap[item.tienda_id]?.nombre_tienda || item.tienda_id,
      cadena: data.tiendaMap[item.tienda_id]?.cadena || "",
      region: data.tiendaMap[item.tienda_id]?.region || "",
    }));
    if (norm(filters.tipo_alerta)) rows = rows.filter((item) => item.tipo_alerta === norm(filters.tipo_alerta));
    if (upper(filters.severidad)) rows = rows.filter((item) => upper(item.severidad) === upper(filters.severidad));
    if (upper(filters.status)) rows = rows.filter((item) => upper(item.status) === upper(filters.status));
    rows.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const paged = paginateRows(rows, req.body?.pagination || {});
    return clientOk(res, { rows: paged.rows }, paged.meta);
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente incidents error");
  }
});

app.post("/miniapp/cliente/deliverables", async (req, res) => {
  try {
    const ctx = await getClientScopeFromRequest(req);
    return clientOk(res, {
      enabled: false,
      message: `Hola ${ctx.cliente.cliente_nombre || "cliente"}. Los entregables automáticos estarán disponibles en la siguiente fase.`,
      rows: [],
    });
  } catch (error) {
    return clientFail(res, error.message === "Solo cliente" ? 403 : 500, error.message || "cliente deliverables error");
  }
});

app.get("/health", async (_req, res) => {
  res.json({ ok: true, service: "promobolsillo-telegram", now: nowISO() });
});

app.get("/", async (_req, res) => {
  res.json({ ok: true, service: "promobolsillo-telegram", miniapp: getMiniAppUrl() });
});

app.post("/telegram/webhook", async (req, res) => {
  try {
    const incoming = parseTelegramUpdate(req.body || {});
    console.log("TG_USER", {
      id: incoming.fromId,
      external_id: incoming.fromId ? `telegram:${incoming.fromId}` : "",
      text: incoming.text,
      type: incoming.type,
    });

    if (norm(incoming.text).toLowerCase() === "/whoami") {
      if (incoming.chatId && incoming.fromId) {
        await sendTelegramText(
          incoming.chatId,
          `telegram_id: ${incoming.fromId}
external_id: telegram:${incoming.fromId}`
        );
      }
      return res.json({ ok: true });
    }
    if (!incoming.chatId || !incoming.fromId) return res.json({ ok: true, ignored: true });
    const externalId = buildExternalIdFromTelegramUser(incoming.fromId);
    await upsertSessionData(externalId, { chat_id: String(incoming.chatId || ""), last_seen_at: nowISO() });
    const actor = await resolveActor(externalId);

    let payload = null;
    const text = upper(incoming.text || "");
    if (["/START", "/MENU", "MENU"].includes(text)) {
      if (actor.role === "supervisor") payload = await buildSupervisorMenu(actor);
      else if (actor.role === "promotor") payload = await buildPromotorMenu(actor);
      else payload = await buildClienteMenu();
    } else if (incoming.type === "callback") {
      if (actor.role === "supervisor") payload = await respondSupervisorShortcut(actor, incoming.text);
      else if (actor.role === "promotor") payload = await respondPromotorShortcut(actor, incoming.text);
      if (incoming.callbackQueryId) await answerCallbackQuery(incoming.callbackQueryId, "Hecho");
    } else {
      if (actor.role === "supervisor") payload = await buildSupervisorMenu(actor);
      else if (actor.role === "promotor") payload = await buildPromotorMenu(actor);
      else payload = await buildClienteMenu();
    }

    if (payload) await sendTelegramText(incoming.chatId, payload.text, payload.reply_markup);
    return res.json({ ok: true });
  } catch (error) {
    console.error("telegram webhook error", error);
    return res.status(500).json({ ok: false, error: error.message || "telegram webhook error" });
  }
});

app.post("/miniapp/bootstrap", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    return res.json({ ok: true, role: actor.role, profile: { nombre: actor.profile.nombre || actor.profile.external_id || "Usuario" } });
  } catch (error) {
    return res.status(401).json({ ok: false, error: error.message || "auth failed" });
  }
});

app.post("/miniapp/promotor/dashboard", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const tiendaMap = await getTiendaMap();
    const assignedIds = await getTiendasAsignadas(actor.profile.promotor_id);
    const stores = assignedIds.map((id) => ({
      tienda_id: id,
      nombre_tienda: tiendaMap[id]?.nombre_tienda || id,
      cadena: tiendaMap[id]?.cadena || "",
    }));
    const visits = await getVisitasToday(actor.profile.promotor_id);
    const visitsToday = visits.map((visit) => ({
      visita_id: visit.visita_id,
      tienda_id: visit.tienda_id,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      hora_inicio: visit.hora_inicio,
      hora_fin: visit.hora_fin,
      estado_visita: visit.estado_visita,
      resultado_geocerca_entrada: visit.resultado_geocerca_entrada,
      resultado_geocerca_salida: visit.resultado_geocerca_salida,
    }));
    const openVisits = visitsToday.filter((visit) => !visit.hora_fin);
    return res.json({
      ok: true,
      promotor: { nombre: actor.profile.nombre },
      stores,
      visitsToday,
      openVisits,
      summary: {
        assignedStores: stores.length,
        openVisits: openVisits.length,
        closedVisits: visitsToday.filter((item) => !!item.hora_fin).length,
      },
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "dashboard error" });
  }
});

app.post("/miniapp/promotor/evidences-today", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const rows = await getEvidenciasTodayByExternalId(actor.profile.external_id);
    const marcaMap = await getMarcaMap();
    const visitMap = await getAllVisitsMap();
    const tiendaMap = await getTiendaMap();
    const promotorMap = await getPromotorMap();
    const evidencias = rows.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap, promotorMap));
    return res.json({ ok: true, evidencias });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "evidences today error" });
  }
});

app.post("/miniapp/promotor/evidence-context", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const visitaId = norm(req.body?.visita_id);
    const visit = await getVisitById(visitaId);
    if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "Visita no encontrada" });
    const marcas = await getMarcasActivas();
    const tiendaMap = await getTiendaMap();
    return res.json({
      ok: true,
      visita: {
        visita_id: visit.visita_id,
        tienda_id: visit.tienda_id,
        tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      },
      marcas,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "evidence context error" });
  }
});

app.post("/miniapp/promotor/evidence-rules", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    let marcaId = norm(req.body?.marca_id);
    const marcaNombre = norm(req.body?.marca_nombre);
    if (!marcaId && marcaNombre) marcaId = await resolveMarcaIdByName(marcaNombre);

    const dedupeRules = (rules) => {
      const seen = new Set();
      const out = [];
      for (const rule of rules || []) {
        const tipo = canonicalEvidenceTypeLabel(rule?.tipo_evidencia);
        if (!tipo) continue;
        const key = evidenceTypeKey(tipo);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({ ...rule, tipo_evidencia: canonicalEvidenceTypeLabel(tipo) });
      }
      return out;
    };

    if (!marcaId) {
      return res.json({ ok: true, reglas: dedupeRules(await getTiposEvidenciaCatalog()) });
    }
    const reglas = dedupeRules(await getReglasPorMarca(marcaId));
    return res.json({ ok: true, reglas });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "evidence rules error" });
  }
});

app.post("/miniapp/promotor/start-entry", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const tiendaId = norm(req.body?.tienda_id);
    const lat = safeNum(req.body?.lat, NaN);
    const lon = safeNum(req.body?.lon, NaN);
    const accuracy = safeNum(req.body?.accuracy, 0);
    const fotoNombre = norm(req.body?.foto_nombre || "entrada.jpg");
    const fotoDataUrl = norm(req.body?.foto_data_url);
    if (!tiendaId) return res.status(400).json({ ok: false, error: "tienda_id requerido" });
    if (!fotoDataUrl) return res.status(400).json({ ok: false, error: "foto requerida" });

    const geofence = await buildGeofenceContext(tiendaId, lat, lon, accuracy);
    const visitId = await createVisitWithGeofence(actor.profile.promotor_id, tiendaId, "Registro de entrada", geofence);
    const evidenceId = `EV-${Date.now()}-ASIS-IN`;
    const attendanceResult = await registrarEvidencia({
      evidencia_id: evidenceId,
      external_id: actor.profile.external_id,
      fecha_hora: nowISO(),
      tipo_evento: "ASISTENCIA_ENTRADA",
      origen: "ASISTENCIA",
      jornada_id: "",
      visita_id: visitId,
      url_foto: fotoDataUrl,
      lat: String(lat),
      lon: String(lon),
      resultado_ai: "Entrada validada (demo)",
      score_confianza: geofence.resultado === "FUERA_DE_GEOCERCA" ? "0.72" : "0.93",
      riesgo: geofence.resultado === "FUERA_DE_GEOCERCA" ? "ALTO" : "BAJO",
      marca_id: "",
      producto_id: "",
      tipo_evidencia: "ASISTENCIA",
      descripcion: "[TELEGRAM_MINIAPP_ENTRADA]",
      status: "ACTIVA",
      note: "",
      fase: "NA",
      foto_nombre: fotoNombre,
      accuracy: String(accuracy || ""),
      hallazgos_ai: "",
      reglas_disparadas: "",
      analizado_at: "",
      version_motor_ai: "",
      hash_foto: buildEvidencePhotoHash(fotoDataUrl, fotoNombre),
    });
    if (upper(geofence.resultado) === "FUERA_DE_GEOCERCA") {
      await createGeofenceAlertIfNeeded(visitId, actor.profile, tiendaId, "GEOCERCA_ENTRADA", geofence);
    }
    return res.json({
      ok: true,
      visita_id: visitId,
      tienda_id: tiendaId,
      tienda_nombre: geofence.tienda?.nombre_tienda || tiendaId,
      started_at: nowISO(),
      warning: attendanceResult.photoOverflow ? "attendance_photo_too_large_for_sheets" : undefined,
    });
  } catch (error) {
    console.error("start-entry error", error);
    return res.status(500).json({ ok: false, error: error.message || "No se pudo registrar la entrada real." });
  }
});

app.post("/miniapp/promotor/close-visit", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const visitaId = norm(req.body?.visita_id);
    const lat = safeNum(req.body?.lat, NaN);
    const lon = safeNum(req.body?.lon, NaN);
    const accuracy = safeNum(req.body?.accuracy, 0);
    const fotoNombre = norm(req.body?.foto_nombre || "salida.jpg");
    const fotoDataUrl = norm(req.body?.foto_data_url);
    if (!visitaId) return res.status(400).json({ ok: false, error: "visita_id requerido" });
    if (!fotoDataUrl) return res.status(400).json({ ok: false, error: "foto requerida" });
    const visit = await getVisitById(visitaId);
    if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "Visita no encontrada" });

    const geofence = await buildGeofenceContext(visit.tienda_id, lat, lon, accuracy);
    await closeVisitWithGeofence(visit, "Registro de salida", geofence);
    const evidenceId = `EV-${Date.now()}-ASIS-OUT`;
    const attendanceResult = await registrarEvidencia({
      evidencia_id: evidenceId,
      external_id: actor.profile.external_id,
      fecha_hora: nowISO(),
      tipo_evento: "ASISTENCIA_SALIDA",
      origen: "ASISTENCIA",
      jornada_id: "",
      visita_id: visitaId,
      url_foto: fotoDataUrl,
      lat: String(lat),
      lon: String(lon),
      resultado_ai: "Salida validada (demo)",
      score_confianza: geofence.resultado === "FUERA_DE_GEOCERCA" ? "0.72" : "0.93",
      riesgo: geofence.resultado === "FUERA_DE_GEOCERCA" ? "ALTO" : "BAJO",
      marca_id: "",
      producto_id: "",
      tipo_evidencia: "ASISTENCIA",
      descripcion: "[TELEGRAM_MINIAPP_SALIDA]",
      status: "ACTIVA",
      note: "",
      fase: "NA",
      foto_nombre: fotoNombre,
      accuracy: String(accuracy || ""),
      hallazgos_ai: "",
      reglas_disparadas: "",
      analizado_at: "",
      version_motor_ai: "",
      hash_foto: buildEvidencePhotoHash(fotoDataUrl, fotoNombre),
    });
    if (upper(geofence.resultado) === "FUERA_DE_GEOCERCA") {
      await createGeofenceAlertIfNeeded(visitaId, actor.profile, visit.tienda_id, "GEOCERCA_SALIDA", geofence);
    }
    return res.json({
      ok: true,
      visita_id: visitaId,
      closed_at: nowISO(),
      warning: attendanceResult.photoOverflow ? "attendance_photo_too_large_for_sheets" : undefined,
    });
  } catch (error) {
    console.error("close-visit error", error);
    return res.status(500).json({ ok: false, error: error.message || "No se pudo registrar la salida real." });
  }
});

app.post("/miniapp/promotor/evidence-register", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const visitaId = norm(req.body?.visita_id);
    const marcaIdRaw = norm(req.body?.marca_id);
    const marcaNombreRaw = norm(req.body?.marca_nombre);
    const tipoEvidencia = canonicalEvidenceTypeLabel(req.body?.tipo_evidencia);
    const fase = norm(req.body?.fase || "NA") || "NA";
    const descripcion = norm(req.body?.descripcion);
    const fotos = Array.isArray(req.body?.fotos) ? req.body.fotos : [];
    const visit = await getVisitById(visitaId);
    if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "Visita no encontrada" });
    if (!tipoEvidencia) return res.status(400).json({ ok: false, error: "tipo_evidencia requerido" });
    if (!fotos.length) return res.status(400).json({ ok: false, error: "Debes enviar al menos una foto" });

    const marcaId = marcaIdRaw || (await resolveMarcaIdByName(marcaNombreRaw));
    const created = [];
    const batchPayloads = fotos.map((photo) => {
      const evidenceId = `EV-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      created.push(evidenceId);
      const photoName = fitCell(norm(photo?.name || "evidencia.jpg"));
      const photoValue = norm(photo?.dataUrl || photo?.url || "");
      return {
        evidencia_id: evidenceId,
        external_id: actor.profile.external_id,
        fecha_hora: nowISO(),
        tipo_evento: "EVIDENCIA_OPERATIVA",
        origen: "OPERACION",
        jornada_id: "",
        visita_id: visitaId,
        url_foto: photoValue,
        lat: "",
        lon: "",
        resultado_ai: "Pendiente",
        score_confianza: "0.90",
        riesgo: "BAJO",
        marca_id: marcaId,
        producto_id: "",
        tipo_evidencia: tipoEvidencia,
        descripcion,
        status: "RECIBIDA",
        note: "",
        fase,
        foto_nombre: photoName,
        accuracy: "",
        requiere_revision_supervisor: "FALSE",
        revisado_por: "",
        fecha_revision: "",
        decision_supervisor: "",
        motivo_revision: "",
        hallazgos_ai: "",
        reglas_disparadas: "",
        analizado_at: "",
        version_motor_ai: "",
        hash_foto: buildEvidencePhotoHash(photoValue, photoName),
      };
    });

    const batchResult = await registrarEvidenciasBatch(batchPayloads);

    await rerunEvidencePlusForGroup(visitaId, marcaId, tipoEvidencia, fase);

    return res.json({
      ok: true,
      visita_id: visitaId,
      created,
      count: created.length,
      warning: batchResult.photoOverflow ? "evidence_photo_too_large_for_sheets" : undefined,
      analysis_status: "scheduled",
    });
  } catch (error) {
    console.error("evidence-register error", error);
    return res.status(500).json({ ok: false, error: error.message || "No se pudo registrar la evidencia." });
  }
});

app.post("/miniapp/promotor/cancel-evidence", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const evidenciaId = norm(req.body?.evidencia_id);
    const note = norm(req.body?.note);
    const found = await getEvidenceById(evidenciaId);
    if (!found) return res.status(404).json({ ok: false, error: "Evidencia no encontrada" });
    if (found.evidence.external_id !== actor.profile.external_id) return res.status(403).json({ ok: false, error: "No autorizada" });
    await updateEvidenceRow(found.header, found.evidence, {
      status: "ANULADA",
      note: note ? `${found.evidence.note ? `${found.evidence.note} | ` : ""}${note}` : found.evidence.note,
    });
    if (upper(found.evidence.tipo_evidencia) !== "ASISTENCIA") {
      scheduleEvidenceGroupAnalysis({
        visitaId: found.evidence.visita_id,
        marcaId: found.evidence.marca_id,
        tipoEvidencia: found.evidence.tipo_evidencia,
        fase: found.evidence.fase || "NA",
      });
    }
    return res.json({ ok: true, evidencia_id: evidenciaId, status: "ANULADA" });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "No se pudo anular la evidencia." });
  }
});

app.post("/miniapp/promotor/replace-evidence", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const evidenciaId = norm(req.body?.evidencia_id);
    const fotoNombre = norm(req.body?.foto_nombre || "reemplazo.jpg");
    const fotoDataUrl = norm(req.body?.foto_data_url);
    const found = await getEvidenceById(evidenciaId);
    if (!found) return res.status(404).json({ ok: false, error: "Evidencia no encontrada" });
    if (found.evidence.external_id !== actor.profile.external_id) return res.status(403).json({ ok: false, error: "No autorizada" });
    const result = await updateEvidenceRow(found.header, found.evidence, {
      url_foto: fotoDataUrl,
      foto_nombre: fotoNombre,
      resultado_ai: "Pendiente",
      score_confianza: "",
      riesgo: "BAJO",
      hallazgos_ai: "",
      reglas_disparadas: "",
      analizado_at: "",
      version_motor_ai: "",
      hash_foto: buildEvidencePhotoHash(fotoDataUrl, fotoNombre),
      requiere_revision_supervisor: "FALSE",
    });
    if (upper(found.evidence.tipo_evidencia) !== "ASISTENCIA") {
      scheduleEvidenceGroupAnalysis({
        visitaId: found.evidence.visita_id,
        marcaId: found.evidence.marca_id,
        tipoEvidencia: found.evidence.tipo_evidencia,
        fase: found.evidence.fase || "NA",
      });
    }
    return res.json({ ok: true, evidencia_id: evidenciaId, replaced: true, warning: result.photoOverflow ? "evidence_photo_too_large_for_sheets" : undefined });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "No se pudo reemplazar la evidencia." });
  }
});

app.post("/miniapp/promotor/evidence-note", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "Solo promotor" });
    const evidenciaId = norm(req.body?.evidencia_id);
    const note = norm(req.body?.note);
    const found = await getEvidenceById(evidenciaId);
    if (!found) return res.status(404).json({ ok: false, error: "Evidencia no encontrada" });
    if (found.evidence.external_id !== actor.profile.external_id) return res.status(403).json({ ok: false, error: "No autorizada" });
    await updateEvidenceRow(found.header, found.evidence, { note });
    return res.json({ ok: true, evidencia_id: evidenciaId, note });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "No se pudo guardar la nota." });
  }
});

app.post("/miniapp/supervisor/dashboard", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = team.map((item) => item.promotor_id);
    let visitasHoy = 0;
    let abiertas = 0;
    for (const item of team) {
      const visits = await getVisitasToday(item.promotor_id);
      visitasHoy += visits.length;
      abiertas += visits.filter((v) => !v.hora_fin).length;
    }
    const allAlerts = await getAlertsAll();
    const alertas = allAlerts.filter((alert) => promotorIds.includes(alert.promotor_id) && upper(alert.status || "ABIERTA") === "ABIERTA").length;
    const allEvidences = await getEvidenciasAll();
    const visitMap = await getAllVisitsMap();
    const evidenciasHoy = allEvidences.filter((item) => {
      const visit = visitMap[item.visita_id];
      return visit && promotorIds.includes(visit.promotor_id) && ymdInTZ(new Date(item.fecha_hora), APP_TZ) === todayISO() && upper(item.tipo_evidencia) !== "ASISTENCIA";
    }).length;
    return res.json({
      ok: true,
      supervisor: { nombre: actor.profile.nombre },
      summary: { promotores: team.length, visitasHoy, abiertas, evidenciasHoy, alertas },
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "supervisor dashboard error" });
  }
});

app.post("/miniapp/supervisor/team", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const alerts = await getAlertsAll();
    const evidences = await getEvidenciasAll();
    const today = todayISO();
    const rows = [];
    for (const member of team) {
      const visits = await getVisitasToday(member.promotor_id);
      const openVisits = visits.filter((v) => !v.hora_fin);
      const latest = visits.slice().sort((a, b) => String(b.hora_inicio).localeCompare(String(a.hora_inicio)))[0] || null;
      const evidenciasHoy = evidences.filter((e) => e.visita_id && visits.some((v) => v.visita_id === e.visita_id) && ymdInTZ(new Date(e.fecha_hora), APP_TZ) === today && upper(e.tipo_evidencia) !== "ASISTENCIA").length;
      const alertasAbiertas = alerts.filter((a) => a.promotor_id === member.promotor_id && upper(a.status || "ABIERTA") === "ABIERTA").length;
      let statusGeneral = "SIN_MOVIMIENTO";
      if (alertasAbiertas > 0) statusGeneral = "ALERTA";
      else if (openVisits.length > 0) statusGeneral = "ACTIVO";
      else if (visits.length > 0) statusGeneral = "OPERANDO";
      rows.push({
        promotor_id: member.promotor_id,
        external_id: member.external_id,
        nombre: member.nombre,
        region: member.region,
        visitas_hoy: visits.length,
        visitas_abiertas: openVisits.length,
        evidencias_hoy: evidenciasHoy,
        alertas_abiertas: alertasAbiertas,
        ultima_tienda: latest?.tienda_id || "",
        ultima_entrada: latest?.hora_inicio || "",
        ultima_salida: latest?.hora_fin || "",
        ultima_visita_id: latest?.visita_id || "",
        status_general: statusGeneral,
      });
    }
    return res.json({ ok: true, team: rows });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "supervisor team error" });
  }
});

app.post("/miniapp/supervisor/alerts", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = new Set(team.map((item) => item.promotor_id));
    const promotorMap = await getPromotorMap();
    const tiendaMap = await getTiendaMap();
    const statusFilter = upper(req.body?.status);
    const severityFilter = upper(req.body?.severidad);
    const promotorFilter = norm(req.body?.promotor_id);
    let alerts = (await getAlertsAll()).filter((item) => promotorIds.has(item.promotor_id));
    if (statusFilter) alerts = alerts.filter((item) => upper(item.status) === statusFilter);
    if (severityFilter) alerts = alerts.filter((item) => upper(item.severidad) === severityFilter);
    if (promotorFilter) alerts = alerts.filter((item) => item.promotor_id === promotorFilter);
    alerts.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const visible = alerts.map((item) => ({
      ...item,
      promotor_nombre: promotorMap[item.promotor_id]?.nombre || item.promotor_id,
      fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
      tienda_nombre: tiendaMap[item.tienda_id]?.nombre_tienda || item.tienda_id,
    }));
    return res.json({ ok: true, alerts: visible });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "supervisor alerts error" });
  }
});

app.post("/miniapp/supervisor/alert-close", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const alertaId = norm(req.body?.alerta_id);
    const status = upper(req.body?.status || "RESUELTA");
    const comentario = norm(req.body?.comentario_cierre);
    const origen = norm(req.body?.origen_cierre || "SUPERVISOR");
    const found = await getAlertById(alertaId);
    if (!found) return res.status(404).json({ ok: false, error: "Alerta no encontrada" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = new Set(team.map((item) => item.promotor_id));
    if (!promotorIds.has(found.alert.promotor_id)) return res.status(403).json({ ok: false, error: "No autorizada" });
    await updateAlertRow(found.header, found.alert, {
      status,
      atendida_por: actor.profile.nombre || actor.profile.supervisor_id,
      fecha_atencion: nowISO(),
      comentario_cierre: comentario,
      origen_cierre: origen,
    });
    return res.json({ ok: true, alerta_id: alertaId, status });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "alert close error" });
  }
});

app.post("/miniapp/supervisor/evidences", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = new Set(team.map((item) => item.promotor_id));
    const marcaMap = await getMarcaMap();
    const visitMap = await getAllVisitsMap();
    const tiendaMap = await getTiendaMap();
    const promotorMap = await getPromotorMap();
    let evidences = (await getEvidenciasAll()).filter((item) => upper(item.tipo_evidencia) !== "ASISTENCIA" && promotorIds.has(visitMap[item.visita_id]?.promotor_id));
    const promotorFilter = norm(req.body?.promotor_id);
    const tiendaFilter = norm(req.body?.tienda_id);
    const marcaFilter = norm(req.body?.marca_id);
    const tipoFilter = norm(req.body?.tipo_evidencia);
    const riesgoFilter = upper(req.body?.riesgo);
    if (promotorFilter) evidences = evidences.filter((item) => visitMap[item.visita_id]?.promotor_id === promotorFilter);
    if (tiendaFilter) evidences = evidences.filter((item) => visitMap[item.visita_id]?.tienda_id === tiendaFilter || tiendaMap[visitMap[item.visita_id]?.tienda_id]?.nombre_tienda === tiendaFilter);
    if (marcaFilter) evidences = evidences.filter((item) => item.marca_id === marcaFilter || marcaMap[item.marca_id]?.marca_nombre === marcaFilter);
    if (tipoFilter) evidences = evidences.filter((item) => evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(tipoFilter));
    if (riesgoFilter) evidences = evidences.filter((item) => upper(item.riesgo) === riesgoFilter);
    evidences.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const visible = evidences.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap, promotorMap));
    return res.json({ ok: true, evidences: visible });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "supervisor evidences error" });
  }
});

app.post("/miniapp/supervisor/evidence-review", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const evidenciaId = norm(req.body?.evidencia_id);
    const decision = upper(req.body?.decision_supervisor);
    const motivo = norm(req.body?.motivo_revision);
    const found = await getEvidenceById(evidenciaId);
    if (!found) return res.status(404).json({ ok: false, error: "Evidencia no encontrada" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = new Set(team.map((item) => item.promotor_id));
    const visit = await getVisitById(found.evidence.visita_id);
    if (!visit || !promotorIds.has(visit.promotor_id)) return res.status(403).json({ ok: false, error: "No autorizada" });
    await updateEvidenceRow(found.header, found.evidence, {
      decision_supervisor: decision,
      motivo_revision: motivo,
      revisado_por: actor.profile.nombre || actor.profile.supervisor_id,
      fecha_revision: nowISO(),
      requiere_revision_supervisor: decision === "APROBADA" ? "FALSE" : "TRUE",
      status: decision || found.evidence.status,
    });
    return res.json({ ok: true, evidencia_id: evidenciaId, decision_supervisor: decision, status: decision });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "evidence review error" });
  }
});

app.post("/miniapp/supervisor/visit-expedient", async (req, res) => {
  try {
    const { actor } = await getActorFromRequest(req);
    if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "Solo supervisor" });
    const visitaId = norm(req.body?.visita_id);
    const visit = await getVisitById(visitaId);
    if (!visit) return res.status(404).json({ ok: false, error: "Visita no encontrada" });
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    const promotorIds = new Set(team.map((item) => item.promotor_id));
    if (!promotorIds.has(visit.promotor_id)) return res.status(403).json({ ok: false, error: "No autorizada" });
    const marcaMap = await getMarcaMap();
    const visitMap = await getAllVisitsMap();
    const tiendaMap = await getTiendaMap();
    const promotorMap = await getPromotorMap();
    const evidencias = (await getEvidenciasByVisitId(visitaId)).map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap, promotorMap));
    const alertas = (await getAlertsAll())
      .filter((item) => item.visita_id === visitaId)
      .map((item) => ({
        ...item,
        promotor_nombre: promotorMap[item.promotor_id]?.nombre || item.promotor_id,
        fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
        tienda_nombre: tiendaMap[item.tienda_id]?.nombre_tienda || item.tienda_id,
      }));
    const visitPayload = {
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      promotor_nombre: promotorMap[visit.promotor_id]?.nombre || visit.promotor_id,
    };
    return res.json({ ok: true, visita: visitPayload, evidencias, alertas });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "visit expedient error" });
  }
});

const port = safeInt(PORT, 10000);
app.listen(port, () => {
  console.log(`🚀 Promobolsillo+ Telegram escuchando en puerto ${port}`);
});
