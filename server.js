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
  return [norm(visitaId), norm(marcaId), evidenceTypeKey(tipoEvidencia), upper(fase || "NA")].join("::");
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
      console.warn("scheduleEvidenceGroupAnalysis error", { key, message: error?.message || error });
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
  const dataCheckString = [...params.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${k}=${v}`).join("\n");
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
  return { telegramUser: user, external_id: buildExternalIdFromTelegramUser(user.id), raw: Object.fromEntries(params.entries()) };
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
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ["https://www.googleapis.com/auth/spreadsheets"] });
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
  await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range, valueInputOption: "USER_ENTERED", requestBody: { values } });
}

async function updateSheetValues(range, values) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range, valueInputOption: "USER_ENTERED", requestBody: { values } });
}

async function getSheetHeader(sheetName) {
  const values = await getSheetValues(`${sheetName}!1:1`);
  return (values[0] || []).map((v) => norm(v));
}

async function getPolicyValidation() {
  try {
    const rows = await getSheetValues("POLITICA_VALIDACION!A2:D");
    const row = rows[0] || [];
    return { radio_estandar: safeNum(row[0], 100), sin_gps: upper(row[1] || "RECHAZAR"), tiempo_revision: safeNum(row[2], 30) };
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
      return { external_id: primary || alternateTelegram, promotor_id: promotorId, nombre, region, cadena_principal: cadena, activo, supervisor_external_id: supervisorExternal };
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
      return { external_id: primary || telegramAlt, supervisor_id: supervisorId, nombre, region, nivel, activo };
    }
  }
  return null;
}

async function getPromotoresDeSupervisor(supervisorExternalId) {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  const raw = rows.filter((row) => {
    const activo = row.length >= 6 ? isTrue(row[5]) : true;
    return activo && norm(row[6]) === supervisorExternalId;
  }).map((row) => ({ external_id: norm(row[0]) || norm(row[7]), promotor_id: norm(row[1]), nombre: norm(row[2]), region: norm(row[3]), cadena_principal: norm(row[4]), supervisor_external_id: norm(row[6]) }));
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
        return { access_id: norm(row[0]), cliente_id: norm(row[1]), external_id: norm(row[2]), nombre_contacto: norm(row[3]), correo: norm(row[4]), activo, rol_cliente: norm(row[6] || "LECTURA") };
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
        return { cliente_id: norm(row[0]), cliente_nombre: norm(row[1]), activo, logo_url: norm(row[3]), color_primario: norm(row[4]), correo_entregables: norm(row[5]), observaciones: norm(row[6]) };
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
    return { role: "cliente", profile: { external_id: externalId, nombre: clienteCtx.access.nombre_contacto || clienteCtx.cliente.cliente_nombre || "Cliente", cliente_id: clienteCtx.cliente.cliente_id, cliente_nombre: clienteCtx.cliente.cliente_nombre, rol_cliente: clienteCtx.access.rol_cliente } };
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
  const rows = await getSheetValues("TIENDAS!A2:K");
  const map = {};
  for (const row of rows) {
    const tienda_id = norm(row[0]);
    if (!tienda_id) continue;
    map[tienda_id] = { tienda_id, nombre_tienda: norm(row[1]), cadena: norm(row[2]), ciudad: norm(row[3]), region: norm(row[4]), activa: row.length >= 6 ? isTrue(row[5]) : true, direccion: norm(row[6]), lat: safeNum(row[7], NaN), lon: safeNum(row[8], NaN), radio_m: safeNum(row[9], 0), supervisor_id: norm(row[10]) };
  }
  return map;
}

async function getTiendasAsignadas(promotorId) {
  const rows = await getSheetValues("ASIGNACIONES!A2:E");
  return unique(rows.filter((row) => norm(row[0]) === promotorId && (row.length < 4 || isTrue(row[3] ?? "TRUE"))).map((row) => norm(row[1])));
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

async function sendTelegramText(chatId, text, options = {}) {
  return telegramApi("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: true,
    reply_markup: options.reply_markup,
  });
}

async function sendTelegramPhoto(chatId, photoValue, caption = "", options = {}) {
  if (!photoValue) return false;
  if (!TELEGRAM_API) return false;

  const isHttp = /^https?:\/\//i.test(photoValue);
  if (isHttp) {
    return telegramApi("sendPhoto", {
      chat_id: chatId,
      photo: photoValue,
      caption,
      parse_mode: "Markdown",
      reply_markup: options.reply_markup,
    });
  }

  const parsed = parseDataUrl(photoValue);
  if (!parsed) return false;

  const form = new FormData();
  form.append("chat_id", String(chatId));
  if (caption) form.append("caption", caption);
  form.append("parse_mode", "Markdown");
  if (options.reply_markup) form.append("reply_markup", JSON.stringify(options.reply_markup));

  const ext =
    parsed.mime === "image/png"
      ? "png"
      : parsed.mime === "image/webp"
      ? "webp"
      : "jpg";

  const blob = new Blob([parsed.buffer], { type: parsed.mime || "image/jpeg" });
  form.append("photo", blob, `alerta.${ext}`);

  const res = await fetch(`${TELEGRAM_API}/sendPhoto`, {
    method: "POST",
    body: form,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Telegram API sendPhoto failed: ${res.status} ${text}`);
  }

  return true;
}

async function notifySupervisorAlert(payload) {
  try {
    const supervisorExternalId = norm(payload.supervisor_id);
    if (!supervisorExternalId || !TELEGRAM_API) return false;

    const chatId = await getChatIdByExternalId(supervisorExternalId);
    if (!chatId) return false;

    const tiendaMap = await getTiendaMap();
    const promotorMap = await getPromotorMap();
    const marcaMap = await getMarcaMap();
    const tiendaNombre = tiendaMap[payload.tienda_id]?.nombre_tienda || payload.tienda_id || "Tienda";
    const promotorNombre = promotorMap[payload.promotor_id]?.nombre || payload.promotor_id || "Promotor";
    const evidenciaFound = payload.evidencia_id ? await getEvidenceById(payload.evidencia_id) : null;
    const evidencia = evidenciaFound?.evidence || null;
    const marcaNombre = evidencia?.marca_id ? (marcaMap[evidencia.marca_id]?.marca_nombre || evidencia.marca_id) : "";

    const text = [
      "🚨 *Nueva alerta*",
      "",
      `Tipo: *${payload.tipo_alerta || "ALERTA"}*`,
      `Severidad: *${payload.severidad || "MEDIA"}*`,
      `Promotor: *${promotorNombre}*`,
      `Tienda: *${tiendaNombre}*`,
      marcaNombre ? `Marca: *${marcaNombre}*` : "",
      evidencia?.tipo_evidencia ? `Evidencia: *${evidencia.tipo_evidencia}*` : "",
      evidencia?.fase && upper(evidencia.fase) !== "NA" ? `Fase: *${evidencia.fase}*` : "",
      payload.descripcion ? `Detalle: ${payload.descripcion}` : "",
    ].filter(Boolean).join("\n");

    const markup = {
      inline_keyboard: [[{ text: "Abrir panel", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
    };

    if (evidencia?.url_foto && evidencia.url_foto !== "[IMAGE_TOO_LARGE_FOR_SHEETS]") {
      try {
        await sendTelegramPhoto(chatId, evidencia.url_foto, text, { reply_markup: markup });
        return true;
      } catch (photoError) {
        console.warn("notifySupervisorAlert photo fallback", photoError?.message || photoError);
      }
    }

    await sendTelegramText(chatId, text, { reply_markup: markup });
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
  return rows.filter((row) => norm(row[0]) && (row.length < 3 || isTrue(row[2]))).map((row) => ({ marca_id: norm(row[0]), marca_nombre: norm(row[1]), cliente_id: norm(row[3]) })).sort((a, b) => a.marca_nombre.localeCompare(b.marca_nombre));
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

async function resolveMarcaIdByName(name) {
  const target = normalizeTextKey(name);
  if (!target) return "";
  const marcas = await getMarcasActivas();
  return marcas.find((m) => normalizeTextKey(m.marca_nombre) === target)?.marca_id || "";
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

async function getAsignacionesActivas() {
  const rows = await getSheetValues("ASIGNACIONES!A2:D");
  return rows
    .filter((row) => norm(row[0]) && norm(row[1]) && isTrue(row[3] ?? "TRUE"))
    .map((row) => ({
      promotor_id: norm(row[0]),
      tienda_id: norm(row[1]),
      frecuencia: upper(row[2]),
      activa: true,
    }));
}

async function getPlaneacionHeader() {
  return getSheetHeader("PLANEACION_VISITAS");
}

function parsePlaneacionRow(row, idx, header) {
  const map = headerIndexMap(header);
  return {
    rowIndex: idx + 2,
    plan_id: norm(row[map.plan_id]),
    fecha: norm(row[map.fecha]),
    promotor_id: norm(row[map.promotor_id]),
    tienda_id: norm(row[map.tienda_id]),
    estatus: upper(row[map.estatus] || "PLANEADA"),
    origen: norm(row[map.origen]),
    override_note: norm(row[map.override_note]),
    updated_at: norm(row[map.updated_at]),
  };
}

function buildPlaneacionRowFromHeader(header, payload) {
  const row = new Array(header.length).fill("");
  header.forEach((name, idx) => {
    const key = norm(name);
    row[idx] = payload[key] != null ? payload[key] : "";
  });
  return row;
}

async function getPlaneacionRowsAll() {
  const header = await getPlaneacionHeader();
  const rows = await getSheetValues(`PLANEACION_VISITAS!A2:${headerRangeEnd(header.length)}`);
  return { header, rows: rows.map((row, idx) => parsePlaneacionRow(row, idx, header)) };
}

async function getPlaneacionByDate(dateYmd) {
  const { rows } = await getPlaneacionRowsAll();
  return rows.filter((item) => item.fecha === dateYmd);
}

async function findPlaneacionForVisit(fecha, promotorId, tiendaId) {
  const rows = await getPlaneacionByDate(fecha);
  return rows.find(
    (item) =>
      item.promotor_id === promotorId &&
      item.tienda_id === tiendaId &&
      ["PLANEADA", "REPROGRAMADA"].includes(item.estatus)
  ) || null;
}

async function updatePlaneacionRow(header, planRow, patch = {}) {
  const row = buildPlaneacionRowFromHeader(header, { ...planRow, ...patch });
  await updateSheetValues(
    `PLANEACION_VISITAS!A${planRow.rowIndex}:${headerRangeEnd(header.length)}${planRow.rowIndex}`,
    [row]
  );
}

const SPANISH_WEEKDAY_TO_INDEX = {
  LUNES: 0,
  MARTES: 1,
  MIERCOLES: 2,
  "MIÉRCOLES": 2,
  JUEVES: 3,
  VIERNES: 4,
  SABADO: 5,
  "SÁBADO": 5,
  DOMINGO: 6,
};

async function generatePlaneacionForRange(startDateYmd, endDateYmd) {
  const asignaciones = await getAsignacionesActivas();
  const start = new Date(`${startDateYmd}T00:00:00`);
  const end = new Date(`${endDateYmd}T00:00:00`);
  const rows = [];

  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const ymd = d.toISOString().slice(0, 10);
    const weekday = (d.getUTCDay() + 6) % 7;
    for (const item of asignaciones) {
      if (SPANISH_WEEKDAY_TO_INDEX[item.frecuencia] !== weekday) continue;
      rows.push({
        plan_id: `PLAN-${ymd.replace(/-/g, "")}-${item.promotor_id}-${item.tienda_id}`,
        fecha: ymd,
        promotor_id: item.promotor_id,
        tienda_id: item.tienda_id,
        estatus: "PLANEADA",
        origen: "ASIGNACION",
        override_note: "",
        updated_at: nowISO(),
      });
    }
  }
  return rows;
}

async function upsertPlaneacionRows(payloadRows = []) {
  if (!payloadRows.length) return { inserted: 0, updated: 0 };
  const { header, rows } = await getPlaneacionRowsAll();
  const existingById = new Map(rows.map((r) => [r.plan_id, r]));
  const inserts = [];
  let updated = 0;

  for (const item of payloadRows) {
    const existing = existingById.get(item.plan_id);
    if (existing) {
      await updatePlaneacionRow(header, existing, item);
      updated += 1;
    } else {
      inserts.push(buildPlaneacionRowFromHeader(header, item));
    }
  }

  if (inserts.length) {
    await appendSheetValues(`PLANEACION_VISITAS!A2:${headerRangeEnd(header.length)}`, inserts);
  }

  return { inserted: inserts.length, updated };
}

async function getTiendaMarcasActivasByTiendaId(tiendaId) {
  const rows = await getSheetValues("TIENDA_MARCAS!A2:D");
  return rows
    .filter((row) => norm(row[0]) === tiendaId && isTrue(row[2] ?? "TRUE"))
    .map((row) => ({
      tienda_id: norm(row[0]),
      marca_id: norm(row[1]),
      activa: true,
      observaciones: norm(row[3]),
    }));
}

async function getTareasVisitaHeader() {
  return getSheetHeader("TAREAS_VISITA");
}

function parseTareaVisitaRow(row, idx, header) {
  const map = headerIndexMap(header);
  return {
    rowIndex: idx + 2,
    tarea_id: norm(row[map.tarea_id]),
    visita_id: norm(row[map.visita_id]),
    plan_id: norm(row[map.plan_id]),
    fecha: norm(row[map.fecha]),
    promotor_id: norm(row[map.promotor_id]),
    tienda_id: norm(row[map.tienda_id]),
    marca_id: norm(row[map.marca_id]),
    tipo_evidencia: canonicalEvidenceTypeLabel(row[map.tipo_evidencia]),
    fotos_requeridas: safeInt(row[map.fotos_requeridas], 0),
    requiere_antes_despues: isTrue(row[map.requiere_antes_despues]),
    estatus: upper(row[map.estatus] || "PENDIENTE"),
    total_fotos_cargadas: safeInt(row[map.total_fotos_cargadas], 0),
    alerta_generada: upper(row[map.alerta_generada] || "FALSE"),
    updated_at: norm(row[map.updated_at]),
  };
}

function buildTareaVisitaRowFromHeader(header, payload) {
  const row = new Array(header.length).fill("");
  header.forEach((name, idx) => {
    const key = norm(name);
    row[idx] = payload[key] != null ? payload[key] : "";
  });
  return row;
}

async function getTareasRowsAll() {
  const header = await getTareasVisitaHeader();
  const rows = await getSheetValues(`TAREAS_VISITA!A2:${headerRangeEnd(header.length)}`);
  return { header, rows: rows.map((row, idx) => parseTareaVisitaRow(row, idx, header)) };
}

async function getTareasByVisitaId(visitaId) {
  const { rows } = await getTareasRowsAll();
  return rows.filter((item) => item.visita_id === visitaId);
}

async function updateTareaVisitaRow(header, tareaRow, patch = {}) {
  const row = buildTareaVisitaRowFromHeader(header, { ...tareaRow, ...patch });
  await updateSheetValues(
    `TAREAS_VISITA!A${tareaRow.rowIndex}:${headerRangeEnd(header.length)}${tareaRow.rowIndex}`,
    [row]
  );
}

async function createTareasForVisita({ visitaId, planId, fecha, promotorId, tiendaId }) {
  const current = await getTareasByVisitaId(visitaId);
  if (current.length) return current;

  const tiendaMarcas = await getTiendaMarcasActivasByTiendaId(tiendaId);
  const header = await getTareasVisitaHeader();
  const rows = [];
  const created = [];

  for (const tm of tiendaMarcas) {
    const reglas = await getReglasPorMarca(tm.marca_id);
    for (const regla of reglas) {
      const payload = {
        tarea_id: `TV-${Date.now()}-${tm.marca_id}-${evidenceTypeKey(regla.tipo_evidencia).replace(/[^A-Z0-9]+/g, "").slice(0, 24)}`,
        visita_id: visitaId,
        plan_id: planId || "",
        fecha,
        promotor_id: promotorId,
        tienda_id: tiendaId,
        marca_id: tm.marca_id,
        tipo_evidencia: canonicalEvidenceTypeLabel(regla.tipo_evidencia),
        fotos_requeridas: safeInt(regla.fotos_requeridas, 1),
        requiere_antes_despues: regla.requiere_antes_despues ? "TRUE" : "FALSE",
        estatus: "PENDIENTE",
        total_fotos_cargadas: 0,
        alerta_generada: "FALSE",
        updated_at: nowISO(),
      };
      rows.push(buildTareaVisitaRowFromHeader(header, payload));
      created.push(payload);
    }
  }

  if (rows.length) {
    await appendSheetValues(`TAREAS_VISITA!A2:${headerRangeEnd(header.length)}`, rows);
  }

  return created;
}

function getValidEvidenceRowsForTask(evidences, task) {
  return evidences.filter(
    (item) =>
      item.visita_id === task.visita_id &&
      item.marca_id === task.marca_id &&
      evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(task.tipo_evidencia) &&
      upper(item.status) !== "ANULADA"
  );
}

function countAntesDespuesForTask(evidences, task) {
  const rows = getValidEvidenceRowsForTask(evidences, task);
  return {
    total: rows.length,
    antes: rows.filter((r) => upper(r.fase) === "ANTES").length,
    despues: rows.filter((r) => upper(r.fase) === "DESPUES").length,
  };
}

function resolveTaskStatus(task, counters) {
  if (!counters.total) return { estatus: "PENDIENTE", total_fotos_cargadas: 0 };

  const required = safeInt(task.fotos_requeridas, 1);

  if (task.requiere_antes_despues) {
    if (counters.antes >= 1 && counters.despues >= 1 && counters.total >= required) {
      return { estatus: "CUMPLIDA", total_fotos_cargadas: counters.total };
    }
    return { estatus: "INCOMPLETA", total_fotos_cargadas: counters.total };
  }

  if (counters.total >= required) {
    return { estatus: "CUMPLIDA", total_fotos_cargadas: counters.total };
  }

  return { estatus: "INCOMPLETA", total_fotos_cargadas: counters.total };
}

async function recalculateTareasForVisita(visitaId) {
  const tareasPack = await getTareasRowsAll();
  const tareas = tareasPack.rows.filter((item) => item.visita_id === visitaId);
  if (!tareas.length) return [];

  const evidences = await getEvidenciasByVisitId(visitaId);
  const updated = [];

  for (const task of tareas) {
    const counters = countAntesDespuesForTask(evidences, task);
    const resolved = resolveTaskStatus(task, counters);
    await updateTareaVisitaRow(tareasPack.header, task, {
      ...resolved,
      updated_at: nowISO(),
    });
    updated.push({ ...task, ...resolved });
  }

  return updated;
}

function isVisitValidForPlan(visit) {
  return ["OK_EN_GEOCERCA", "OK_CON_TOLERANCIA_GPS"].includes(
    upper(visit?.resultado_geocerca_entrada)
  );
}

async function getSupervisorExternalIdByPromotorId(promotorId) {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  const row = rows.find((r) => norm(r[1]) === promotorId);
  return row ? norm(row[6]) : "";
}

async function createNoVisitaAlert(planRow) {
  const supervisor_id = await getSupervisorExternalIdByPromotorId(planRow.promotor_id);
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: planRow.promotor_id,
    visita_id: "",
    evidencia_id: "",
    tipo_alerta: "NO_VISITA_TIENDA",
    severidad: "ALTA",
    descripcion: `No se registró entrada válida para ${planRow.tienda_id} en la fecha ${planRow.fecha}`,
    status: "ABIERTA",
    supervisor_id,
    tienda_id: planRow.tienda_id,
    canal_notificacion: "PLANEACION",
  });
}

async function createIncumplimientoEvidenciaAlert(taskRow) {
  const supervisor_id = await getSupervisorExternalIdByPromotorId(taskRow.promotor_id);
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: taskRow.promotor_id,
    visita_id: taskRow.visita_id,
    evidencia_id: "",
    tipo_alerta: "INCUMPLIMIENTO_EVIDENCIA",
    severidad: "MEDIA",
    descripcion: `Incumplimiento en ${taskRow.marca_id} / ${taskRow.tipo_evidencia}. Requeridas: ${taskRow.fotos_requeridas}, cargadas: ${taskRow.total_fotos_cargadas}`,
    status: "ABIERTA",
    supervisor_id,
    tienda_id: taskRow.tienda_id,
    canal_notificacion: "TAREAS_VISITA",
  });
}

async function runDailyComplianceClose(dateYmd) {
  const planeacionPack = await getPlaneacionRowsAll();
  const planeacion = planeacionPack.rows.filter((item) => item.fecha === dateYmd);
  const visitMap = await getAllVisitsMap();
  const allVisits = Object.values(visitMap).filter((v) => v.fecha === dateYmd);

  let noVisitAlerts = 0;
  for (const plan of planeacion) {
    if (!["PLANEADA", "REPROGRAMADA"].includes(plan.estatus)) continue;
    const validVisit = allVisits.find(
      (v) =>
        v.promotor_id === plan.promotor_id &&
        v.tienda_id === plan.tienda_id &&
        isVisitValidForPlan(v)
    );
    if (!validVisit) {
      await updatePlaneacionRow(planeacionPack.header, plan, {
        estatus: "NO_VISITADA",
        updated_at: nowISO(),
      });
      await createNoVisitaAlert(plan);
      noVisitAlerts += 1;
    }
  }

  const tareasPack = await getTareasRowsAll();
  let incumplimientoAlerts = 0;
  for (const tarea of tareasPack.rows.filter((item) => item.fecha === dateYmd)) {
    if (["CUMPLIDA", "INCUMPLIDA"].includes(tarea.estatus)) continue;
    await updateTareaVisitaRow(tareasPack.header, tarea, {
      estatus: "INCUMPLIDA",
      updated_at: nowISO(),
    });
    if (upper(tarea.alerta_generada) !== "TRUE") {
      await createIncumplimientoEvidenciaAlert(tarea);
      await updateTareaVisitaRow(tareasPack.header, tarea, {
        alerta_generada: "TRUE",
        updated_at: nowISO(),
        estatus: "INCUMPLIDA",
      });
      incumplimientoAlerts += 1;
    }
  }

  return { noVisitAlerts, incumplimientoAlerts };
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

function buildEvidenceView(evidence, marcaMap, visitMap, tiendaMap, promotorMap) {
  const visit = visitMap[evidence.visita_id] || {};
  const tienda = tiendaMap[visit.tienda_id] || {};
  const promotor = promotorMap[visit.promotor_id] || {};
  return {
    evidencia_id: evidence.evidencia_id,
    visita_id: evidence.visita_id,
    fecha_hora: evidence.fecha_hora,
    fecha_hora_fmt: fmtDateTimeTZ(evidence.fecha_hora),
    url_foto: evidence.url_foto,
    tienda_id: visit.tienda_id || "",
    tienda_nombre: tienda.nombre_tienda || visit.tienda_id || "",
    cadena: tienda.cadena || "",
    region: tienda.region || "",
    marca_id: evidence.marca_id,
    marca_nombre: marcaMap[evidence.marca_id]?.marca_nombre || evidence.marca_id,
    tipo_evidencia: evidence.tipo_evidencia,
    fase: evidence.fase,
    descripcion: evidence.descripcion,
    decision_supervisor: evidence.decision_supervisor,
    status: evidence.status,
    riesgo: evidence.riesgo,
    resultado_ai: evidence.resultado_ai,
    hallazgos_ai: evidence.hallazgos_ai,
    promotor_nombre: promotor.nombre || visit.promotor_id || "",
  };
}

function getMiniAppUrl() {
  return (MINIAPP_BASE_URL || PUBLIC_BASE_URL || "").replace(/\/+$/, "");
}

async function telegramApi(method, payload) {
  if (!TELEGRAM_API) throw new Error("TELEGRAM_API not configured");
  const res = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.ok === false) {
    throw new Error(`telegram ${method} failed: ${res.status} ${JSON.stringify(json)}`);
  }
  return json.result;
}

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function pickPhotoUrl(photo) {
  if (!photo) return "";
  return norm(photo.dataUrl || photo.url || photo.photo || "");
}

function pickPhotoName(photo, fallback = "evidencia.jpg") {
  return fitCell(norm(photo?.name || fallback));
}

async function buildGeofenceResult(tiendaId, lat, lon, accuracy) {
  const tiendaMap = await getTiendaMap();
  const tienda = tiendaMap[tiendaId];
  const policy = await getPolicyValidation();
  const latNum = safeNum(lat, NaN);
  const lonNum = safeNum(lon, NaN);
  const accNum = safeNum(accuracy, 0);

  if (!tienda || !Number.isFinite(tienda.lat) || !Number.isFinite(tienda.lon)) {
    return {
      resultado: "SIN_TIENDA_GPS",
      distancia_m: "",
      accuracy_m: accNum || "",
      radio_tienda_m: tienda?.radio_m || policy.radio_estandar,
      lat_tienda: tienda?.lat || "",
      lon_tienda: tienda?.lon || "",
      severidad: "MEDIA",
    };
  }

  if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) {
    return {
      resultado: policy.sin_gps === "PERMITIR" ? "OK_SIN_GPS" : "SIN_DATOS_GPS",
      distancia_m: "",
      accuracy_m: accNum || "",
      radio_tienda_m: tienda.radio_m || policy.radio_estandar,
      lat_tienda: tienda.lat,
      lon_tienda: tienda.lon,
      severidad: "MEDIA",
    };
  }

  const distance = haversineDistanceMeters(latNum, lonNum, tienda.lat, tienda.lon);
  const classified = classifyGeofence(distance, tienda.radio_m || policy.radio_estandar, accNum);
  return {
    resultado: classified.result,
    distancia_m: distance,
    accuracy_m: accNum,
    radio_tienda_m: tienda.radio_m || policy.radio_estandar,
    lat_tienda: tienda.lat,
    lon_tienda: tienda.lon,
    severidad: classified.severity,
  };
}

function buildSupervisorKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }],
    ],
  };
}

function buildPromotorKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }],
    ],
  };
}

function buildClienteKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }],
    ],
  };
}

async function getPromotorHomeSummary(promotor) {
  const visitsToday = await getVisitasToday(promotor.promotor_id);
  const openVisits = visitsToday.filter((v) => !v.hora_fin);
  const evidencesToday = await getEvidenciasTodayByExternalId(promotor.external_id);
  return {
    text: [
      `👋 *${promotor.nombre || "Promotor"}*`,
      "",
      `Visitas hoy: *${visitsToday.length}*`,
      `Abiertas: *${openVisits.length}*`,
      `Evidencias hoy: *${evidencesToday.filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA").length}*`,
      "",
      "Usa el panel para registrar entrada, salida y evidencias.",
    ].join("\n"),
    reply_markup: buildPromotorKeyboard(),
  };
}

async function getSupervisorHomeSummary(supervisor) {
  const team = await getPromotoresDeSupervisor(supervisor.external_id);
  const promotorIds = team.map((p) => p.promotor_id);
  const visitMap = await getAllVisitsMap();
  const allVisits = Object.values(visitMap).filter((v) => v.fecha === todayISO() && promotorIds.includes(v.promotor_id));
  const abiertas = allVisits.filter((v) => !v.hora_fin);
  const alerts = (await getAlertsAll()).filter((a) => promotorIds.includes(a.promotor_id) && upper(a.status) === "ABIERTA");
  const allEvidences = await getEvidenciasAll();
  const visible = allEvidences.filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA" && promotorIds.includes(visitMap[e.visita_id]?.promotor_id));
  return {
    text: [
      `🧭 *${supervisor.nombre || "Supervisor"}*`,
      "",
      `Equipo: *${team.length}*`,
      `Visitas hoy: *${allVisits.length}*`,
      `Abiertas: *${abiertas.length}*`,
      `Alertas abiertas: *${alerts.length}*`,
      `Evidencias operativas: *${visible.length}*`,
      "",
      "Abre el panel para revisar equipo, alertas y evidencias.",
    ].join("\n"),
    reply_markup: buildSupervisorKeyboard(),
  };
}

async function getClienteHomeSummary(clienteProfile) {
  const ctx = await getClienteContextByExternalId(clienteProfile.external_id);
  const clienteId = ctx?.cliente?.cliente_id || clienteProfile.cliente_id || "";
  const marcaMap = await getMarcaMap();
  const clienteMarcaIds = Object.values(marcaMap).filter((m) => m.cliente_id === clienteId).map((m) => m.marca_id);
  const allEvidences = await getEvidenciasAll();
  const evidences = allEvidences.filter((e) => clienteMarcaIds.includes(e.marca_id));
  const aprobadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "APROBADA").length;
  const observadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "OBSERVADA").length;
  const rechazadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "RECHAZADA").length;
  return {
    text: [
      `📊 *${ctx?.cliente?.cliente_nombre || clienteProfile.cliente_nombre || "Cliente"}*`,
      "",
      `Marcas visibles: *${clienteMarcaIds.length}*`,
      `Evidencias: *${evidences.length}*`,
      `Aprobadas: *${aprobadas}*`,
      `Observadas: *${observadas}*`,
      `Rechazadas: *${rechazadas}*`,
      "",
      "Abre el panel para ver resumen, tiendas, evidencias e incidencias.",
    ].join("\n"),
    reply_markup: buildClienteKeyboard(),
  };
}

async function getDynamicMenuText(actor) {
  if (actor.role === "promotor") return getPromotorHomeSummary(actor.profile);
  if (actor.role === "supervisor") return getSupervisorHomeSummary(actor.profile);
  return getClienteHomeSummary(actor.profile);
}
  return { header, rows: rows.map((row, idx) => parseTareaVisitaRow(row, idx, header)) };
}

async function getTareasByVisitaId(visitaId) {
  const { rows } = await getTareasRowsAll();
  return rows.filter((item) => item.visita_id === visitaId);
}

async function updateTareaVisitaRow(header, tareaRow, patch = {}) {
  const row = buildTareaVisitaRowFromHeader(header, { ...tareaRow, ...patch });
  await updateSheetValues(`TAREAS_VISITA!A${tareaRow.rowIndex}:${headerRangeEnd(header.length)}${tareaRow.rowIndex}`, [row]);
}

async function createTareasForVisita({ visitaId, planId, fecha, promotorId, tiendaId }) {
  const current = await getTareasByVisitaId(visitaId);
  if (current.length) return current;
  const tiendaMarcas = await getTiendaMarcasActivasByTiendaId(tiendaId);
  const header = await getTareasVisitaHeader();
  const rows = [];
  const created = [];

  for (const tm of tiendaMarcas) {
    const reglas = await getReglasPorMarca(tm.marca_id);
    for (const regla of reglas) {
      const payload = {
        tarea_id: `TV-${Date.now()}-${tm.marca_id}-${evidenceTypeKey(regla.tipo_evidencia).replace(/[^A-Z0-9]+/g, "").slice(0, 24)}`,
        visita_id: visitaId,
        plan_id: planId || "",
        fecha,
        promotor_id: promotorId,
        tienda_id: tiendaId,
        marca_id: tm.marca_id,
        tipo_evidencia: canonicalEvidenceTypeLabel(regla.tipo_evidencia),
        fotos_requeridas: safeInt(regla.fotos_requeridas, 1),
        requiere_antes_despues: regla.requiere_antes_despues ? "TRUE" : "FALSE",
        estatus: "PENDIENTE",
        total_fotos_cargadas: 0,
        alerta_generada: "FALSE",
        updated_at: nowISO(),
      };
      rows.push(buildTareaVisitaRowFromHeader(header, payload));
      created.push(payload);
    }
  }

  if (rows.length) await appendSheetValues(`TAREAS_VISITA!A2:${headerRangeEnd(header.length)}`, rows);
  return created;
}

function getValidEvidenceRowsForTask(evidences, task) {
  return evidences.filter(
    (item) =>
      item.visita_id === task.visita_id &&
      item.marca_id === task.marca_id &&
      evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(task.tipo_evidencia) &&
      upper(item.status) !== "ANULADA"
  );
}

function countAntesDespuesForTask(evidences, task) {
  const rows = getValidEvidenceRowsForTask(evidences, task);
  return {
    total: rows.length,
    antes: rows.filter((r) => upper(r.fase) === "ANTES").length,
    despues: rows.filter((r) => upper(r.fase) === "DESPUES").length,
  };
}

function resolveTaskStatus(task, counters) {
  if (!counters.total) return { estatus: "PENDIENTE", total_fotos_cargadas: 0 };

  const required = safeInt(task.fotos_requeridas, 1);

  if (task.requiere_antes_despues) {
    if (counters.antes >= 1 && counters.despues >= 1 && counters.total >= required) {
      return { estatus: "CUMPLIDA", total_fotos_cargadas: counters.total };
    }
    return { estatus: "INCOMPLETA", total_fotos_cargadas: counters.total };
  }

  if (counters.total >= required) {
    return { estatus: "CUMPLIDA", total_fotos_cargadas: counters.total };
  }

  return { estatus: "INCOMPLETA", total_fotos_cargadas: counters.total };
}

async function recalculateTareasForVisita(visitaId) {
  const tareasPack = await getTareasRowsAll();
  const tareas = tareasPack.rows.filter((item) => item.visita_id === visitaId);
  if (!tareas.length) return [];

  const evidences = await getEvidenciasByVisitId(visitaId);
  const updated = [];

  for (const task of tareas) {
    const counters = countAntesDespuesForTask(evidences, task);
    const resolved = resolveTaskStatus(task, counters);
    await updateTareaVisitaRow(tareasPack.header, task, {
      ...resolved,
      updated_at: nowISO(),
    });
    updated.push({ ...task, ...resolved });
  }

  return updated;
}

function isVisitValidForPlan(visit) {
  return ["OK_EN_GEOCERCA", "OK_CON_TOLERANCIA_GPS"].includes(upper(visit?.resultado_geocerca_entrada));
}

async function getSupervisorExternalIdByPromotorId(promotorId) {
  const rows = await getSheetValues("PROMOTORES!A2:H");
  const row = rows.find((r) => norm(r[1]) === promotorId);
  return row ? norm(row[6]) : "";
}

async function createNoVisitaAlert(planRow) {
  const supervisor_id = await getSupervisorExternalIdByPromotorId(planRow.promotor_id);
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: planRow.promotor_id,
    visita_id: "",
    evidencia_id: "",
    tipo_alerta: "NO_VISITA_TIENDA",
    severidad: "ALTA",
    descripcion: `No se registró entrada válida para ${planRow.tienda_id} en la fecha ${planRow.fecha}`,
    status: "ABIERTA",
    supervisor_id,
    tienda_id: planRow.tienda_id,
    canal_notificacion: "PLANEACION",
  });
}

async function createIncumplimientoEvidenciaAlert(taskRow) {
  const supervisor_id = await getSupervisorExternalIdByPromotorId(taskRow.promotor_id);
  return createAlert({
    alerta_id: `ALT-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    fecha_hora: nowISO(),
    promotor_id: taskRow.promotor_id,
    visita_id: taskRow.visita_id,
    evidencia_id: "",
    tipo_alerta: "INCUMPLIMIENTO_EVIDENCIA",
    severidad: "MEDIA",
    descripcion: `Incumplimiento en ${taskRow.marca_id} / ${taskRow.tipo_evidencia}. Requeridas: ${taskRow.fotos_requeridas}, cargadas: ${taskRow.total_fotos_cargadas}`,
    status: "ABIERTA",
    supervisor_id,
    tienda_id: taskRow.tienda_id,
    canal_notificacion: "TAREAS_VISITA",
  });
}

async function runDailyComplianceClose(dateYmd) {
  const planeacionPack = await getPlaneacionRowsAll();
  const planeacion = planeacionPack.rows.filter((item) => item.fecha === dateYmd);
  const visitMap = await getAllVisitsMap();
  const allVisits = Object.values(visitMap).filter((v) => v.fecha === dateYmd);

  let noVisitAlerts = 0;
  for (const plan of planeacion) {
    if (!["PLANEADA", "REPROGRAMADA"].includes(plan.estatus)) continue;
    const validVisit = allVisits.find(
      (v) =>
        v.promotor_id === plan.promotor_id &&
        v.tienda_id === plan.tienda_id &&
        isVisitValidForPlan(v)
    );
    if (!validVisit) {
      await updatePlaneacionRow(planeacionPack.header, plan, {
        estatus: "NO_VISITADA",
        updated_at: nowISO(),
      });
      await createNoVisitaAlert(plan);
      noVisitAlerts += 1;
    }
  }

  const tareasPack = await getTareasRowsAll();
  let incumplimientoAlerts = 0;
  for (const tarea of tareasPack.rows.filter((item) => item.fecha === dateYmd)) {
    if (["CUMPLIDA", "INCUMPLIDA"].includes(tarea.estatus)) continue;
    await updateTareaVisitaRow(tareasPack.header, tarea, {
      estatus: "INCUMPLIDA",
      updated_at: nowISO(),
    });
    if (upper(tarea.alerta_generada) !== "TRUE") {
      await createIncumplimientoEvidenciaAlert(tarea);
      await updateTareaVisitaRow(tareasPack.header, tarea, {
        alerta_generada: "TRUE",
        updated_at: nowISO(),
        estatus: "INCUMPLIDA",
      });
      incumplimientoAlerts += 1;
    }
  }

  return { noVisitAlerts, incumplimientoAlerts };
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

function buildEvidenceView(evidence, marcaMap, visitMap, tiendaMap, promotorMap) {
  const visit = visitMap[evidence.visita_id] || {};
  const tienda = tiendaMap[visit.tienda_id] || {};
  const promotor = promotorMap[visit.promotor_id] || {};
  return {
    evidencia_id: evidence.evidencia_id,
    visita_id: evidence.visita_id,
    fecha_hora: evidence.fecha_hora,
    fecha_hora_fmt: fmtDateTimeTZ(evidence.fecha_hora),
    url_foto: evidence.url_foto,
    tienda_id: visit.tienda_id || "",
    tienda_nombre: tienda.nombre_tienda || visit.tienda_id || "",
    cadena: tienda.cadena || "",
    region: tienda.region || "",
    marca_id: evidence.marca_id,
    marca_nombre: marcaMap[evidence.marca_id]?.marca_nombre || evidence.marca_id,
    tipo_evidencia: evidence.tipo_evidencia,
    fase: evidence.fase,
    descripcion: evidence.descripcion,
    decision_supervisor: evidence.decision_supervisor,
    status: evidence.status,
    riesgo: evidence.riesgo,
    resultado_ai: evidence.resultado_ai,
    hallazgos_ai: evidence.hallazgos_ai,
    promotor_nombre: promotor.nombre || visit.promotor_id || "",
  };
}

function getMiniAppUrl() {
  return (MINIAPP_BASE_URL || PUBLIC_BASE_URL || "").replace(/\/+$/, "");
}

async function telegramApi(method, payload) {
  if (!TELEGRAM_API) throw new Error("TELEGRAM_API not configured");
  const res = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.ok === false) {
    throw new Error(`telegram ${method} failed: ${res.status} ${JSON.stringify(json)}`);
  }
  return json.result;
}

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function pickPhotoUrl(photo) {
  if (!photo) return "";
  return norm(photo.dataUrl || photo.url || photo.photo || "");
}

function pickPhotoName(photo, fallback = "evidencia.jpg") {
  return fitCell(norm(photo?.name || fallback));
}

async function buildGeofenceResult(tiendaId, lat, lon, accuracy) {
  const tiendaMap = await getTiendaMap();
  const tienda = tiendaMap[tiendaId];
  const policy = await getPolicyValidation();
  const latNum = safeNum(lat, NaN);
  const lonNum = safeNum(lon, NaN);
  const accNum = safeNum(accuracy, 0);

  if (!tienda || !Number.isFinite(tienda.lat) || !Number.isFinite(tienda.lon)) {
    return {
      resultado: "SIN_TIENDA_GPS",
      distancia_m: "",
      accuracy_m: accNum || "",
      radio_tienda_m: tienda?.radio_m || policy.radio_estandar,
      lat_tienda: tienda?.lat || "",
      lon_tienda: tienda?.lon || "",
      severidad: "MEDIA",
    };
  }

  if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) {
    return {
      resultado: policy.sin_gps === "PERMITIR" ? "OK_SIN_GPS" : "SIN_DATOS_GPS",
      distancia_m: "",
      accuracy_m: accNum || "",
      radio_tienda_m: tienda.radio_m || policy.radio_estandar,
      lat_tienda: tienda.lat,
      lon_tienda: tienda.lon,
      severidad: "MEDIA",
    };
  }

  const distance = haversineDistanceMeters(latNum, lonNum, tienda.lat, tienda.lon);
  const classified = classifyGeofence(distance, tienda.radio_m || policy.radio_estandar, accNum);
  return {
    resultado: classified.result,
    distancia_m: distance,
    accuracy_m: accNum,
    radio_tienda_m: tienda.radio_m || policy.radio_estandar,
    lat_tienda: tienda.lat,
    lon_tienda: tienda.lon,
    severidad: classified.severity,
  };
}

function buildSupervisorKeyboard() {
  return {
    inline_keyboard: [[{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }]],
  };
}

function buildPromotorKeyboard() {
  return {
    inline_keyboard: [[{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }]],
  };
}

function buildClienteKeyboard() {
  return {
    inline_keyboard: [[{ text: "Abrir panel", web_app: { url: getMiniAppUrl() } }]],
  };
}

async function getPromotorHomeSummary(promotor) {
  const visitsToday = await getVisitasToday(promotor.promotor_id);
  const openVisits = visitsToday.filter((v) => !v.hora_fin);
  const evidencesToday = await getEvidenciasTodayByExternalId(promotor.external_id);
  return {
    text: [
      `👋 *${promotor.nombre || "Promotor"}*`,
      "",
      `Visitas hoy: *${visitsToday.length}*`,
      `Abiertas: *${openVisits.length}*`,
      `Evidencias hoy: *${evidencesToday.filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA").length}*`,
      "",
      "Usa el panel para registrar entrada, salida y evidencias.",
    ].join("\n"),
    reply_markup: buildPromotorKeyboard(),
  };
}

async function getSupervisorHomeSummary(supervisor) {
  const team = await getPromotoresDeSupervisor(supervisor.external_id);
  const promotorIds = team.map((p) => p.promotor_id);
  const visitMap = await getAllVisitsMap();
  const allVisits = Object.values(visitMap).filter((v) => v.fecha === todayISO() && promotorIds.includes(v.promotor_id));
  const abiertas = allVisits.filter((v) => !v.hora_fin);
  const alerts = (await getAlertsAll()).filter((a) => promotorIds.includes(a.promotor_id) && upper(a.status) === "ABIERTA");
  const allEvidences = await getEvidenciasAll();
  const visible = allEvidences.filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA" && promotorIds.includes(visitMap[e.visita_id]?.promotor_id));
  return {
    text: [
      `🧭 *${supervisor.nombre || "Supervisor"}*`,
      "",
      `Equipo: *${team.length}*`,
      `Visitas hoy: *${allVisits.length}*`,
      `Abiertas: *${abiertas.length}*`,
      `Alertas abiertas: *${alerts.length}*`,
      `Evidencias operativas: *${visible.length}*`,
      "",
      "Abre el panel para revisar equipo, alertas y evidencias.",
    ].join("\n"),
    reply_markup: buildSupervisorKeyboard(),
  };
}

async function getClienteHomeSummary(clienteProfile) {
  const ctx = await getClienteContextByExternalId(clienteProfile.external_id);
  const clienteId = ctx?.cliente?.cliente_id || clienteProfile.cliente_id || "";
  const marcaMap = await getMarcaMap();
  const clienteMarcaIds = Object.values(marcaMap).filter((m) => m.cliente_id === clienteId).map((m) => m.marca_id);
  const allEvidences = await getEvidenciasAll();
  const evidences = allEvidences.filter((e) => clienteMarcaIds.includes(e.marca_id));
  const aprobadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "APROBADA").length;
  const observadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "OBSERVADA").length;
  const rechazadas = evidences.filter((e) => upper(e.decision_supervisor || e.status) === "RECHAZADA").length;
  return {
    text: [
      `📊 *${ctx?.cliente?.cliente_nombre || clienteProfile.cliente_nombre || "Cliente"}*`,
      "",
      `Marcas visibles: *${clienteMarcaIds.length}*`,
      `Evidencias: *${evidences.length}*`,
      `Aprobadas: *${aprobadas}*`,
      `Observadas: *${observadas}*`,
      `Rechazadas: *${rechazadas}*`,
      "",
      "Abre el panel para ver resumen, tiendas, evidencias e incidencias.",
    ].join("\n"),
    reply_markup: buildClienteKeyboard(),
  };
}

async function getDynamicMenuText(actor) {
  if (actor.role === "promotor") return getPromotorHomeSummary(actor.profile);
  if (actor.role === "supervisor") return getSupervisorHomeSummary(actor.profile);
  return getClienteHomeSummary(actor.profile);
}

app.get("/health", async (_req, res) => {
  res.json({ ok: true, app_tz: APP_TZ, version: EVPLUS_VERSION });
});

app.get("/telegram/webhook", asyncHandler(async (req, res) => {
  res.json({ ok: true, mode: "telegram-miniapp" });
}));

app.post("/telegram/webhook", asyncHandler(async (req, res) => {
  const update = req.body || {};
  const message = update.message || update.edited_message;
  if (!message?.chat?.id) return res.json({ ok: true, ignored: true });

  const chatId = String(message.chat.id);
  const text = norm(message.text);
  const fromId = message.from?.id;
  const externalId = fromId ? buildExternalIdFromTelegramUser(fromId) : "";
  if (externalId) {
    await upsertSessionData(externalId, { chat_id: chatId, last_message_at: nowISO() });
  }

  if (text === "/start" || text === "/menu" || upper(text) === "ABRIR PANEL") {
    let actor = { role: "cliente", profile: { external_id: externalId, nombre: "Usuario" } };
    if (externalId) actor = await resolveActor(externalId);
    const summary = await getDynamicMenuText(actor);
    await sendTelegramText(chatId, summary.text, { reply_markup: summary.reply_markup });
    return res.json({ ok: true, action: "menu" });
  }

  await sendTelegramText(chatId, "Usa /menu para abrir el panel.");
  res.json({ ok: true, action: "fallback" });
}));

app.post("/miniapp/bootstrap", asyncHandler(async (req, res) => {
  const { actor, validated } = await getActorFromRequest(req);
  await upsertSessionData(validated.external_id, { chat_id: String(req.body?.chat_id || ""), last_bootstrap_at: nowISO() });
  const miniappUrl = getMiniAppUrl();

  if (actor.role === "promotor") {
    const assignedStores = await getTiendasAsignadas(actor.profile.promotor_id);
    const tiendaMap = await getTiendaMap();
    const marcas = await getMarcasActivas();
    return res.json({
      ok: true,
      role: "promotor",
      profile: actor.profile,
      config: {
        miniapp_url: miniappUrl,
        today: todayISO(),
      },
      stores: assignedStores.map((id) => ({
        tienda_id: id,
        tienda_nombre: tiendaMap[id]?.nombre_tienda || id,
        cadena: tiendaMap[id]?.cadena || "",
        region: tiendaMap[id]?.region || "",
      })),
      marcas,
    });
  }

  if (actor.role === "supervisor") {
    const team = await getPromotoresDeSupervisor(actor.profile.external_id);
    return res.json({
      ok: true,
      role: "supervisor",
      profile: actor.profile,
      config: {
        miniapp_url: miniappUrl,
        today: todayISO(),
      },
      team,
    });
  }

  const ctx = await getClienteContextByExternalId(actor.profile.external_id);
  return res.json({
    ok: true,
    role: "cliente",
    profile: actor.profile,
    cliente: ctx?.cliente || null,
    access: ctx?.access || null,
    config: {
      miniapp_url: miniappUrl,
      today: todayISO(),
    },
  });
}));

app.post("/miniapp/promotor/start-entry", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });

  const {
    tienda_id,
    lat = "",
    lon = "",
    accuracy = "",
    selfie_url = "",
    foto_data_url = "",
    foto_nombre = "",
    notas = "",
  } = req.body || {};

  if (!tienda_id) return res.status(400).json({ ok: false, error: "tienda_id requerido" });

  const existingOpen = await getOpenVisitsToday(actor.profile.promotor_id);
  const duplicated = existingOpen.find((visit) => visit.tienda_id === tienda_id);
  if (duplicated) {
    return res.status(409).json({ ok: false, error: "ya_hay_visita_abierta", visita_id: duplicated.visita_id });
  }

  const geo = await buildGeofenceResult(tienda_id, lat, lon, accuracy);
  const visita_id = await createVisitWithGeofence(actor.profile.promotor_id, tienda_id, notas, geo);
  const photoUrl = selfie_url || foto_data_url || "";

  let warning = "";
  if (photoUrl || lat || lon) {
    const aux = await registrarEvidencia({
      evidencia_id: `EV-${Date.now()}-ASIS-IN`,
      external_id: actor.profile.external_id,
      tipo_evento: "ASISTENCIA_ENTRADA",
      origen: "ASISTENCIA",
      visita_id,
      url_foto: photoUrl,
      lat,
      lon,
      tipo_evidencia: "ASISTENCIA",
      descripcion: `[TELEGRAM_MINIAPP_ENTRADA] GEO:${geo.resultado}`,
      riesgo: geo.resultado === "FUERA_DE_GEOCERCA" ? "ALTO" : geo.resultado === "OK_CON_TOLERANCIA_GPS" ? "MEDIO" : "BAJO",
      score_confianza: 0.93,
      resultado_ai: `Entrada validada (${geo.resultado})`,
      status: "ACTIVA",
      note: geo.resultado,
      fase: "NA",
      foto_nombre,
      accuracy,
    });
    if (aux.photoOverflow) warning = "attendance_photo_too_large_for_sheets";
  }

  if (geo.resultado === "FUERA_DE_GEOCERCA") {
    await createAlert({
      alerta_id: `ALT-${Date.now()}`,
      fecha_hora: nowISO(),
      promotor_id: actor.profile.promotor_id,
      visita_id,
      evidencia_id: "",
      tipo_alerta: "ASISTENCIA_FUERA_GEOCERCA_ENTRADA",
      severidad: "ALTA",
      descripcion: `Entrada fuera de geocerca en tienda ${tienda_id}. Distancia ${geo.distancia_m}m / Radio ${geo.radio_tienda_m}m`,
      status: "ABIERTA",
      supervisor_id: actor.profile.supervisor_external_id || "",
      tienda_id,
      canal_notificacion: "MINIAPP",
    });
  }

  const planRow = await findPlaneacionForVisit(todayISO(), actor.profile.promotor_id, tienda_id);
  if (planRow) {
    const planeacionHeader = await getPlaneacionHeader();
    await updatePlaneacionRow(planeacionHeader, planRow, {
      estatus: "VISITADA",
      updated_at: nowISO(),
    });
    await createTareasForVisita({
      visitaId: visita_id,
      planId: planRow.plan_id,
      fecha: todayISO(),
      promotorId: actor.profile.promotor_id,
      tiendaId: tienda_id,
    });
  }

  const tiendaMap = await getTiendaMap();
  res.json({
    ok: true,
    visita_id,
    tienda_id,
    tienda_nombre: tiendaMap[tienda_id]?.nombre_tienda || tienda_id,
    started_at: nowISO(),
    warning,
    geofence: {
      result: geo.resultado,
      distance_m: geo.distancia_m,
      accuracy_m: geo.accuracy_m,
      radius_m: geo.radio_tienda_m,
    },
    linked_plan: !!planRow,
    plan_id: planRow?.plan_id || "",
  });
}));

app.post("/miniapp/promotor/end-entry", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });

  const { visita_id, lat = "", lon = "", accuracy = "", notas = "", selfie_url = "", foto_data_url = "", foto_nombre = "" } = req.body || {};
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) {
    return res.status(404).json({ ok: false, error: "visita_no_encontrada" });
  }

  const geo = await buildGeofenceResult(visit.tienda_id, lat, lon, accuracy);
  await closeVisitWithGeofence(visit, notas, geo);

  const photoUrl = selfie_url || foto_data_url || "";
  let warning = "";
  if (photoUrl || lat || lon) {
    const aux = await registrarEvidencia({
      evidencia_id: `EV-${Date.now()}-ASIS-OUT`,
      external_id: actor.profile.external_id,
      tipo_evento: "ASISTENCIA_SALIDA",
      origen: "ASISTENCIA",
      visita_id,
      url_foto: photoUrl,
      lat,
      lon,
      tipo_evidencia: "ASISTENCIA",
      descripcion: `[TELEGRAM_MINIAPP_SALIDA] GEO:${geo.resultado}`,
      riesgo: geo.resultado === "FUERA_DE_GEOCERCA" ? "ALTO" : geo.resultado === "OK_CON_TOLERANCIA_GPS" ? "MEDIO" : "BAJO",
      score_confianza: 0.93,
      resultado_ai: `Salida validada (${geo.resultado})`,
      status: "ACTIVA",
      note: geo.resultado,
      fase: "NA",
      foto_nombre,
      accuracy,
    });
    if (aux.photoOverflow) warning = "attendance_photo_too_large_for_sheets";
  }

  if (geo.resultado === "FUERA_DE_GEOCERCA") {
    await createAlert({
      alerta_id: `ALT-${Date.now()}`,
      fecha_hora: nowISO(),
      promotor_id: actor.profile.promotor_id,
      visita_id,
      evidencia_id: "",
      tipo_alerta: "ASISTENCIA_FUERA_GEOCERCA_SALIDA",
      severidad: "ALTA",
      descripcion: `Salida fuera de geocerca en tienda ${visit.tienda_id}. Distancia ${geo.distancia_m}m / Radio ${geo.radio_tienda_m}m`,
      status: "ABIERTA",
      supervisor_id: actor.profile.supervisor_external_id || "",
      tienda_id: visit.tienda_id,
      canal_notificacion: "MINIAPP",
    });
  }

  res.json({
    ok: true,
    visita_id,
    ended_at: nowISO(),
    warning,
    geofence: {
      result: geo.resultado,
      distance_m: geo.distancia_m,
      accuracy_m: geo.accuracy_m,
      radius_m: geo.radio_tienda_m,
    },
  });
}));

app.post("/miniapp/promotor/evidence-register", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });

  const {
    visita_id,
    marca_id = "",
    marca_nombre = "",
    tipo_evidencia,
    descripcion = "",
    fase = "NA",
    lat = "",
    lon = "",
    accuracy = "",
    fotos = [],
    foto_data_url = "",
    foto_nombre = "",
  } = req.body || {};

  if (!visita_id || !tipo_evidencia) return res.status(400).json({ ok: false, error: "payload_incompleto" });

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) {
    return res.status(404).json({ ok: false, error: "visita_no_encontrada" });
  }

  let resolvedMarcaId = norm(marca_id);
  if (!resolvedMarcaId && marca_nombre) resolvedMarcaId = await resolveMarcaIdByName(marca_nombre);
  if (!resolvedMarcaId && marca_nombre) resolvedMarcaId = marca_nombre;

  const finalFotos =
    Array.isArray(fotos) && fotos.length
      ? fotos
      : (foto_data_url ? [{ dataUrl: foto_data_url, name: foto_nombre || "evidencia.jpg" }] : []);

  if (!finalFotos.length) {
    return res.status(400).json({ ok: false, error: "fotos_requeridas" });
  }

  if (resolvedMarcaId) {
    const reglas = await getReglasPorMarca(resolvedMarcaId);
    const rule = reglas.find((item) => evidenceTypeKey(item.tipo_evidencia) === evidenceTypeKey(tipo_evidencia));
    if (rule && finalFotos.length < rule.fotos_requeridas) {
      return res.status(400).json({
        ok: false,
        error: "fotos_insuficientes",
        expected: rule.fotos_requeridas,
        received: finalFotos.length,
      });
    }
  }

  const created = [];
  let photoOverflow = false;
  const tipo_evento = `EVIDENCIA_${upper(tipo_evidencia).replace(/\W+/g, "_")}`;

  for (let i = 0; i < finalFotos.length; i += 1) {
    const foto = finalFotos[i];
    const evidencia_id = `EV-${Date.now()}-${i + 1}`;
    const result = await registrarEvidencia({
      evidencia_id,
      external_id: actor.profile.external_id,
      tipo_evento,
      origen: "EVIDENCIA",
      visita_id,
      url_foto: pickPhotoUrl(foto),
      lat,
      lon,
      accuracy,
      marca_id: resolvedMarcaId,
      tipo_evidencia,
      descripcion,
      riesgo: "BAJO",
      score_confianza: 0.9,
      resultado_ai: "Evidencia recibida",
      status: "RECIBIDA",
      note: "",
      fase,
      foto_nombre: pickPhotoName(foto, foto_nombre || `evidencia_${i + 1}.jpg`),
      hash_foto: buildEvidencePhotoHash(pickPhotoUrl(foto), pickPhotoName(foto, foto_nombre || `evidencia_${i + 1}.jpg`)),
    });
    if (result.photoOverflow) photoOverflow = true;
    created.push(evidencia_id);
  }

  scheduleEvidenceGroupAnalysis({
    visitaId: visita_id,
    marcaId: resolvedMarcaId,
    tipoEvidencia: tipo_evidencia,
    fase,
  });

  await recalculateTareasForVisita(visita_id);

  res.json({
    ok: true,
    visita_id,
    created,
    count: created.length,
    warning: photoOverflow ? "evidence_photo_too_large_for_sheets" : "",
    analysis_status: "scheduled",
  });
}));

app.post("/miniapp/promotor/cancel-evidence", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });

  const { evidencia_id, note = "" } = req.body || {};
  if (!evidencia_id) return res.status(400).json({ ok: false, error: "evidencia_id requerido" });

  const found = await getEvidenceById(evidencia_id);
  if (!found || found.evidence.external_id !== actor.profile.external_id) {
    return res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
  }

  await updateEvidenceRow(found.header, found.evidence, {
    status: "ANULADA",
    note,
    resultado_ai: "ANULADA_MANUAL",
    riesgo: "BAJO",
  });

  if (upper(found.evidence.tipo_evidencia) !== "ASISTENCIA") {
    await recalculateTareasForVisita(found.evidence.visita_id);
  }

  res.json({ ok: true, evidencia_id, status: "ANULADA" });
}));

app.post("/miniapp/promotor/replace-evidence", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });

  const {
    evidencia_id,
    url_foto = "",
    foto_data_url = "",
    foto_nombre = "",
    resultado_ai = "",
    score_confianza = "",
    riesgo = "",
  } = req.body || {};

  if (!evidencia_id) return res.status(400).json({ ok: false, error: "evidencia_id requerido" });

  const found = await getEvidenceById(evidencia_id);
  if (!found || found.evidence.external_id !== actor.profile.external_id) {
    return res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
  }

  const newPhotoUrl = url_foto || foto_data_url;
  if (!newPhotoUrl) return res.status(400).json({ ok: false, error: "foto_requerida" });

  const result = await updateEvidenceRow(found.header, found.evidence, {
    url_foto: newPhotoUrl,
    fecha_hora: nowISO(),
    foto_nombre: foto_nombre || found.evidence.foto_nombre,
    resultado_ai: resultado_ai || found.evidence.resultado_ai,
    score_confianza: score_confianza || found.evidence.score_confianza,
    riesgo: riesgo || found.evidence.riesgo,
    hash_foto: buildEvidencePhotoHash(newPhotoUrl, foto_nombre || found.evidence.foto_nombre),
  });

  if (upper(found.evidence.tipo_evidencia) !== "ASISTENCIA") {
    scheduleEvidenceGroupAnalysis({
      visitaId: found.evidence.visita_id,
      marcaId: found.evidence.marca_id,
      tipoEvidencia: found.evidence.tipo_evidencia,
      fase: found.evidence.fase || "NA",
    });
    await recalculateTareasForVisita(found.evidence.visita_id);
  }

  res.json({
    ok: true,
    evidencia_id,
    replaced: true,
    warning: result.photoOverflow ? "evidence_photo_too_large_for_sheets" : "",
  });
}));

app.post("/miniapp/promotor/today", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const visits = await getVisitasToday(actor.profile.promotor_id);
  const tiendaMap = await getTiendaMap();
  const evidences = await getEvidenciasTodayByExternalId(actor.profile.external_id);

  res.json({
    ok: true,
    fecha: todayISO(),
    visitas: visits.map((v) => ({
      ...v,
      tienda_nombre: tiendaMap[v.tienda_id]?.nombre_tienda || v.tienda_id,
      cadena: tiendaMap[v.tienda_id]?.cadena || "",
      region: tiendaMap[v.tienda_id]?.region || "",
    })),
    evidencias: evidences,
  });
}));

app.post("/miniapp/supervisor/bootstrap", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  return res.json({ ok: true, profile: actor.profile, team });
}));

app.post("/miniapp/supervisor/team", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const visitMap = await getAllVisitsMap();
  const allVisits = Object.values(visitMap).filter((v) => v.fecha === todayISO());
  const evidences = await getEvidenciasAll();

  const rows = team.map((member) => {
    const memberVisits = allVisits.filter((v) => v.promotor_id === member.promotor_id);
    const abiertas = memberVisits.filter((v) => !v.hora_fin).length;
    const evids = evidences.filter((e) => visitMap[e.visita_id]?.promotor_id === member.promotor_id && upper(e.tipo_evidencia) !== "ASISTENCIA");
    return {
      ...member,
      visitas_hoy: memberVisits.length,
      abiertas,
      evidencias: evids.length,
      ultima_visita: memberVisits.slice().sort((a, b) => String(b.hora_inicio).localeCompare(String(a.hora_inicio)))[0]?.hora_inicio || "",
    };
  });

  res.json({ ok: true, rows });
}));

app.post("/miniapp/supervisor/alerts", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const promotorIds = new Set(team.map((p) => p.promotor_id));
  const alerts = (await getAlertsAll()).filter((a) => promotorIds.has(a.promotor_id));
  const visitMap = await getAllVisitsMap();
  const tiendaMap = await getTiendaMap();
  const promotorMap = await getPromotorMap();

  const rows = alerts
    .map((a) => ({
      ...a,
      fecha_hora_fmt: fmtDateTimeTZ(a.fecha_hora),
      tienda_nombre: tiendaMap[a.tienda_id]?.nombre_tienda || a.tienda_id,
      promotor_nombre: promotorMap[a.promotor_id]?.nombre || a.promotor_id,
      visita_abierta: !!visitMap[a.visita_id] && !visitMap[a.visita_id]?.hora_fin,
    }))
    .sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));

  res.json({ ok: true, rows });
}));

app.post("/miniapp/supervisor/evidences", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const promotorIds = new Set(team.map((p) => p.promotor_id));
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const tiendaMap = await getTiendaMap();
  const promotorMap = await getPromotorMap();
  let evidences = (await getEvidenciasAll()).filter(
    (e) => upper(e.tipo_evidencia) !== "ASISTENCIA" && promotorIds.has(visitMap[e.visita_id]?.promotor_id)
  );

  const promotor_id = norm(req.body?.promotor_id);
  if (promotor_id) evidences = evidences.filter((e) => visitMap[e.visita_id]?.promotor_id === promotor_id);

  const tipo = norm(req.body?.tipo_evidencia);
  if (tipo) evidences = evidences.filter((e) => evidenceTypeKey(e.tipo_evidencia) === evidenceTypeKey(tipo));

  const rows = evidences
    .map((e) => buildEvidenceView(e, marcaMap, visitMap, tiendaMap, promotorMap))
    .sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));

  res.json({ ok: true, rows });
}));

app.post("/miniapp/supervisor/review-evidence", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const { evidencia_ids = [], decision = "", motivo_revision = "" } = req.body || {};
  if (!Array.isArray(evidencia_ids) || !evidencia_ids.length) {
    return res.status(400).json({ ok: false, error: "evidencia_ids requeridos" });
  }

  const decisionUpper = upper(decision);
  if (!["APROBADA", "OBSERVADA", "RECHAZADA"].includes(decisionUpper)) {
    return res.status(400).json({ ok: false, error: "decision_invalida" });
  }

  const updated = [];
  for (const evidencia_id of evidencia_ids) {
    const found = await getEvidenceById(evidencia_id);
    if (!found) continue;
    await updateEvidenceRow(found.header, found.evidence, {
      decision_supervisor: decisionUpper,
      motivo_revision,
      revisado_por: actor.profile.supervisor_id || actor.profile.external_id,
      fecha_revision: nowISO(),
      status: decisionUpper,
      requiere_revision_supervisor: "FALSE",
    });
    updated.push(evidencia_id);
  }

  res.json({ ok: true, updated, count: updated.length });
}));

app.post("/miniapp/supervisor/visit-dossier", asyncHandler(async (req, res) => {
  const { actor } = await getActorFromRequest(req);
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });

  const { visita_id } = req.body || {};
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });

  const visit = await getVisitById(visita_id);
  if (!visit) return res.status(404).json({ ok: false, error: "visita_no_encontrada" });

  const tiendaMap = await getTiendaMap();
  const marcaMap = await getMarcaMap();
  const promotorMap = await getPromotorMap();
  const visitMap = await getAllVisitsMap();
  const evidences = await getEvidenciasByVisitId(visita_id);
  const alerts = (await getAlertsAll()).filter((a) => a.visita_id === visita_id);

  res.json({
    ok: true,
    visit: {
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      promotor_nombre: promotorMap[visit.promotor_id]?.nombre || visit.promotor_id,
    },
    evidences: evidences.map((e) => buildEvidenceView(e, marcaMap, visitMap, tiendaMap, promotorMap)),
    alerts,
  });
}));

app.post("/miniapp/cliente/bootstrap", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, error: "Solo cliente" });
    return res.json({ ok: true, data: { role: "cliente", cliente: ctx.cliente, access: ctx.access }, meta: {}, error: null });
  } catch (error) {
    return res.status(error.message === "Solo cliente" ? 403 : 401).json({ ok: false, data: null, meta: {}, error: error.message || "auth failed" });
  }
});

function parseClientDateFilters(filters = {}) {
  const start = norm(filters.fecha_inicio) || todayISO();
  const end = norm(filters.fecha_fin) || todayISO();
  return { fecha_inicio: start, fecha_fin: end };
}

function inRangeYmd(isoLike, startYmd, endYmd) {
  if (!isoLike) return false;
  const ymd = /^\d{4}-\d{2}-\d{2}$/.test(String(isoLike)) ? String(isoLike) : ymdInTZ(new Date(isoLike), APP_TZ);
  return ymd >= startYmd && ymd <= endYmd;
}

async function getClientDataset(clienteId, filters = {}) {
  const { fecha_inicio, fecha_fin } = parseClientDateFilters(filters);
  const marcaMap = await getMarcaMap();
  const allowedMarcaIds = Object.values(marcaMap).filter((item) => item.cliente_id === clienteId).map((item) => item.marca_id);

  const visitMapAll = await getAllVisitsMap();
  const evidences = (await getEvidenciasAll()).filter(
    (e) =>
      allowedMarcaIds.includes(e.marca_id) &&
      inRangeYmd(e.fecha_hora, fecha_inicio, fecha_fin)
  );

  const visitIds = new Set(evidences.map((e) => e.visita_id));
  const visits = Object.values(visitMapAll).filter((v) => visitIds.has(v.visita_id));
  const tiendaMap = await getTiendaMap();
  const promotorMap = await getPromotorMap();
  const alerts = (await getAlertsAll()).filter((a) => {
    const visit = visitMapAll[a.visita_id];
    const evidence = evidences.find((e) => e.evidencia_id === a.evidencia_id);
    return (visit && visitIds.has(visit.visita_id)) || !!evidence;
  });

  return {
    fecha_inicio,
    fecha_fin,
    allowedMarcaIds,
    evidences,
    visits,
    visitMap: visitMapAll,
    tiendaMap,
    promotorMap,
    marcaMap,
    alerts,
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

app.post("/miniapp/cliente/filter-options", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const data = await getClientDataset(ctx.cliente.cliente_id, req.body || {});
    const tiendas = unique(data.visits.map((v) => v.tienda_id))
      .map((id) => ({ id, label: data.tiendaMap[id]?.nombre_tienda || id }));
    const marcas = data.allowedMarcaIds.map((id) => ({ id, label: data.marcaMap[id]?.marca_nombre || id }));
    const tipos = unique(data.evidences.map((e) => e.tipo_evidencia)).map((t) => ({ id: t, label: t }));

    return res.json({
      ok: true,
      data: {
        periodos_predefinidos: [
          { id: "DIARIO", label: "Diario" },
          { id: "SEMANAL", label: "Semanal" },
          { id: "QUINCENAL", label: "Quincenal" },
          { id: "MENSUAL", label: "Mensual" },
          { id: "PERSONALIZADO", label: "Personalizado" },
        ],
        tiendas,
        marcas,
        tipos_evidencia: tipos,
        estatus_evidencia: [
          { id: "APROBADA", label: "Aprobada" },
          { id: "OBSERVADA", label: "Observada" },
          { id: "RECHAZADA", label: "Rechazada" },
        ],
        riesgos: [
          { id: "BAJO", label: "Bajo" },
          { id: "MEDIO", label: "Medio" },
          { id: "ALTO", label: "Alto" },
        ],
      },
      meta: {},
      error: null,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente filter-options error" });
  }
});

app.post("/miniapp/cliente/dashboard", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    const tiendasVisitadasSet = new Set(data.visits.map((v) => v.tienda_id));
    const evidAprobadas = data.evidences.filter((e) => upper(e.decision_supervisor || e.status) === "APROBADA").length;
    const evidObservadas = data.evidences.filter((e) => upper(e.decision_supervisor || e.status) === "OBSERVADA").length;
    const evidRechazadas = data.evidences.filter((e) => upper(e.decision_supervisor || e.status) === "RECHAZADA").length;
    const cumplimiento = data.visits.length ? 100 : 0;
    const semaforo = cumplimiento >= 90 ? { status: "VERDE", label: "Cumplimiento alto" } : cumplimiento >= 70 ? { status: "AMARILLO", label: "Cumplimiento parcial" } : { status: "ROJO", label: "Cumplimiento bajo" };

    return res.json({
      ok: true,
      data: {
        periodo: { fecha_inicio: data.fecha_inicio, fecha_fin: data.fecha_fin, label: `${data.fecha_inicio} a ${data.fecha_fin}` },
        kpis: {
          tiendas_visitadas: tiendasVisitadasSet.size,
          visitas_ejecutadas: data.visits.length,
          cumplimiento_pct: cumplimiento,
          evidencias_capturadas: data.evidences.length,
          evidencias_aprobadas: evidAprobadas,
          evidencias_observadas: evidObservadas,
          evidencias_rechazadas: evidRechazadas,
          alertas_total: data.alerts.length,
          alertas_abiertas: data.alerts.filter((a) => upper(a.status) === "ABIERTA").length,
        },
        semaforo_general: semaforo,
      },
      meta: {},
      error: null,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente dashboard error" });
  }
});

app.post("/miniapp/cliente/stores", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    const tiendaIds = unique(data.visits.map((v) => v.tienda_id));

    const rows = tiendaIds.map((tienda_id) => {
      const visits = data.visits.filter((v) => v.tienda_id === tienda_id);
      const evids = data.evidences.filter((e) => data.visitMap[e.visita_id]?.tienda_id === tienda_id);
      const latest = visits.slice().sort((a, b) => String(b.hora_inicio).localeCompare(String(a.hora_inicio)))[0] || null;
      return {
        tienda_id,
        tienda_nombre: data.tiendaMap[tienda_id]?.nombre_tienda || tienda_id,
        cadena: data.tiendaMap[tienda_id]?.cadena || "",
        region: data.tiendaMap[tienda_id]?.region || "",
        ciudad: data.tiendaMap[tienda_id]?.ciudad || "",
        visitas_periodo: visits.length,
        ultima_visita: latest?.hora_inicio || latest?.fecha || "",
        ultima_visita_fmt: latest?.hora_inicio ? fmtDateTimeTZ(latest.hora_inicio) : latest?.fecha || "",
        evidencias_aprobadas: evids.filter((e) => upper(e.decision_supervisor || e.status) === "APROBADA").length,
        evidencias_observadas: evids.filter((e) => upper(e.decision_supervisor || e.status) === "OBSERVADA").length,
        evidencias_rechazadas: evids.filter((e) => upper(e.decision_supervisor || e.status) === "RECHAZADA").length,
      };
    });

    const paged = paginateRows(rows, req.body?.pagination || {});
    return res.json({ ok: true, data: { rows: paged.rows }, meta: paged.meta, error: null });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente stores error" });
  }
});

app.post("/miniapp/cliente/store-detail", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const tiendaId = norm(req.body?.tienda_id);
    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    const visits = data.visits.filter((v) => v.tienda_id === tiendaId);
    const evidences = data.evidences.filter((e) => data.visitMap[e.visita_id]?.tienda_id === tiendaId)
      .map((e) => buildEvidenceView(e, data.marcaMap, data.visitMap, data.tiendaMap, data.promotorMap));
    const alerts = data.alerts.filter((a) => data.visitMap[a.visita_id]?.tienda_id === tiendaId);

    return res.json({
      ok: true,
      data: {
        store: data.tiendaMap[tiendaId] || null,
        summary: {
          visitas_periodo: visits.length,
          evidencias_aprobadas: evidences.filter((e) => upper(e.decision_supervisor || e.status) === "APROBADA").length,
          evidencias_observadas: evidences.filter((e) => upper(e.decision_supervisor || e.status) === "OBSERVADA").length,
          evidencias_rechazadas: evidences.filter((e) => upper(e.decision_supervisor || e.status) === "RECHAZADA").length,
          alertas_total: alerts.length,
        },
        visitas: visits,
        evidencias_preview: evidences.slice(0, 30),
        incidencias: alerts,
      },
      meta: {},
      error: null,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente store-detail error" });
  }
});

app.post("/miniapp/cliente/evidences", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);

    let rows = data.evidences
      .filter((e) => upper(e.tipo_evidencia) !== "ASISTENCIA")
      .map((e) => buildEvidenceView(e, data.marcaMap, data.visitMap, data.tiendaMap, data.promotorMap));

    const tipo = norm(filters.tipo_evidencia);
    const decision = upper(filters.decision_supervisor);
    if (tipo) rows = rows.filter((r) => evidenceTypeKey(r.tipo_evidencia) === evidenceTypeKey(tipo));
    if (decision) rows = rows.filter((r) => upper(r.decision_supervisor || r.status) === decision);

    rows.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const paged = paginateRows(rows, req.body?.pagination || { page_size: 40 });

    return res.json({ ok: true, data: { rows: paged.rows }, meta: paged.meta, error: null });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente evidences error" });
  }
});

app.post("/miniapp/cliente/incidents", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    const filters = req.body?.filters || req.body || {};
    const data = await getClientDataset(ctx.cliente.cliente_id, filters);
    let rows = data.alerts.map((a) => ({
      ...a,
      fecha_hora_fmt: fmtDateTimeTZ(a.fecha_hora),
      tienda_nombre: data.tiendaMap[a.tienda_id]?.nombre_tienda || a.tienda_id,
      cadena: data.tiendaMap[a.tienda_id]?.cadena || "",
      region: data.tiendaMap[a.tienda_id]?.region || "",
      promotor_nombre: data.promotorMap[a.promotor_id]?.nombre || a.promotor_id,
    }));

    rows.sort((a, b) => String(b.fecha_hora).localeCompare(String(a.fecha_hora)));
    const paged = paginateRows(rows, req.body?.pagination || {});

    return res.json({ ok: true, data: { rows: paged.rows }, meta: paged.meta, error: null });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente incidents error" });
  }
});

app.post("/miniapp/cliente/deliverables", async (req, res) => {
  try {
    const { validated } = await getActorFromRequest(req);
    const ctx = await getClienteContextByExternalId(validated.external_id);
    if (!ctx) return res.status(403).json({ ok: false, data: null, meta: {}, error: "Solo cliente" });

    return res.json({
      ok: true,
      data: { rows: [], enabled: false, message: "Los entregables automáticos estarán disponibles en Fase 2." },
      meta: {},
      error: null,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, data: null, meta: {}, error: error.message || "cliente deliverables error" });
  }
});

app.post("/ops/planeacion/generate-range", asyncHandler(async (req, res) => {
  const start_date = norm(req.body?.start_date || todayISO());
  const end_date = norm(req.body?.end_date || start_date);
  const rows = await generatePlaneacionForRange(start_date, end_date);
  const summary = await upsertPlaneacionRows(rows);
  res.json({ ok: true, start_date, end_date, total_rows: rows.length, ...summary });
}));

app.post("/ops/compliance/run-close", asyncHandler(async (req, res) => {
  const date = norm(req.body?.date || todayISO());
  const result = await runDailyComplianceClose(date);
  res.json({ ok: true, date, ...result });
}));

app.post("/ops/tareas/rebuild-visita", asyncHandler(async (req, res) => {
  const visita_id = norm(req.body?.visita_id);
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });
  const rows = await recalculateTareasForVisita(visita_id);
  res.json({ ok: true, visita_id, updated: rows.length });
}));

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({ ok: false, error: error?.message || "internal_error" });
});

app.listen(Number(PORT), () => {
  console.log(`Promobolsillo server listening on :${PORT}`);
});
