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
  TELEGRAM_WEBHOOK_SECRET,
  MINIAPP_BASE_URL,
  PUBLIC_BASE_URL,
} = process.env;

const TELEGRAM_API = TELEGRAM_BOT_TOKEN
  ? `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`
  : "";

const app = express();
app.use(bodyParser.json({ limit: "35mb" }));
app.use(bodyParser.urlencoded({ extended: false, limit: "35mb" }));

const ALLOWED_ORIGINS = [
  MINIAPP_BASE_URL,
  "http://localhost:5173",
  "https://localhost:5173",
].filter(Boolean);

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
  const v = upper(value);
  return ["TRUE", "1", "SI", "SÍ", "VERDADERO", "YES"].includes(v);
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

function buildTelegramExternalId(telegramUserId) {
  return `telegram:${telegramUserId}`;
}

function unique(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function pickPhotoUrl(photoLike) {
  if (!photoLike) return "";
  if (typeof photoLike === "string") return photoLike;
  return norm(photoLike.url || photoLike.dataUrl || photoLike.file_id || photoLike.fileId);
}

function pickPhotoName(photoLike, fallback = "") {
  if (!photoLike) return fallback;
  if (typeof photoLike === "string") return fallback;
  return norm(photoLike.name || photoLike.fileName || fallback);
}

const SHEETS_CELL_SOFT_LIMIT = 48000;

function fitCell(value, limit = SHEETS_CELL_SOFT_LIMIT) {
  const str = value == null ? "" : String(value);
  return str.length > limit ? str.slice(0, limit) : str;
}

function normalizePhotoForSheet(value, fallbackTag = "[IMAGE_TOO_LARGE_FOR_SHEETS]") {
  const str = norm(value);
  if (!str) return { value: "", overflow: false, originalLength: 0 };
  const isDataUrl = str.startsWith("data:");
  if (!isDataUrl) {
    return {
      value: fitCell(str),
      overflow: str.length > SHEETS_CELL_SOFT_LIMIT,
      originalLength: str.length,
    };
  }
  if (str.length <= SHEETS_CELL_SOFT_LIMIT) {
    return { value: str, overflow: false, originalLength: str.length };
  }
  return { value: fallbackTag, overflow: true, originalLength: str.length };
}

function mergeOverflowNote(note, info) {
  const base = norm(note);
  if (!info?.overflow) return fitCell(base);
  const msg = `FOTO_EXCEDIDA_EN_SHEETS:${info.originalLength}`;
  return fitCell(base ? `${base} | ${msg}` : msg);
}

function toRadians(deg) {
  return (deg * Math.PI) / 180;
}

function haversineDistanceMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Math.round(R * c);
}

function classifyGeofence(distanceM, radiusM, accuracyM, fallbackPolicy) {
  if (!Number.isFinite(distanceM)) {
    return { result: fallbackPolicy || "SIN_DATOS_GPS", severity: "MEDIA" };
  }
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

function buildBaseUrl(req) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL.replace(/\/+$/, "");
  const proto = String(req.headers["x-forwarded-proto"] || "https");
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "");
  return `${proto}://${host}`.replace(/\/+$/, "");
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

function headerIndexMap(header) {
  const map = {};
  header.forEach((h, idx) => {
    if (h) map[h] = idx;
  });
  return map;
}

async function getPolicyValidation() {
  const rows = await getSheetValues("POLITICA_VALIDACION!A2:D");
  const row = rows[0] || [];
  return {
    radio_estandar: safeNum(row[0], 100),
    sin_gps: upper(row[1] || "RECHAZAR"),
    tiempo_revision: safeNum(row[2], 30),
  };
}

async function getPromotorByExternalId(externalId) {
  const rows = await getSheetValues("PROMOTORES!A2:G");
  for (const row of rows) {
    if (norm(row[0]) === externalId && isTrue(row[5])) {
      return {
        external_id: norm(row[0]),
        promotor_id: norm(row[1]),
        nombre: norm(row[2]),
        region: norm(row[3]),
        cadena_principal: norm(row[4]),
        activo: isTrue(row[5]),
        supervisor_external_id: norm(row[6]),
      };
    }
  }
  return null;
}

async function getSupervisorByExternalId(externalId) {
  const rows = await getSheetValues("SUPERVISORES!A2:H");
  for (const row of rows) {
    const primary = norm(row[0]);
    const tg = norm(row[7]);
    if ((primary === externalId || tg === externalId) && isTrue(row[5])) {
      return {
        external_id: primary || tg,
        supervisor_id: norm(row[1]),
        nombre: norm(row[2]),
        region: norm(row[3]),
        nivel: norm(row[4]),
        activo: isTrue(row[5]),
      };
    }
  }
  return null;
}

async function getPromotoresDeSupervisor(supervisorExternalId) {
  const rows = await getSheetValues("PROMOTORES!A2:G");
  return rows
    .filter((row) => isTrue(row[5]) && norm(row[6]) === supervisorExternalId)
    .map((row) => ({
      external_id: norm(row[0]),
      promotor_id: norm(row[1]),
      nombre: norm(row[2]),
      region: norm(row[3]),
      cadena_principal: norm(row[4]),
    }));
}

async function resolveActor(externalId) {
  const supervisor = await getSupervisorByExternalId(externalId);
  if (supervisor) return { role: "supervisor", profile: supervisor };
  const promotor = await getPromotorByExternalId(externalId);
  if (promotor) return { role: "promotor", profile: promotor };
  return { role: "cliente", profile: { external_id: externalId, nombre: "Cliente" } };
}

async function getTiendaMap() {
  const rows = await getSheetValues("TIENDAS!A2:J");
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
      activa: isTrue(row[5]),
      direccion: norm(row[6]),
      lat: safeNum(row[7], NaN),
      lon: safeNum(row[8], NaN),
      radio_m: safeNum(row[9], 0),
    };
  }
  return map;
}

async function getTiendasAsignadas(promotorId) {
  const rows = await getSheetValues("ASIGNACIONES!A2:D");
  return unique(rows.filter((row) => norm(row[0]) === promotorId && isTrue(row[3] ?? "TRUE")).map((row) => norm(row[1])));
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

async function getVisitasAllByPromotor(promotorId) {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${String.fromCharCode(64 + header.length)}`);
  return rows.filter((row) => norm(row[1]) === promotorId).map((row, idx) => parseVisitRow(row, idx, header));
}

async function getVisitasToday(promotorId) {
  const today = todayISO();
  const all = await getVisitasAllByPromotor(promotorId);
  return all.filter((visit) => visit.fecha === today);
}

async function getOpenVisitsToday(promotorId) {
  const visits = await getVisitasToday(promotorId);
  return visits.filter((visit) => !visit.hora_fin);
}

async function getVisitById(visitaId) {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${String.fromCharCode(64 + header.length)}`);
  for (let i = 0; i < rows.length; i += 1) {
    const parsed = parseVisitRow(rows[i], i, header);
    if (parsed.visita_id === visitaId) return parsed;
  }
  return null;
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

async function createVisitWithGeofence(promotorId, tiendaId, baseNotes, geofence) {
  const header = await getVisitsHeader();
  const visitaId = `V-${Date.now()}`;
  const payload = {
    visita_id: visitaId,
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
  const lastCol = String.fromCharCode(64 + header.length);
  await appendSheetValues(`VISITAS!A2:${lastCol}`, [row]);
  return visitaId;
}

async function closeVisitWithGeofence(visit, closeNotes, geofence) {
  const header = await getVisitsHeader();
  const payload = {
    ...visit,
    hora_fin: nowISO(),
    notas: closeNotes || visit.notas,
    estado_visita:
      geofence.resultado === "FUERA_DE_GEOCERCA" || upper(visit.resultado_geocerca_entrada) === "FUERA_DE_GEOCERCA"
        ? "CERRADA_CON_ALERTA"
        : "CERRADA",
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
  const lastCol = String.fromCharCode(64 + header.length);
  await updateSheetValues(`VISITAS!A${visit.rowIndex}:${lastCol}${visit.rowIndex}`, [row]);
}

async function createAlert(payload) {
  try {
    const header = await getSheetHeader("ALERTAS");
    if (!header.length) return false;
    const row = new Array(header.length).fill("");
    header.forEach((name, idx) => {
      switch (name) {
        case "alerta_id": row[idx] = payload.alerta_id; break;
        case "fecha_hora": row[idx] = payload.fecha_hora; break;
        case "promotor_id": row[idx] = payload.promotor_id; break;
        case "visita_id": row[idx] = payload.visita_id; break;
        case "evidencia_id": row[idx] = payload.evidencia_id || ""; break;
        case "tipo_alerta": row[idx] = payload.tipo_alerta; break;
        case "severidad": row[idx] = payload.severidad; break;
        case "descripcion": row[idx] = payload.descripcion; break;
        case "status": row[idx] = payload.status || "ABIERTA"; break;
        default: break;
      }
    });
    const lastCol = String.fromCharCode(64 + header.length);
    await appendSheetValues(`ALERTAS!A2:${lastCol}`, [row]);
    return true;
  } catch (error) {
    console.warn("No se pudo guardar ALERTA", error?.message || error);
    return false;
  }
}

function parseEvidenceRow(row, idx) {
  return {
    rowIndex: idx + 2,
    evidencia_id: norm(row[0]),
    external_id: norm(row[1]),
    fecha_hora: norm(row[2]),
    tipo_evento: norm(row[3]),
    origen: norm(row[4]),
    jornada_id: norm(row[5]),
    visita_id: norm(row[6]),
    url_foto: norm(row[7]),
    lat: norm(row[8]),
    lon: norm(row[9]),
    resultado_ai: norm(row[10]),
    score_confianza: norm(row[11]),
    riesgo: upper(row[12] || "BAJO"),
    marca_id: norm(row[13]),
    producto_id: norm(row[14]),
    tipo_evidencia: norm(row[15]),
    descripcion: norm(row[16]),
    status: norm(row[17]) || "ACTIVA",
    note: norm(row[18]),
    fase: norm(row[19]),
    foto_nombre: norm(row[20]),
    accuracy: norm(row[21]),
  };
}

async function getEvidenceById(evidenciaId) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  for (let i = 0; i < rows.length; i += 1) {
    const parsed = parseEvidenceRow(rows[i], i);
    if (parsed.evidencia_id === evidenciaId) return parsed;
  }
  return null;
}

async function getEvidenciasTodayByExternalId(externalId) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  const today = todayISO();
  return rows
    .map(parseEvidenceRow)
    .filter((row) => row.external_id === externalId && row.fecha_hora && ymdInTZ(new Date(row.fecha_hora), APP_TZ) === today);
}

async function getEvidenciasByVisitId(visitaId) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  return rows.map(parseEvidenceRow).filter((row) => row.visita_id === visitaId);
}

async function registrarEvidencia(payload) {
  const photoInfo = normalizePhotoForSheet(payload.url_foto);
  const safeNote = mergeOverflowNote(payload.note || "", photoInfo);
  await appendSheetValues("EVIDENCIAS!A2:V", [[
    fitCell(payload.evidencia_id),
    fitCell(payload.external_id || ""),
    fitCell(payload.fecha_hora || nowISO()),
    fitCell(payload.tipo_evento),
    fitCell(payload.origen),
    fitCell(payload.jornada_id || ""),
    fitCell(payload.visita_id || ""),
    photoInfo.value,
    fitCell(payload.lat || ""),
    fitCell(payload.lon || ""),
    fitCell(payload.resultado_ai || "Pendiente / demo"),
    fitCell(payload.score_confianza || 0.9),
    fitCell(payload.riesgo || "BAJO"),
    fitCell(payload.marca_id || ""),
    fitCell(payload.producto_id || ""),
    fitCell(payload.tipo_evidencia || ""),
    fitCell(payload.descripcion || ""),
    fitCell(payload.status || "ACTIVA"),
    safeNote,
    fitCell(payload.fase || ""),
    fitCell(payload.foto_nombre || ""),
    fitCell(payload.accuracy || ""),
  ]]);
  return { photoOverflow: photoInfo.overflow, originalPhotoLength: photoInfo.originalLength };
}

async function updateEvidenceRow(evidence, patch = {}) {
  const photoInfo = normalizePhotoForSheet(patch.url_foto ?? evidence.url_foto);
  const noteValue = mergeOverflowNote(patch.note ?? evidence.note, photoInfo);
  await updateSheetValues(`EVIDENCIAS!A${evidence.rowIndex}:V${evidence.rowIndex}`, [[
    fitCell(patch.evidencia_id ?? evidence.evidencia_id),
    fitCell(patch.external_id ?? evidence.external_id),
    fitCell(patch.fecha_hora ?? evidence.fecha_hora),
    fitCell(patch.tipo_evento ?? evidence.tipo_evento),
    fitCell(patch.origen ?? evidence.origen),
    fitCell(patch.jornada_id ?? evidence.jornada_id),
    fitCell(patch.visita_id ?? evidence.visita_id),
    photoInfo.value,
    fitCell(patch.lat ?? evidence.lat),
    fitCell(patch.lon ?? evidence.lon),
    fitCell(patch.resultado_ai ?? evidence.resultado_ai),
    fitCell(patch.score_confianza ?? evidence.score_confianza),
    fitCell(patch.riesgo ?? evidence.riesgo),
    fitCell(patch.marca_id ?? evidence.marca_id),
    fitCell(patch.producto_id ?? evidence.producto_id),
    fitCell(patch.tipo_evidencia ?? evidence.tipo_evidencia),
    fitCell(patch.descripcion ?? evidence.descripcion),
    fitCell(patch.status ?? evidence.status),
    noteValue,
    fitCell(patch.fase ?? evidence.fase),
    fitCell(patch.foto_nombre ?? evidence.foto_nombre),
    fitCell(patch.accuracy ?? evidence.accuracy),
  ]]);
  return { photoOverflow: photoInfo.overflow, originalPhotoLength: photoInfo.originalLength };
}

async function getMarcasActivas() {
  const rows = await getSheetValues("MARCAS!A2:C");
  return rows
    .filter((row) => norm(row[0]) && isTrue(row[2]))
    .map((row) => ({ marca_id: norm(row[0]), marca_nombre: norm(row[1]) }))
    .sort((a, b) => a.marca_nombre.localeCompare(b.marca_nombre));
}

async function getMarcaMap() {
  const rows = await getSheetValues("MARCAS!A2:C");
  const map = {};
  rows.forEach((row) => {
    const id = norm(row[0]);
    if (!id) return;
    map[id] = { marca_id: id, marca_nombre: norm(row[1]), activa: isTrue(row[2]) };
  });
  return map;
}

async function resolveMarcaIdByName(marcaNombre) {
  const target = upper(marcaNombre);
  if (!target) return "";
  const rows = await getSheetValues("MARCAS!A2:C");
  for (const row of rows) {
    if (upper(row[1]) === target) return norm(row[0]);
  }
  return "";
}

async function getReglasPorMarca(marcaId) {
  const rows = await getSheetValues("REGLAS_EVIDENCIA!A2:E");
  return rows
    .filter((row) => norm(row[0]) === marcaId && isTrue(row[4] ?? "TRUE"))
    .map((row) => ({
      marca_id: marcaId,
      tipo_evidencia: norm(row[1]),
      fotos_requeridas: safeInt(row[2], 1),
      requiere_antes_despues: isTrue(row[3]),
    }));
}

async function getAllVisitsMap() {
  const header = await getVisitsHeader();
  const rows = await getSheetValues(`VISITAS!A2:${String.fromCharCode(64 + header.length)}`);
  const map = {};
  rows.forEach((row, idx) => {
    const parsed = parseVisitRow(row, idx, header);
    if (parsed.visita_id) map[parsed.visita_id] = parsed;
  });
  return map;
}

function buildEvidenceView(item, marcaMap, visitMap, tiendaMap) {
  const visit = visitMap[item.visita_id];
  const tienda = visit ? tiendaMap[visit.tienda_id] : null;
  return {
    ...item,
    marca_nombre: marcaMap[item.marca_id]?.marca_nombre || item.marca_id,
    fecha_hora_fmt: fmtDateTimeTZ(item.fecha_hora),
    tienda_id: visit?.tienda_id || "",
    tienda_nombre: tienda?.nombre_tienda || visit?.tienda_id || "",
  };
}

const STATE_MENU = "MENU";
const STATE_SUP_MENU = "SUP_MENU";
const STATE_CLIENT_MENU = "CLIENT_MENU";

async function findSessionRow(externalId) {
  const rows = await getSheetValues("SESIONES!A2:C");
  for (let i = 0; i < rows.length; i += 1) {
    if (norm(rows[i][0]) === externalId) {
      let data_json = {};
      try {
        data_json = rows[i][2] ? JSON.parse(rows[i][2]) : {};
      } catch {
        data_json = {};
      }
      return {
        rowIndex: i + 2,
        estado_actual: norm(rows[i][1]) || STATE_MENU,
        data_json,
      };
    }
  }
  return null;
}

async function getSession(externalId) {
  const found = await findSessionRow(externalId);
  if (found) return found;
  await appendSheetValues("SESIONES!A2:C", [[externalId, STATE_MENU, "{}"]]);
  return findSessionRow(externalId);
}

async function setSession(externalId, estado_actual, data_json = {}) {
  const found = await findSessionRow(externalId);
  const body = JSON.stringify(data_json || {});
  if (!found) {
    await appendSheetValues("SESIONES!A2:C", [[externalId, estado_actual, body]]);
    return;
  }
  await updateSheetValues(`SESIONES!A${found.rowIndex}:C${found.rowIndex}`, [[externalId, estado_actual, body]]);
}

function parseTelegramUpdate(update) {
  const message = update.message || update.edited_message || null;
  const callback = update.callback_query || null;

  if (callback) {
    const chatId = callback.message?.chat?.id;
    const senderId = callback.from?.id;
    return {
      updateType: "callback_query",
      updateId: update.update_id,
      chatId,
      senderId,
      senderHandle: buildTelegramExternalId(senderId),
      text: norm(callback.data),
      callbackQueryId: callback.id,
      raw: update,
    };
  }

  if (!message) {
    return {
      updateType: "unsupported",
      updateId: update.update_id,
      chatId: null,
      senderId: null,
      senderHandle: "",
      text: "",
      raw: update,
    };
  }

  const senderId = message.from?.id;
  return {
    updateType: "message",
    updateId: update.update_id,
    chatId: message.chat?.id,
    senderId,
    senderHandle: buildTelegramExternalId(senderId),
    text: norm(message.text || message.caption || message.web_app_data?.data || ""),
    raw: update,
  };
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

async function sendTelegramText(chatId, text, options = {}) {
  return telegramApi("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: true,
    reply_markup: options.reply_markup,
  });
}

async function sendTelegramPhoto(chatId, fileIdOrUrl, caption = "", options = {}) {
  return telegramApi("sendPhoto", {
    chat_id: chatId,
    photo: fileIdOrUrl,
    caption,
    parse_mode: "Markdown",
    reply_markup: options.reply_markup,
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
      [{ text: "Abrir Mini App", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }],
    ],
  };
}

function buildSupervisorKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Asistencias HOY", callback_data: "SUP:ASIS" }, { text: "Alertas", callback_data: "SUP:ALERTS" }],
      [{ text: "Evidencias", callback_data: "SUP:EVID" }, { text: "Resumen", callback_data: "SUP:SUMMARY" }],
      [{ text: "Abrir panel", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }],
    ],
  };
}

function buildClienteKeyboard() {
  return {
    inline_keyboard: [[{ text: "Abrir expediente", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
  };
}

function promotorMenuText() {
  return [
    "👋 *Promobolsillo+ Telegram*",
    "",
    "1️⃣ Asistencia",
    "2️⃣ Evidencias",
    "3️⃣ Mis evidencias",
    "4️⃣ Resumen del día",
    "",
    "Escribe /menu cuando quieras volver aquí.",
  ].join("\n");
}

async function handlePromotorChannel(actor, incoming, session) {
  const text = norm(incoming.text).toLowerCase();
  if (text === "/start" || text === "/menu" || text === "menu") {
    await setSession(actor.profile.external_id, STATE_MENU, session.data_json || {});
    return { type: "text", text: promotorMenuText(), reply_markup: buildPromotorKeyboard() };
  }

  if (incoming.updateType === "callback_query") {
    if (text === "promo:asis") {
      const visits = await getVisitasToday(actor.profile.promotor_id);
      const open = visits.filter((v) => !v.hora_fin).length;
      return {
        type: "text",
        text: `🕒 *Asistencia*\n\nVisitas hoy: *${visits.length}*\nAbiertas: *${open}*\n\nAbre la Mini App para registrar entrada/salida con geocerca, selfie y ubicación.`,
        reply_markup: { inline_keyboard: [[{ text: "Abrir asistencia", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]] },
      };
    }
    if (text === "promo:evid") {
      return {
        type: "text",
        text: "📸 *Evidencias*\n\nCaptura fotos estructuradas por marca, tipo y fase desde la Mini App.",
        reply_markup: { inline_keyboard: [[{ text: "Abrir evidencias", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]] },
      };
    }
    if (text === "promo:my_evid") {
      const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
      return {
        type: "text",
        text: `📚 *Mis evidencias de hoy*: *${evidencias.length}*\n\nRevisa filtros, notas y reemplazos dentro de la Mini App.`,
        reply_markup: { inline_keyboard: [[{ text: "Abrir galería", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]] },
      };
    }
    if (text === "promo:summary") {
      const visits = await getVisitasToday(actor.profile.promotor_id);
      const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
      const outside = visits.filter((v) => upper(v.resultado_geocerca_entrada) === "FUERA_DE_GEOCERCA" || upper(v.resultado_geocerca_salida) === "FUERA_DE_GEOCERCA").length;
      return {
        type: "text",
        text: `📊 *Resumen del día* (${todayISO()})\n\n🏬 Visitas: *${visits.length}*\n📸 Evidencias: *${evidencias.length}*\n🚨 Con alerta geocerca: *${outside}*`,
      };
    }
  }

  return { type: "text", text: promotorMenuText(), reply_markup: buildPromotorKeyboard() };
}

async function handleSupervisorChannel(actor, incoming, session) {
  const text = norm(incoming.text).toLowerCase();
  if (text === "/start" || text === "/menu" || text === "/sup" || text === "sup") {
    await setSession(actor.profile.external_id, STATE_SUP_MENU, session.data_json || {});
    return { type: "text", text: `👋 Hola, *${actor.profile.nombre}* (Supervisor).\n\nTu panel está disponible en Telegram.`, reply_markup: buildSupervisorKeyboard() };
  }

  if (incoming.updateType === "callback_query") {
    if (text === "sup:asis") {
      const team = await getPromotoresDeSupervisor(actor.profile.external_id);
      let totalVisits = 0;
      let openVisits = 0;
      for (const prom of team) {
        const visits = await getVisitasToday(prom.promotor_id);
        totalVisits += visits.length;
        openVisits += visits.filter((v) => !v.hora_fin).length;
      }
      return { type: "text", text: `🕒 *Asistencias HOY*\n\nPromotores: *${team.length}*\nVisitas: *${totalVisits}*\nAbiertas: *${openVisits}*`, reply_markup: buildSupervisorKeyboard() };
    }
    if (text === "sup:alerts") {
      const rows = await getSheetValues("ALERTAS!A2:H");
      const openAlerts = rows.filter((row) => upper(row[7] || "ABIERTA") === "ABIERTA").length;
      return { type: "text", text: `🚨 *Alertas abiertas*: *${openAlerts}*\n\nAbre el panel para revisar detalles.`, reply_markup: buildSupervisorKeyboard() };
    }
    if (text === "sup:evid") {
      return { type: "text", text: "📸 *Evidencias*\n\nLa revisión visual y el filtrado se realizan desde la Mini App.", reply_markup: buildSupervisorKeyboard() };
    }
    if (text === "sup:summary") {
      const rows = await getSheetValues("ALERTAS!A2:H");
      const today = todayISO();
      const todayAlerts = rows.filter((row) => row[1] && ymdInTZ(new Date(row[1]), APP_TZ) === today).length;
      return { type: "text", text: `📊 *Resumen Supervisor*\n\nAlertas hoy: *${todayAlerts}*`, reply_markup: buildSupervisorKeyboard() };
    }
  }

  return { type: "text", text: `👋 Hola, *${actor.profile.nombre}* (Supervisor).\n\nUsa el menú para continuar.`, reply_markup: buildSupervisorKeyboard() };
}

async function handleClienteChannel(_actor, incoming, session) {
  const text = norm(incoming.text).toLowerCase();
  if (text === "/start" || text === "/menu" || text === "menu") {
    await setSession(session?.external_id || "cliente", STATE_CLIENT_MENU, {});
    return {
      type: "text",
      text: "👋 *Canal cliente*\n\nAquí recibirás acceso al expediente y evidencia publicada.",
      reply_markup: buildClienteKeyboard(),
    };
  }
  return { type: "text", text: "Escribe /menu para abrir tu panel cliente.", reply_markup: buildClienteKeyboard() };
}

async function routeIncoming(incoming) {
  const actor = await resolveActor(incoming.senderHandle);
  const session = await getSession(actor.profile.external_id);
  if (actor.role === "promotor") return handlePromotorChannel(actor, incoming, session);
  if (actor.role === "supervisor") return handleSupervisorChannel(actor, incoming, session);
  return handleClienteChannel(actor, incoming, session);
}

async function respondToTelegram(incoming, response) {
  if (!response || !incoming.chatId) return;
  if (incoming.callbackQueryId) await answerCallbackQuery(incoming.callbackQueryId, "OK");
  if (response.type === "photo") {
    await sendTelegramPhoto(incoming.chatId, response.photo, response.caption || "", { reply_markup: response.reply_markup });
    return;
  }
  await sendTelegramText(incoming.chatId, response.text || "OK", { reply_markup: response.reply_markup });
}

const userLocks = new Map();

async function withUserLock(key, fn) {
  const previous = userLocks.get(key) || Promise.resolve();
  let release;
  const current = new Promise((resolve) => {
    release = resolve;
  });
  userLocks.set(key, previous.then(() => current).catch(() => current));
  await previous;
  try {
    return await fn();
  } finally {
    release();
    setTimeout(() => {
      if (userLocks.get(key) === current) userLocks.delete(key);
    }, 5000);
  }
}

function verifyTelegramWebAppInitData(initData) {
  if (!initData || !TELEGRAM_BOT_TOKEN) return false;
  const params = new URLSearchParams(initData);
  const hash = params.get("hash");
  if (!hash) return false;
  params.delete("hash");
  const dataCheckString = [...params.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
  const secretKey = crypto.createHmac("sha256", "WebAppData").update(TELEGRAM_BOT_TOKEN).digest();
  const expectedHash = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex");
  return expectedHash === hash;
}

function decodeTelegramWebAppUser(initData) {
  const params = new URLSearchParams(initData);
  const userRaw = params.get("user");
  return userRaw ? JSON.parse(userRaw) : null;
}

async function resolveActorFromInitData(initData) {
  if (!verifyTelegramWebAppInitData(initData)) return null;
  const user = decodeTelegramWebAppUser(initData);
  if (!user?.id) return null;
  const externalId = buildTelegramExternalId(user.id);
  const actor = await resolveActor(externalId);
  return { ...actor, telegramUser: user, externalId };
}

async function requireMiniAppActor(req, res, next) {
  try {
    const initData = req.body?.initData || req.headers["x-telegram-init-data"] || req.query?.initData;
    const actor = await resolveActorFromInitData(initData);
    if (!actor) return res.status(401).json({ ok: false, error: "initData inválido" });
    req.miniappActor = actor;
    next();
  } catch (error) {
    console.error("MiniApp auth error", error);
    res.status(500).json({ ok: false, error: "miniapp_auth_failed" });
  }
}

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

async function buildGeofenceResult(tiendaId, lat, lon, accuracy) {
  const tiendaMap = await getTiendaMap();
  const policy = await getPolicyValidation();
  const tienda = tiendaMap[tiendaId];
  if (!tienda) {
    return {
      resultado: "TIENDA_SIN_COORDENADAS",
      severidad: "ALTA",
      distancia_m: "",
      accuracy_m: accuracy || "",
      lat_tienda: "",
      lon_tienda: "",
      radio_tienda_m: "",
    };
  }

  if (!Number.isFinite(safeNum(lat, NaN)) || !Number.isFinite(safeNum(lon, NaN))) {
    return {
      resultado: policy.sin_gps || "SIN_DATOS_GPS",
      severidad: "MEDIA",
      distancia_m: "",
      accuracy_m: accuracy || "",
      lat_tienda: tienda.lat,
      lon_tienda: tienda.lon,
      radio_tienda_m: tienda.radio_m || policy.radio_estandar,
    };
  }

  const radius = tienda.radio_m || policy.radio_estandar;
  const distance = haversineDistanceMeters(safeNum(lat), safeNum(lon), safeNum(tienda.lat), safeNum(tienda.lon));
  const classified = classifyGeofence(distance, radius, safeNum(accuracy, 0), policy.sin_gps);
  return {
    resultado: classified.result,
    severidad: classified.severity,
    distancia_m: distance,
    accuracy_m: safeNum(accuracy, 0),
    lat_tienda: tienda.lat,
    lon_tienda: tienda.lon,
    radio_tienda_m: radius,
  };
}

app.post("/miniapp/bootstrap", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  res.json({ ok: true, role: actor.role, profile: actor.profile, telegramUser: actor.telegramUser, serverTime: nowISO(), today: todayISO() });
}));

app.post("/miniapp/promotor/dashboard", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const tiendaMap = await getTiendaMap();
  const assignedStoreIds = await getTiendasAsignadas(actor.profile.promotor_id);
  const stores = assignedStoreIds.map((id) => tiendaMap[id]).filter(Boolean).map((store) => ({
    tienda_id: store.tienda_id,
    nombre_tienda: store.nombre_tienda,
    cadena: store.cadena,
  }));
  const visitsToday = await getVisitasToday(actor.profile.promotor_id);
  const openVisits = visitsToday.filter((visit) => !visit.hora_fin);
  const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
  res.json({
    ok: true,
    promotor: actor.profile,
    stores,
    visitsToday: visitsToday.map((visit) => ({ ...visit, tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id })),
    openVisits: openVisits.map((visit) => ({ ...visit, tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id })),
    summary: {
      assignedStores: stores.length,
      openVisits: openVisits.length,
      closedVisits: visitsToday.filter((visit) => visit.hora_fin).length,
      evidenciasHoy: evidencias.length,
    },
  });
}));

app.post("/miniapp/promotor/start-entry", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { tienda_id, lat = "", lon = "", accuracy = "", selfie_url = "", foto_data_url = "", foto_nombre = "", notas = "" } = req.body || {};
  if (!tienda_id) return res.status(400).json({ ok: false, error: "tienda_id requerido" });

  const existingOpen = await getOpenVisitsToday(actor.profile.promotor_id);
  const duplicated = existingOpen.find((visit) => visit.tienda_id === tienda_id);
  if (duplicated) return res.status(409).json({ ok: false, error: "ya_hay_visita_abierta", visita_id: duplicated.visita_id });

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
      tipo_alerta: "ASISTENCIA_FUERA_GEOCERCA_ENTRADA",
      severidad: "ALTA",
      descripcion: `Entrada fuera de geocerca en tienda ${tienda_id}. Distancia ${geo.distancia_m}m / Radio ${geo.radio_tienda_m}m`,
      status: "ABIERTA",
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
  });
}));

app.post("/miniapp/promotor/close-visit", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { visita_id, lat = "", lon = "", accuracy = "", selfie_url = "", foto_data_url = "", foto_nombre = "", notas = "" } = req.body || {};
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "visita_no_encontrada" });

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
      score_confianza: 0.92,
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
      tipo_alerta: "ASISTENCIA_FUERA_GEOCERCA_SALIDA",
      severidad: "ALTA",
      descripcion: `Salida fuera de geocerca en tienda ${visit.tienda_id}. Distancia ${geo.distancia_m}m / Radio ${geo.radio_tienda_m}m`,
      status: "ABIERTA",
    });
  }

  res.json({
    ok: true,
    visita_id,
    closed_at: nowISO(),
    warning,
    geofence: {
      result: geo.resultado,
      distance_m: geo.distancia_m,
      accuracy_m: geo.accuracy_m,
      radius_m: geo.radio_tienda_m,
    },
  });
}));

app.post("/miniapp/promotor/evidence-context", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { visita_id } = req.body || {};
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });
  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "visita_no_encontrada" });
  const marcas = await getMarcasActivas();
  const tiendaMap = await getTiendaMap();
  res.json({ ok: true, visita: { ...visit, tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id }, marcas });
}));

app.post("/miniapp/promotor/evidence-rules", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { marca_id, marca_nombre = "" } = req.body || {};
  let resolvedMarcaId = norm(marca_id);
  if (!resolvedMarcaId && marca_nombre) resolvedMarcaId = await resolveMarcaIdByName(marca_nombre);
  if (!resolvedMarcaId) return res.status(400).json({ ok: false, error: "marca_id requerido" });
  const reglas = await getReglasPorMarca(resolvedMarcaId);
  res.json({ ok: true, reglas });
}));

app.post("/miniapp/promotor/evidence-register", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { visita_id, marca_id = "", marca_nombre = "", tipo_evidencia, descripcion = "", fase = "NA", lat = "", lon = "", accuracy = "", fotos = [], foto_data_url = "", foto_nombre = "" } = req.body || {};
  if (!visita_id || !tipo_evidencia) return res.status(400).json({ ok: false, error: "payload_incompleto" });
  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) return res.status(404).json({ ok: false, error: "visita_no_encontrada" });

  let resolvedMarcaId = norm(marca_id);
  if (!resolvedMarcaId && marca_nombre) resolvedMarcaId = await resolveMarcaIdByName(marca_nombre);
  if (!resolvedMarcaId && marca_nombre) resolvedMarcaId = marca_nombre;

  const finalFotos = Array.isArray(fotos) && fotos.length ? fotos : (foto_data_url ? [{ dataUrl: foto_data_url, name: foto_nombre || "evidencia.jpg" }] : []);
  if (!finalFotos.length) return res.status(400).json({ ok: false, error: "fotos_requeridas" });

  if (resolvedMarcaId) {
    const reglas = await getReglasPorMarca(resolvedMarcaId);
    const rule = reglas.find((item) => upper(item.tipo_evidencia) === upper(tipo_evidencia));
    if (rule && finalFotos.length < rule.fotos_requeridas) {
      return res.status(400).json({ ok: false, error: "fotos_insuficientes", expected: rule.fotos_requeridas, received: finalFotos.length });
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
    });
    if (result.photoOverflow) photoOverflow = true;
    created.push(evidencia_id);
  }
  res.json({ ok: true, visita_id, created, count: created.length, warning: photoOverflow ? "evidence_photo_too_large_for_sheets" : "" });
}));

app.post("/miniapp/promotor/evidences-today", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const tiendaMap = await getTiendaMap();
  const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
  res.json({ ok: true, evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)) });
}));

app.post("/miniapp/promotor/evidence-note", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { evidencia_id, note = "" } = req.body || {};
  if (!evidencia_id) return res.status(400).json({ ok: false, error: "evidencia_id requerido" });
  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) return res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
  await updateEvidenceRow(evidence, { note });
  res.json({ ok: true, evidencia_id, note });
}));

app.post("/miniapp/promotor/cancel-evidence", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { evidencia_id, note = "" } = req.body || {};
  if (!evidencia_id) return res.status(400).json({ ok: false, error: "evidencia_id requerido" });
  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) return res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
  await updateEvidenceRow(evidence, { status: "ANULADA", note, resultado_ai: "ANULADA_MANUAL", riesgo: "BAJO" });
  res.json({ ok: true, evidencia_id, status: "ANULADA" });
}));

app.post("/miniapp/promotor/replace-evidence", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") return res.status(403).json({ ok: false, error: "solo_promotor" });
  const { evidencia_id, url_foto = "", foto_data_url = "", foto_nombre = "", resultado_ai = "", score_confianza = "", riesgo = "" } = req.body || {};
  if (!evidencia_id) return res.status(400).json({ ok: false, error: "evidencia_id requerido" });
  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) return res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
  const newPhotoUrl = url_foto || foto_data_url;
  if (!newPhotoUrl) return res.status(400).json({ ok: false, error: "foto_requerida" });
  const result = await updateEvidenceRow(evidence, {
    url_foto: newPhotoUrl,
    fecha_hora: nowISO(),
    foto_nombre: foto_nombre || evidence.foto_nombre,
    resultado_ai: resultado_ai || evidence.resultado_ai,
    score_confianza: score_confianza || evidence.score_confianza,
    riesgo: riesgo || evidence.riesgo,
  });
  res.json({ ok: true, evidencia_id, replaced: true, warning: result.photoOverflow ? "evidence_photo_too_large_for_sheets" : "" });
}));

app.post("/miniapp/supervisor/dashboard", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });
  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const summary = { promotores: team.length, visitasHoy: 0, abiertas: 0, evidenciasHoy: 0, alertas: 0 };
  for (const promotor of team) {
    const visits = await getVisitasToday(promotor.promotor_id);
    const evidencias = await getEvidenciasTodayByExternalId(promotor.external_id);
    summary.visitasHoy += visits.length;
    summary.abiertas += visits.filter((v) => !v.hora_fin).length;
    summary.evidenciasHoy += evidencias.length;
  }
  const alertRows = await getSheetValues("ALERTAS!A2:H");
  summary.alertas = alertRows.filter((row) => upper(row[7] || "ABIERTA") === "ABIERTA").length;
  res.json({ ok: true, supervisor: actor.profile, summary });
}));

app.post("/miniapp/supervisor/visit-expedient", requireMiniAppActor, asyncHandler(async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "supervisor") return res.status(403).json({ ok: false, error: "solo_supervisor" });
  const { visita_id } = req.body || {};
  if (!visita_id) return res.status(400).json({ ok: false, error: "visita_id requerido" });
  const visit = await getVisitById(visita_id);
  if (!visit) return res.status(404).json({ ok: false, error: "visita_no_encontrada" });
  const team = await getPromotoresDeSupervisor(actor.profile.external_id);
  const allowed = new Set(team.map((item) => item.promotor_id));
  if (!allowed.has(visit.promotor_id)) return res.status(403).json({ ok: false, error: "sin_acceso" });
  const tiendaMap = await getTiendaMap();
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const evidencias = await getEvidenciasByVisitId(visita_id);
  res.json({
    ok: true,
    visita: { ...visit, tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id },
    evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)),
  });
}));

app.post("/telegram/webhook", asyncHandler(async (req, res) => {
  if (TELEGRAM_WEBHOOK_SECRET) {
    const token = req.headers["x-telegram-bot-api-secret-token"];
    if (token !== TELEGRAM_WEBHOOK_SECRET) return res.status(401).json({ ok: false, error: "invalid_secret" });
  }

  const incoming = parseTelegramUpdate(req.body || {});
  if (!incoming.senderHandle) return res.json({ ok: true, ignored: true });

  await withUserLock(incoming.senderHandle, async () => {
    try {
      const response = await routeIncoming(incoming);
      await respondToTelegram(incoming, response);
    } catch (error) {
      console.error("telegram webhook error", error);
      if (incoming.chatId) {
        await sendTelegramText(incoming.chatId, "Ocurrió un error procesando tu mensaje. Intenta de nuevo 🙏");
      }
    }
  });

  res.json({ ok: true });
}));

app.post("/telegram/set-webhook", asyncHandler(async (req, res) => {
  const webhookUrl = `${buildBaseUrl(req)}/telegram/webhook`;
  const payload = { url: webhookUrl };
  if (TELEGRAM_WEBHOOK_SECRET) payload.secret_token = TELEGRAM_WEBHOOK_SECRET;
  const result = await telegramApi("setWebhook", payload);
  res.json({ ok: true, webhookUrl, result });
}));

app.get("/", (_req, res) => {
  res.send("Promobolsillo+ Telegram backend v3.7 full geocerca ✅");
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "promobolsillo-telegram", now: nowISO() });
});

app.use((error, req, res, _next) => {
  console.error("Unhandled route error", {
    path: req.path,
    method: req.method,
    message: error?.message || String(error),
    stack: error?.stack || "",
  });
  res.status(500).json({ ok: false, error: error?.message || "server_error" });
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED_REJECTION", reason);
});

process.on("uncaughtException", (error) => {
  console.error("UNCAUGHT_EXCEPTION", error);
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Promobolsillo+ Telegram escuchando en puerto ${PORT}`);
});
