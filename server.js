import express from "express";
import bodyParser from "body-parser";
import crypto from "crypto";
import { google } from "googleapis";

/**
 * Promobolsillo+ Telegram Backend v3
 *
 * Cierre largo del módulo promotor:
 * - Asistencia con foto + GPS en entrada/salida.
 * - Evidencias reales contra Sheets.
 * - Endpoints para crear, listar, anular, reemplazar y anotar evidencias.
 * - Compatibilidad con la Mini App actual y payloads más robustos.
 */

const {
  PORT = 10000,
  APP_TZ = "America/Mexico_City",
  SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON,
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_WEBHOOK_SECRET,
  PUBLIC_BASE_URL,
  MINIAPP_BASE_URL,
} = process.env;

if (!SHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON) {
  console.warn("⚠️ Falta SHEET_ID o GOOGLE_SERVICE_ACCOUNT_JSON.");
}
if (!TELEGRAM_BOT_TOKEN) {
  console.warn("⚠️ Falta TELEGRAM_BOT_TOKEN.");
}

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

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

function norm(value) {
  return (value || "").toString().trim();
}

function upper(value) {
  return norm(value).toUpperCase();
}

function safeInt(value, fallback = 0) {
  const parsed = parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function safeFloat(value, fallback = 0) {
  const parsed = parseFloat(value);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function isTrue(value) {
  const t = upper(value);
  return t === "TRUE" || t === "1" || t === "SI" || t === "SÍ";
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

function buildBaseUrl(req) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL.replace(/\/+$/, "");
  const proto = (req.headers["x-forwarded-proto"] || "https").toString();
  const host = (req.headers["x-forwarded-host"] || req.headers.host || "").toString();
  return `${proto}://${host}`.replace(/\/+$/, "");
}

function buildTelegramExternalId(telegramUserId) {
  return `telegram:${telegramUserId}`;
}

function nEmoji(index) {
  const arr = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"];
  return arr[index] || `${index + 1})`;
}

function unique(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function pickPhotoUrl(photoLike) {
  if (!photoLike) return "";
  if (typeof photoLike === "string") return photoLike;
  return norm(photoLike.url || photoLike.dataUrl || photoLike.file_id || photoLike.fileId);
}

function pickPhotoName(photoLike, fallback = "foto.jpg") {
  if (!photoLike || typeof photoLike === "string") return fallback;
  return norm(photoLike.name || photoLike.file_name) || fallback;
}

function normalizeEvidenceStatus(value) {
  const v = upper(value || "ACTIVA");
  if (v === "ANULADA" || v === "CANCELADA") return "ANULADA";
  return "ACTIVA";
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

const STATE_MENU = "MENU";
const STATE_SUP_MENU = "SUP_MENU";

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

async function getPromotorByExternalId(externalId) {
  const rows = await getSheetValues("PROMOTORES!A2:G");
  for (const row of rows) {
    if (norm(row[0]) === externalId) {
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
  const rows = await getSheetValues("SUPERVISORES!A2:F");
  for (const row of rows) {
    if (norm(row[0]) === externalId && isTrue(row[5])) {
      return {
        external_id: norm(row[0]),
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

async function getTiendaMap() {
  const rows = await getSheetValues("TIENDAS!A2:K");
  const map = {};
  for (const row of rows) {
    const tienda_id = norm(row[0]);
    if (!tienda_id) continue;
    map[tienda_id] = {
      tienda_id,
      nombre_tienda: norm(row[1]),
      cadena: norm(row[2]),
      ciudad: norm(row[3]),
      estado: norm(row[4]),
      activa: isTrue(row[5]),
      lat: norm(row[6]),
      lon: norm(row[7]),
      radio_m: safeInt(row[8], 0),
      cliente: norm(row[9]),
      zona: norm(row[10]),
    };
  }
  return map;
}

async function getTiendasAsignadas(promotor_id) {
  const rows = await getSheetValues("ASIGNACIONES!A2:D");
  return unique(
    rows
      .filter((row) => norm(row[0]) === promotor_id && isTrue(row[3] ?? "TRUE"))
      .map((row) => norm(row[1]))
  );
}

async function getVisitasAllByPromotor(promotor_id) {
  const rows = await getSheetValues("VISITAS!A2:G");
  return rows
    .filter((row) => norm(row[1]) === promotor_id)
    .map((row, idx) => ({
      rowIndex: idx + 2,
      visita_id: norm(row[0]),
      promotor_id: norm(row[1]),
      tienda_id: norm(row[2]),
      fecha: norm(row[3]),
      hora_inicio: norm(row[4]),
      hora_fin: norm(row[5]),
      notas: norm(row[6]),
    }));
}

async function getAllVisitsMap() {
  const rows = await getSheetValues("VISITAS!A2:G");
  const map = {};
  rows.forEach((row, idx) => {
    const visita_id = norm(row[0]);
    if (!visita_id) return;
    map[visita_id] = {
      rowIndex: idx + 2,
      visita_id,
      promotor_id: norm(row[1]),
      tienda_id: norm(row[2]),
      fecha: norm(row[3]),
      hora_inicio: norm(row[4]),
      hora_fin: norm(row[5]),
      notas: norm(row[6]),
    };
  });
  return map;
}

async function getVisitasToday(promotor_id) {
  const today = todayISO();
  const all = await getVisitasAllByPromotor(promotor_id);
  return all.filter((visit) => visit.fecha === today);
}

async function getOpenVisitsToday(promotor_id) {
  const visits = await getVisitasToday(promotor_id);
  return visits.filter((visit) => !visit.hora_fin);
}

async function getVisitById(visita_id) {
  const rows = await getSheetValues("VISITAS!A2:G");
  for (let i = 0; i < rows.length; i += 1) {
    if (norm(rows[i][0]) === visita_id) {
      return {
        rowIndex: i + 2,
        visita_id: norm(rows[i][0]),
        promotor_id: norm(rows[i][1]),
        tienda_id: norm(rows[i][2]),
        fecha: norm(rows[i][3]),
        hora_inicio: norm(rows[i][4]),
        hora_fin: norm(rows[i][5]),
        notas: norm(rows[i][6]),
      };
    }
  }
  return null;
}

async function createVisit(promotor_id, tienda_id, notas = "") {
  const visita_id = `V-${Date.now()}`;
  await appendSheetValues("VISITAS!A2:G", [[visita_id, promotor_id, tienda_id, todayISO(), nowISO(), "", notas]]);
  return visita_id;
}

async function closeVisitById(visita_id, notas = "") {
  const visit = await getVisitById(visita_id);
  if (!visit) return false;
  await updateSheetValues(`VISITAS!F${visit.rowIndex}:G${visit.rowIndex}`, [[nowISO(), notas]]);
  return true;
}

async function getMarcasActivas() {
  const rows = await getSheetValues("MARCAS!A2:D");
  const out = [];
  for (const row of rows) {
    const marca_id = norm(row[0]);
    if (!marca_id) continue;
    if (!isTrue(row[3] ?? "TRUE")) continue;
    out.push({
      marca_id,
      marca_nombre: norm(row[2]),
    });
  }
  return out.sort((a, b) => a.marca_nombre.localeCompare(b.marca_nombre));
}

async function getMarcaMap() {
  const rows = await getSheetValues("MARCAS!A2:D");
  const map = {};
  for (const row of rows) {
    const marca_id = norm(row[0]);
    if (!marca_id) continue;
    map[marca_id] = {
      marca_id,
      marca_nombre: norm(row[2]),
      activa: isTrue(row[3] ?? "TRUE"),
    };
  }
  return map;
}

async function resolveMarcaIdByName(marcaNombre) {
  const target = upper(marcaNombre);
  if (!target) return "";
  const rows = await getSheetValues("MARCAS!A2:D");
  for (const row of rows) {
    if (upper(row[2]) === target) return norm(row[0]);
  }
  return "";
}

async function getReglasPorMarca(marca_id) {
  const rows = await getSheetValues("REGLAS_EVIDENCIA!A2:E");
  return rows
    .filter((row) => norm(row[0]) === marca_id && isTrue(row[4] ?? "TRUE"))
    .map((row) => ({
      marca_id,
      tipo_evidencia: norm(row[1]),
      fotos_requeridas: safeInt(row[2], 1),
      requiere_antes_despues: isTrue(row[3]),
    }));
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
    score_confianza: Number(row[11] || 0),
    riesgo: upper(row[12] || "BAJO"),
    marca_id: norm(row[13]),
    producto_id: norm(row[14]),
    tipo_evidencia: norm(row[15]),
    descripcion: norm(row[16]),
    status: normalizeEvidenceStatus(row[17] || "ACTIVA"),
    note: norm(row[18]),
    fase: norm(row[19]),
    foto_nombre: norm(row[20]),
    accuracy: norm(row[21]),
  };
}

async function getEvidenceById(evidencia_id) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  for (let i = 0; i < rows.length; i += 1) {
    if (norm(rows[i][0]) === evidencia_id) {
      return parseEvidenceRow(rows[i], i);
    }
  }
  return null;
}

async function getEvidenciasTodayByExternalId(externalId) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  const today = todayISO();
  return rows
    .map(parseEvidenceRow)
    .filter((row) => {
      if (row.external_id !== externalId) return false;
      if (!row.fecha_hora) return false;
      return ymdInTZ(new Date(row.fecha_hora), APP_TZ) === today;
    });
}

async function getEvidenciasByVisitId(visita_id) {
  const rows = await getSheetValues("EVIDENCIAS!A2:V");
  return rows
    .map(parseEvidenceRow)
    .filter((row) => row.visita_id === visita_id);
}

async function registrarEvidencia(payload) {
  await appendSheetValues("EVIDENCIAS!A2:V", [[
    payload.evidencia_id,
    payload.external_id || payload.telefono || "",
    payload.fecha_hora || nowISO(),
    payload.tipo_evento,
    payload.origen,
    payload.jornada_id || "",
    payload.visita_id || "",
    payload.url_foto || "",
    payload.lat || "",
    payload.lon || "",
    payload.resultado_ai || "Pendiente / demo",
    payload.score_confianza || 0.9,
    payload.riesgo || "BAJO",
    payload.marca_id || "",
    payload.producto_id || "",
    payload.tipo_evidencia || "",
    payload.descripcion || "",
    payload.status || "ACTIVA",
    payload.note || "",
    payload.fase || "",
    payload.foto_nombre || "",
    payload.accuracy || "",
  ]]);
}

async function updateEvidenceRow(evidence, patch = {}) {
  const values = [[
    patch.evidencia_id ?? evidence.evidencia_id,
    patch.external_id ?? evidence.external_id,
    patch.fecha_hora ?? evidence.fecha_hora,
    patch.tipo_evento ?? evidence.tipo_evento,
    patch.origen ?? evidence.origen,
    patch.jornada_id ?? evidence.jornada_id,
    patch.visita_id ?? evidence.visita_id,
    patch.url_foto ?? evidence.url_foto,
    patch.lat ?? evidence.lat,
    patch.lon ?? evidence.lon,
    patch.resultado_ai ?? evidence.resultado_ai,
    patch.score_confianza ?? evidence.score_confianza,
    patch.riesgo ?? evidence.riesgo,
    patch.marca_id ?? evidence.marca_id,
    patch.producto_id ?? evidence.producto_id,
    patch.tipo_evidencia ?? evidence.tipo_evidencia,
    patch.descripcion ?? evidence.descripcion,
    patch.status ?? evidence.status,
    patch.note ?? evidence.note,
    patch.fase ?? evidence.fase,
    patch.foto_nombre ?? evidence.foto_nombre,
    patch.accuracy ?? evidence.accuracy,
  ]];
  await updateSheetValues(`EVIDENCIAS!A${evidence.rowIndex}:V${evidence.rowIndex}`, values);
}

async function resolveActor(externalId) {
  const supervisor = await getSupervisorByExternalId(externalId);
  if (supervisor) return { role: "supervisor", profile: supervisor };

  const promotor = await getPromotorByExternalId(externalId);
  if (promotor) return { role: "promotor", profile: promotor };

  return {
    role: "cliente",
    profile: {
      external_id: externalId,
      nombre: "Cliente",
    },
  };
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
  return telegramApi("answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
  });
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
      [{ text: "Evidencias", callback_data: "SUP:EVID" }, { text: "Carrito", callback_data: "SUP:CART" }],
      [{ text: "Abrir panel", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }],
    ],
  };
}

function buildClienteKeyboard() {
  return {
    inline_keyboard: [[{ text: "Abrir expediente", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
  };
}

function mainMenu(hasPending = false) {
  const extra = hasPending ? `\n*${nEmoji(5)}* Continuar evidencia pendiente ⏯️` : "";
  return (
    "👋 *Promobolsillo+ Telegram*\n\n" +
    `*${nEmoji(0)}* Asistencia\n` +
    `*${nEmoji(1)}* Evidencias\n` +
    `*${nEmoji(2)}* Mis evidencias\n` +
    `*${nEmoji(3)}* Resumen del día\n` +
    `*${nEmoji(4)}* Ayuda\n` +
    extra +
    "\n\nAtajos: /menu /activas /continuar /ayuda"
  );
}

async function handlePromotorChannel(actor, incoming, session) {
  const text = norm(incoming.text).toLowerCase();
  const data = session.data_json || {};

  if (text === "/start" || text === "/menu" || text === "menu") {
    await setSession(actor.profile.external_id, STATE_MENU, data);
    return {
      type: "text",
      text: mainMenu(Boolean(data?._pending_evid)),
      reply_markup: buildPromotorKeyboard(),
    };
  }

  if (incoming.updateType === "callback_query") {
    if (text === "promo:asis") {
      const openVisits = await getOpenVisitsToday(actor.profile.promotor_id);
      return {
        type: "text",
        text:
          `🕒 *Asistencia*\n\n` +
          `Visitas abiertas HOY: *${openVisits.length}*\n` +
          "Abre la Mini App para registrar entrada/salida con foto, GPS y validación guiada.",
        reply_markup: {
          inline_keyboard: [[{ text: "Abrir asistencia", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
        },
      };
    }

    if (text === "promo:evid") {
      return {
        type: "text",
        text:
          "📸 *Evidencias*\n\n" +
          "La captura fuerte vive en la Mini App para controlar tamaño, peso y orden de envío.",
        reply_markup: {
          inline_keyboard: [[{ text: "Abrir captura", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
        },
      };
    }

    if (text === "promo:my_evid") {
      const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
      return {
        type: "text",
        text: `📚 *Mis evidencias de hoy*: *${evidencias.length}*\n\nAbre la Mini App para ver galería, reemplazar o anular.`,
        reply_markup: {
          inline_keyboard: [[{ text: "Abrir galería", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
        },
      };
    }

    if (text === "promo:summary") {
      const visits = await getVisitasToday(actor.profile.promotor_id);
      const open = visits.filter((v) => !v.hora_fin).length;
      const closed = visits.filter((v) => v.hora_fin).length;
      const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);
      return {
        type: "text",
        text:
          `📊 *Resumen del día* (${todayISO()})\n\n` +
          `🏬 Visitas: *${visits.length}*\n` +
          `🟢 Abiertas: *${open}*\n` +
          `✅ Cerradas: *${closed}*\n` +
          `📸 Evidencias: *${evidencias.length}*`,
      };
    }
  }

  return {
    type: "text",
    text: mainMenu(Boolean(data?._pending_evid)),
    reply_markup: buildPromotorKeyboard(),
  };
}

async function handleSupervisorChannel(actor, incoming, session) {
  const text = norm(incoming.text).toLowerCase();

  if (text === "/start" || text === "/menu" || text === "/sup" || text === "sup") {
    await setSession(actor.profile.external_id, STATE_SUP_MENU, session.data_json || {});
    return {
      type: "text",
      text: `👋 Hola, *${actor.profile.nombre}* (Supervisor).\n\nTu panel ya corre sobre Telegram.`,
      reply_markup: buildSupervisorKeyboard(),
    };
  }

  if (incoming.updateType === "callback_query") {
    switch (text) {
      case "sup:asis": {
        const equipo = await getPromotoresDeSupervisor(actor.profile.external_id);
        return {
          type: "text",
          text: `🧑‍🤝‍🧑 Equipo detectado: *${equipo.length}* promotor(es).\n\nAbre el panel para ver asistencias HOY y detalle por visita.`,
          reply_markup: {
            inline_keyboard: [[{ text: "Abrir panel supervisor", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
          },
        };
      }
      case "sup:alerts":
        return {
          type: "text",
          text: "🚦 Alertas del día listas para consultarse en el panel supervisor.",
          reply_markup: {
            inline_keyboard: [[{ text: "Abrir alertas", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
          },
        };
      case "sup:evid":
        return {
          type: "text",
          text: "📷 Evidencias del equipo listas para revisión fotográfica dentro de la Mini App.",
          reply_markup: {
            inline_keyboard: [[{ text: "Abrir revisión", web_app: { url: MINIAPP_BASE_URL || "https://example.com" } }]],
          },
        };
      case "sup:cart":
        return {
          type: "text",
          text: "🛒 El carrito ya se puede modelar desde el panel supervisor y luego publicar al cliente Telegram.",
        };
      default:
        break;
    }
  }

  return {
    type: "text",
    text: `👋 Hola, *${actor.profile.nombre}* (Supervisor).\n\nUsa el menú para continuar.`,
    reply_markup: buildSupervisorKeyboard(),
  };
}

async function handleClienteChannel(_actor, incoming) {
  const text = norm(incoming.text).toLowerCase();

  if (text === "/start" || text === "/menu" || text === "menu") {
    return {
      type: "text",
      text:
        "👋 *Canal cliente*\n\n" +
        "Aquí recibirás visitas aprobadas, fotos clave y acceso al expediente completo dentro de Telegram.",
      reply_markup: buildClienteKeyboard(),
    };
  }

  return {
    type: "text",
    text: "Escribe /menu para abrir tu panel cliente.",
    reply_markup: buildClienteKeyboard(),
  };
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

  if (incoming.callbackQueryId) {
    await answerCallbackQuery(incoming.callbackQueryId, "OK");
  }

  if (response.type === "photo") {
    await sendTelegramPhoto(incoming.chatId, response.photo, response.caption || "", {
      reply_markup: response.reply_markup,
    });
    return;
  }

  await sendTelegramText(incoming.chatId, response.text || "OK", {
    reply_markup: response.reply_markup,
  });
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

    console.log("MiniApp auth start", {
      origin: req.headers.origin || "",
      referer: req.headers.referer || "",
      hasInitData: Boolean(initData),
      initDataLength: initData ? String(initData).length : 0,
    });

    const actor = await resolveActorFromInitData(initData);

    if (!actor) {
      console.warn("MiniApp auth rejected", {
        origin: req.headers.origin || "",
        referer: req.headers.referer || "",
        hasInitData: Boolean(initData),
      });
      res.status(401).json({ ok: false, error: "initData inválido" });
      return;
    }

    console.log("MiniApp auth ok", {
      role: actor.role,
      externalId: actor.externalId,
      telegramUserId: actor.telegramUser?.id || null,
    });

    req.miniappActor = actor;
    next();
  } catch (error) {
    console.error("MiniApp auth error", error);
    res.status(500).json({ ok: false, error: "miniapp_auth_failed" });
  }
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

app.post("/miniapp/bootstrap", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;

  console.log("miniapp/bootstrap ok", {
    role: actor.role,
    externalId: actor.externalId,
    telegramUserId: actor.telegramUser?.id || null,
  });

  res.json({
    ok: true,
    role: actor.role,
    profile: actor.profile,
    telegramUser: actor.telegramUser,
    serverTime: nowISO(),
    today: todayISO(),
  });
});

app.post("/miniapp/promotor/dashboard", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const tiendaMap = await getTiendaMap();
  const assignedStoreIds = await getTiendasAsignadas(actor.profile.promotor_id);
  const assignedStores = assignedStoreIds
    .map((id) => tiendaMap[id])
    .filter(Boolean)
    .map((store) => ({
      tienda_id: store.tienda_id,
      nombre_tienda: store.nombre_tienda,
      cadena: store.cadena,
      ciudad: store.ciudad,
      cliente: store.cliente,
      zona: store.zona,
    }));

  const visitsToday = await getVisitasToday(actor.profile.promotor_id);
  const openVisits = visitsToday.filter((visit) => !visit.hora_fin);
  const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);

  res.json({
    ok: true,
    promotor: actor.profile,
    stores: assignedStores,
    visitsToday: visitsToday.map((visit) => ({
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
    })),
    openVisits: openVisits.map((visit) => ({
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
    })),
    summary: {
      assignedStores: assignedStores.length,
      openVisits: openVisits.length,
      evidenciasHoy: evidencias.length,
      closedVisits: visitsToday.filter((visit) => visit.hora_fin).length,
    },
  });
});

app.post("/miniapp/promotor/start-entry", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

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

  if (!tienda_id) {
    res.status(400).json({ ok: false, error: "tienda_id requerido" });
    return;
  }

  const existingOpen = await getOpenVisitsToday(actor.profile.promotor_id);
  const duplicated = existingOpen.find((visit) => visit.tienda_id === tienda_id);
  if (duplicated) {
    res.status(409).json({ ok: false, error: "ya_hay_visita_abierta", visita_id: duplicated.visita_id });
    return;
  }

  const visita_id = await createVisit(actor.profile.promotor_id, tienda_id, notas);
  const photoUrl = selfie_url || foto_data_url || "";

  if (photoUrl || lat || lon) {
    await registrarEvidencia({
      evidencia_id: `EV-${Date.now()}-ASIS-IN`,
      external_id: actor.profile.external_id,
      tipo_evento: "ASISTENCIA_ENTRADA",
      origen: "ASISTENCIA",
      visita_id,
      url_foto: photoUrl,
      lat,
      lon,
      tipo_evidencia: "ASISTENCIA",
      descripcion: "[TELEGRAM_MINIAPP_ENTRADA]",
      riesgo: "BAJO",
      score_confianza: 0.93,
      resultado_ai: "Entrada validada (demo)",
      status: "ACTIVA",
      note: "",
      fase: "NA",
      foto_nombre,
      accuracy,
    });
  }

  const storeMap = await getTiendaMap();
  res.json({
    ok: true,
    visita_id,
    tienda_id,
    tienda_nombre: storeMap[tienda_id]?.nombre_tienda || tienda_id,
    started_at: nowISO(),
  });
});

app.post("/miniapp/promotor/close-visit", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const {
    visita_id,
    lat = "",
    lon = "",
    accuracy = "",
    selfie_url = "",
    foto_data_url = "",
    foto_nombre = "",
    notas = "",
  } = req.body || {};

  if (!visita_id) {
    res.status(400).json({ ok: false, error: "visita_id requerido" });
    return;
  }

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) {
    res.status(404).json({ ok: false, error: "visita_no_encontrada" });
    return;
  }

  await closeVisitById(visita_id, notas);
  const photoUrl = selfie_url || foto_data_url || "";

  if (photoUrl || lat || lon) {
    await registrarEvidencia({
      evidencia_id: `EV-${Date.now()}-ASIS-OUT`,
      external_id: actor.profile.external_id,
      tipo_evento: "ASISTENCIA_SALIDA",
      origen: "ASISTENCIA",
      visita_id,
      url_foto: photoUrl,
      lat,
      lon,
      tipo_evidencia: "ASISTENCIA",
      descripcion: "[TELEGRAM_MINIAPP_SALIDA]",
      riesgo: "BAJO",
      score_confianza: 0.92,
      resultado_ai: "Salida validada (demo)",
      status: "ACTIVA",
      note: "",
      fase: "NA",
      foto_nombre,
      accuracy,
    });
  }

  res.json({ ok: true, visita_id, closed_at: nowISO() });
});

app.post("/miniapp/promotor/evidence-context", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const { visita_id } = req.body || {};
  if (!visita_id) {
    res.status(400).json({ ok: false, error: "visita_id requerido" });
    return;
  }

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) {
    res.status(404).json({ ok: false, error: "visita_no_encontrada" });
    return;
  }

  const marcas = await getMarcasActivas();
  const storeMap = await getTiendaMap();
  res.json({
    ok: true,
    visita: {
      ...visit,
      tienda_nombre: storeMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
    },
    marcas,
  });
});

app.post("/miniapp/promotor/evidence-rules", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const { marca_id, marca_nombre = "" } = req.body || {};
  let resolvedMarcaId = norm(marca_id);
  if (!resolvedMarcaId && marca_nombre) {
    resolvedMarcaId = await resolveMarcaIdByName(marca_nombre);
  }
  if (!resolvedMarcaId) {
    res.status(400).json({ ok: false, error: "marca_id requerido" });
    return;
  }

  const reglas = await getReglasPorMarca(resolvedMarcaId);
  res.json({ ok: true, reglas });
});

app.post("/miniapp/promotor/evidence-register", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const {
    visita_id,
    marca_id = "",
    marca_nombre = "",
    tipo_evidencia,
    fase = "NA",
    descripcion = "",
    lat = "",
    lon = "",
    accuracy = "",
    fotos = [],
    foto_data_url = "",
    foto_nombre = "",
  } = req.body || {};

  if (!visita_id || !tipo_evidencia) {
    res.status(400).json({ ok: false, error: "payload_incompleto" });
    return;
  }

  const visit = await getVisitById(visita_id);
  if (!visit || visit.promotor_id !== actor.profile.promotor_id) {
    res.status(404).json({ ok: false, error: "visita_no_encontrada" });
    return;
  }

  let resolvedMarcaId = norm(marca_id);
  if (!resolvedMarcaId && marca_nombre) {
    resolvedMarcaId = await resolveMarcaIdByName(marca_nombre);
  }
  if (!resolvedMarcaId && marca_nombre) {
    resolvedMarcaId = marca_nombre;
  }

  const finalFotos = Array.isArray(fotos) && fotos.length
    ? fotos
    : (foto_data_url ? [{ dataUrl: foto_data_url, name: foto_nombre || "evidencia.jpg" }] : []);

  if (!finalFotos.length) {
    res.status(400).json({ ok: false, error: "fotos_requeridas" });
    return;
  }

  if (resolvedMarcaId) {
    const reglas = await getReglasPorMarca(resolvedMarcaId);
    const reglaMatch = reglas.find((item) => upper(item.tipo_evidencia) === upper(tipo_evidencia));
    if (reglaMatch && finalFotos.length < reglaMatch.fotos_requeridas) {
      res.status(400).json({
        ok: false,
        error: "fotos_insuficientes",
        expected: reglaMatch.fotos_requeridas,
        received: finalFotos.length,
      });
      return;
    }
  }

  const tipo_evento = `EVIDENCIA_${upper(tipo_evidencia).replace(/\W+/g, "_")}`;
  const created = [];

  for (let i = 0; i < finalFotos.length; i += 1) {
    const foto = finalFotos[i];
    const evidencia_id = `EV-${Date.now()}-${i + 1}`;
    await registrarEvidencia({
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
      riesgo: foto.riesgo || "BAJO",
      score_confianza: foto.score_confianza || 0.9,
      resultado_ai: foto.resultado_ai || "Evidencia coherente (demo)",
      status: "ACTIVA",
      note: "",
      fase,
      foto_nombre: pickPhotoName(foto, foto_nombre || `evidencia_${i + 1}.jpg`),
    });
    created.push(evidencia_id);
  }

  res.json({
    ok: true,
    visita_id,
    created,
    count: created.length,
  });
});

app.post("/miniapp/promotor/evidences-today", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const tiendaMap = await getTiendaMap();
  const evidencias = await getEvidenciasTodayByExternalId(actor.profile.external_id);

  res.json({
    ok: true,
    evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)),
  });
});

app.post("/miniapp/promotor/evidence-note", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const { evidencia_id, note = "" } = req.body || {};
  if (!evidencia_id) {
    res.status(400).json({ ok: false, error: "evidencia_id requerido" });
    return;
  }

  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) {
    res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
    return;
  }

  await updateEvidenceRow(evidence, {
    note,
  });

  res.json({ ok: true, evidencia_id, note });
});

app.post("/miniapp/promotor/cancel-evidence", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const { evidencia_id, note = "" } = req.body || {};
  if (!evidencia_id) {
    res.status(400).json({ ok: false, error: "evidencia_id requerido" });
    return;
  }

  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) {
    res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
    return;
  }

  await updateEvidenceRow(evidence, {
    status: "ANULADA",
    note: note || evidence.note,
  });

  res.json({ ok: true, evidencia_id, status: "ANULADA" });
});

app.post("/miniapp/promotor/replace-evidence", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "promotor") {
    res.status(403).json({ ok: false, error: "solo_promotor" });
    return;
  }

  const {
    evidencia_id,
    url_foto = "",
    foto_data_url = "",
    foto_nombre = "",
    resultado_ai = "",
    score_confianza = "",
    riesgo = "",
  } = req.body || {};

  if (!evidencia_id) {
    res.status(400).json({ ok: false, error: "evidencia_id requerido" });
    return;
  }

  const evidence = await getEvidenceById(evidencia_id);
  if (!evidence || evidence.external_id !== actor.profile.external_id) {
    res.status(404).json({ ok: false, error: "evidencia_no_encontrada" });
    return;
  }

  const newPhotoUrl = url_foto || foto_data_url;
  if (!newPhotoUrl) {
    res.status(400).json({ ok: false, error: "foto_requerida" });
    return;
  }

  await updateEvidenceRow(evidence, {
    url_foto: newPhotoUrl,
    fecha_hora: nowISO(),
    foto_nombre: foto_nombre || evidence.foto_nombre,
    resultado_ai: resultado_ai || evidence.resultado_ai,
    score_confianza: score_confianza || evidence.score_confianza,
    riesgo: riesgo || evidence.riesgo,
    status: "ACTIVA",
  });

  res.json({ ok: true, evidencia_id, replaced: true });
});

app.post("/miniapp/supervisor/dashboard", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "supervisor") {
    res.status(403).json({ ok: false, error: "solo_supervisor" });
    return;
  }

  const equipo = await getPromotoresDeSupervisor(actor.profile.external_id);
  const byPromotor = [];

  for (const promotor of equipo) {
    const visits = await getVisitasToday(promotor.promotor_id);
    const evidencias = await getEvidenciasTodayByExternalId(promotor.external_id);
    byPromotor.push({
      promotor_id: promotor.promotor_id,
      external_id: promotor.external_id,
      nombre: promotor.nombre,
      visitas_hoy: visits.length,
      abiertas: visits.filter((visit) => !visit.hora_fin).length,
      cerradas: visits.filter((visit) => visit.hora_fin).length,
      evidencias_hoy: evidencias.length,
      alertas_riesgo: evidencias.filter((item) => item.riesgo === "ALTO" || item.riesgo === "MEDIO").length,
    });
  }

  res.json({
    ok: true,
    supervisor: actor.profile,
    equipo: byPromotor,
    resumen: {
      promotores: byPromotor.length,
      evidenciasHoy: byPromotor.reduce((acc, item) => acc + item.evidencias_hoy, 0),
      alertas: byPromotor.reduce((acc, item) => acc + item.alertas_riesgo, 0),
    },
  });
});

app.post("/miniapp/supervisor/promotor-detail", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "supervisor") {
    res.status(403).json({ ok: false, error: "solo_supervisor" });
    return;
  }

  const { promotor_external_id } = req.body || {};
  if (!promotor_external_id) {
    res.status(400).json({ ok: false, error: "promotor_external_id requerido" });
    return;
  }

  const promotor = await getPromotorByExternalId(promotor_external_id);
  if (!promotor || promotor.supervisor_external_id !== actor.profile.external_id) {
    res.status(404).json({ ok: false, error: "promotor_no_encontrado" });
    return;
  }

  const tiendaMap = await getTiendaMap();
  const visits = await getVisitasToday(promotor.promotor_id);
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const evidencias = await getEvidenciasTodayByExternalId(promotor.external_id);

  res.json({
    ok: true,
    promotor,
    visitas: visits.map((visit) => ({
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      hora_inicio_fmt: fmtDateTimeTZ(visit.hora_inicio),
      hora_fin_fmt: visit.hora_fin ? fmtDateTimeTZ(visit.hora_fin) : "pendiente",
    })),
    evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)),
  });
});

app.post("/miniapp/supervisor/visit-expedient", requireMiniAppActor, async (req, res) => {
  const actor = req.miniappActor;
  if (actor.role !== "supervisor") {
    res.status(403).json({ ok: false, error: "solo_supervisor" });
    return;
  }

  const { visita_id } = req.body || {};
  if (!visita_id) {
    res.status(400).json({ ok: false, error: "visita_id requerido" });
    return;
  }

  const visit = await getVisitById(visita_id);
  if (!visit) {
    res.status(404).json({ ok: false, error: "visita_no_encontrada" });
    return;
  }

  const promotorRows = await getPromotoresDeSupervisor(actor.profile.external_id);
  const allowedIds = new Set(promotorRows.map((item) => item.promotor_id));
  if (!allowedIds.has(visit.promotor_id)) {
    res.status(403).json({ ok: false, error: "sin_acceso" });
    return;
  }

  const tiendaMap = await getTiendaMap();
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const evidencias = await getEvidenciasByVisitId(visita_id);

  res.json({
    ok: true,
    visita: {
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      hora_inicio_fmt: fmtDateTimeTZ(visit.hora_inicio),
      hora_fin_fmt: visit.hora_fin ? fmtDateTimeTZ(visit.hora_fin) : "pendiente",
    },
    evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)),
  });
});

app.post("/miniapp/client/expedient", requireMiniAppActor, async (req, res) => {
  const { visita_id } = req.body || {};
  if (!visita_id) {
    res.status(400).json({ ok: false, error: "visita_id requerido" });
    return;
  }

  const visit = await getVisitById(visita_id);
  if (!visit) {
    res.status(404).json({ ok: false, error: "visita_no_encontrada" });
    return;
  }

  const tiendaMap = await getTiendaMap();
  const marcaMap = await getMarcaMap();
  const visitMap = await getAllVisitsMap();
  const evidencias = await getEvidenciasByVisitId(visita_id);

  res.json({
    ok: true,
    visita: {
      ...visit,
      tienda_nombre: tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id,
      cliente: tiendaMap[visit.tienda_id]?.cliente || "",
      hora_inicio_fmt: fmtDateTimeTZ(visit.hora_inicio),
      hora_fin_fmt: visit.hora_fin ? fmtDateTimeTZ(visit.hora_fin) : "pendiente",
    },
    evidencias: evidencias.map((item) => buildEvidenceView(item, marcaMap, visitMap, tiendaMap)),
    resumen: {
      total: evidencias.length,
      riesgo_alto: evidencias.filter((item) => item.riesgo === "ALTO").length,
      riesgo_medio: evidencias.filter((item) => item.riesgo === "MEDIO").length,
      riesgo_bajo: evidencias.filter((item) => item.riesgo === "BAJO").length,
    },
  });
});

app.post("/internal/client/publish-expedient", async (req, res) => {
  try {
    const { chat_id, visita_id, headline = "Visita completada" } = req.body || {};
    if (!chat_id || !visita_id) {
      res.status(400).json({ ok: false, error: "chat_id y visita_id requeridos" });
      return;
    }

    const visit = await getVisitById(visita_id);
    if (!visit) {
      res.status(404).json({ ok: false, error: "visita_no_encontrada" });
      return;
    }

    const tiendaMap = await getTiendaMap();
    const evidencias = await getEvidenciasByVisitId(visita_id);
    const cover = evidencias.find((item) => item.url_foto) || evidencias[0];

    const text =
      `📦 *${headline}*\n\n` +
      `🏬 *Tienda:* ${tiendaMap[visit.tienda_id]?.nombre_tienda || visit.tienda_id}\n` +
      `📅 *Fecha:* ${visit.fecha}\n` +
      `📸 *Evidencias:* ${evidencias.length}\n\n` +
      "Abre el expediente completo desde la Mini App.";

    if (cover?.url_foto) {
      await sendTelegramPhoto(chat_id, cover.url_foto, text, { reply_markup: buildClienteKeyboard() });
    } else {
      await sendTelegramText(chat_id, text, { reply_markup: buildClienteKeyboard() });
    }

    res.json({ ok: true, sent: true, visita_id });
  } catch (error) {
    console.error("client publish error", error);
    res.status(500).json({ ok: false, error: error.message || "publish_failed" });
  }
});

app.post("/telegram/webhook", async (req, res) => {
  if (TELEGRAM_WEBHOOK_SECRET) {
    const token = req.headers["x-telegram-bot-api-secret-token"];
    if (token !== TELEGRAM_WEBHOOK_SECRET) {
      res.status(401).json({ ok: false, error: "invalid_secret" });
      return;
    }
  }

  const incoming = parseTelegramUpdate(req.body || {});
  console.log("Telegram senderHandle:", incoming.senderHandle, "text:", incoming.text);
  if (!incoming.senderHandle) {
    res.json({ ok: true, ignored: true });
    return;
  }

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
});

app.post("/telegram/set-webhook", async (req, res) => {
  try {
    const baseUrl = buildBaseUrl(req);
    const webhookUrl = `${baseUrl}/telegram/webhook`;
    const payload = { url: webhookUrl };
    if (TELEGRAM_WEBHOOK_SECRET) payload.secret_token = TELEGRAM_WEBHOOK_SECRET;
    const result = await telegramApi("setWebhook", payload);
    res.json({ ok: true, webhookUrl, result });
  } catch (error) {
    console.error("setWebhook error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/", (_req, res) => {
  res.send("Promobolsillo+ Telegram backend v3 ✅");
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "promobolsillo-telegram", now: nowISO() });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Promobolsillo+ Telegram escuchando en puerto ${PORT}`);
});
