// E028_FIX3_BUILD_FRONTEND: elimina declaración obsoleta que impedía compilar en Render.
// E028_FIX2_REVISION_CONTINUA: avance inmediato, cola persistente, guardado por lotes, precarga y deshacer.
// E028_SUPERVISOR_CARGA_PROGRESIVA: metadatos primero, miniaturas ligeras y foto completa al seleccionarla.
// E027_FIX5_RETORNO_CAMARA_SEGURO: confirma guardado, refresca Mis fotos al volver y oculta errores técnicos de abort.
// E027_FIX4_PREDEPLOY_FRONTEND: conserva timeout ampliado para fotos de revisión.
// E027_CALIDAD_FOTOGRAFICA_CLOUDINARY: captura de revisión hasta 1280 px/82%; mismo flujo visible del promotor; sin retroalimentación nueva.
// E025_FECHAS_CDMX: toda fecha visible se fuerza a America/Mexico_City.
// E024J_CAMERA_SESSION_3H_EXPIRED_SCREEN: sesión cámara 3h y pantalla expirada limpia.
// E024I_MIS_FOTOS_LAYOUT_ROWS: Mis fotos organiza datos por renglón y detalle útil usa tarjeta compacta tipo resumen.
// E024G_BUILD_FIX_UNUSED_RETURN: elimina código de retorno deep-link que ya no se usa y corrige TS6133.
// E024F_UN_BOTON_VOLVER_REVISAR: elimina botón secundario y usa cierre seguro como única acción final.
// E024E_FIX_BOT_USERNAME_RETURN: usa @promobolsillo_operacion_bot para regresar desde cámara a Mini App.
// E024D_CLARIDAD_MIS_FOTOS_RETURN: Con foto/Sin servicio/Todo, regreso a Mis fotos, botón claro y sin texto redundante.
// E024C_RETURN_TO_MINIAPP_CAPTURE: botón de regreso abre Mini App en Capturar/Evidencias y evita volver al chat.
// E024B_FINALIZAR_SEGURO_Y_SIN_SERVICIO_CONTEXTUAL: evita Aw Snap al finalizar y mueve Sin servicio junto a Marca.
// E024_EXTERNAL_CAMERA_UX_WORKSPACE: mini app simplificada y cámara externa con contexto integrado, galeria horizontal y anulación.
// E023_EXTERNAL_CAMERA_WORKSPACE: cámara externa como espacio de captura; permite cambiar marca/tipo/estado y registrar múltiples fotos sin volver a Telegram.
// E022_EXTERNAL_CAMERA_PHASE1_PWA: prototipo de captura externa desde Telegram con token temporal; no usa galeria salvo autorizacion.
// E020F_BUILD_FIX_OPEN_CAMERA_FACING_USED: corrige TS6133 usando el parametro facing en openCamera; conserva E020C/E020D/E020E.
// E020E_BUILD_FIX_UNUSED_FACING: corrige TS6133 variable facing no usada; conserva E020C/E020D.
// E020D_BUILD_FIX_CSS_STYLE_TAG: corrige JSX style tag mal formado en E020C; conserva camara mejorada, filtros claros y fixes E020B.
// E020C_PROMOTOR_CAMERA_MEJORADA_Y_FIXES_E020B: conserva selfie frontal, evidencia trasera sin galeria, visor amplio 16:9, zoom si el dispositivo lo soporta, mensajes claros y filtros Mis evidencias corregidos.
// E020B_FIX_PROMOTOR_CAMERA_GUARDS_EVIDENCE_FILTERS: selfie vuelve a cámara frontal integrada; evidencia usa cámara trasera integrada, no galería; mensajes claros sin tienda; Mis evidencias liga Fotos/Sin servicio/Todo con detalle útil.
// E020A_BUILD_FIX_TS2367_CAMERA_NATIVE: corrige comparación TS2367 en openCamera; conserva E020 filtros claros y cámara nativa.
// E020_SUPERVISOR_FILTROS_CLAROS_PROMOTOR_CAPTURA_NATIVA: filtros desplegables, actualizar resultados unico, bloqueo captura sin tienda/visita, panel sin servicio compacto y cámara nativa.
// E019_PROMOTOR_SUPERVISOR_FLUIDEZ_REVISION_Y_SIN_SERVICIO: entrada mas fluida, fuera de servicio bajo solicitud, mis evidencias con Sin servicio, aprobacion con estado visible, consulta de aprobadas y seleccion masiva visible.
// E018B_PROMOTOR_SUPERVISOR_NAVEGACION_DETALLE_UTIL_CONFIRMADO: marcador visible para verificar en GitHub; incluye recargar arriba, Inicio/Final, fecha completa, detalle útil y filtro Sin servicio.
// E017_PROMOTOR_OPTIMIZACION_REGISTRO_EVIDENCIA: reduce peso de foto, evita refresco bloqueante despues de registrar evidencia y mejora mensajes de guardado.
// E016_SUPERVISOR_FILTROS_COMENTAR_REDESIGN: filtros Hoy/Semana/Rango, Observar->Comentar, resumen/detalle supervisor visual.
// E015_PROMOTOR_MARCA_FUERA_SERVICIO: permite justificar marca fuera de servicio por visita en version promotor.
// E014E_FIX4_REZGO_LOGO_HEADER_BUILD_OK: corrige export, conserva resumen E014C FIX1 y muestra tagline ASCII marker Pasion por la movilidad.
// E014F_REGISTRAR_EVIDENCIA_INLINE_LEFT: boton Registrar evidencia con icono y texto en una sola linea, alineado a la izquierda.
// E014E_FIX2_REZGO_LOGO_HEADER_TAGLINE: logo oficial REZGO con frase visible Pasión por la movilidad. Verifier ASCII marker: Pasion por la movilidad.
// E014E_FIX1_REZGO_LOGO_HEADER: logo oficial REZGO con frase Pasión por la movilidad.
// E014_REZGO_RULES_VERIFIER: conserva ESTADO_ACTUAL, tienda en rutero sin marcas activas y tipos de evidencia flexibles.
// E014C_FIX1_PROMOTOR_SUMMARY_REDESIGN_VERIFIER: conserva Promotor > Resumen layout operativo.
// E014C_FIX1_REZGO_RULES_VERIFIER: conserva ESTADO_ACTUAL y tienda en rutero sin marcas activas.
// E014B_CANCEL_EVIDENCE_REFRESH: corrige visor promotor con controles visibles arriba y abajo.
import React, { useEffect, useMemo, useRef, useState } from "react";
// E010_UX_ACTION_FIRST_SUPERVISOR: rediseño UX visual action-first conservando E009B inline demo.
import { motion } from "framer-motion";
import {
  AlertTriangle,
  Camera,
  CheckCircle2,
  Eye,
  Image as ImageIcon,
  MapPin,
  Pencil,
  RefreshCw,
  ShieldAlert,
  Trash2,
  Users,
  Check,
  ClipboardList,
  Store,
  RotateCcw,
} from "lucide-react";

declare global {
  interface Window {
    Telegram?: {
      WebApp?: {
        initData?: string;
        initDataUnsafe?: { start_param?: string };
        ready?: () => void;
        expand?: () => void;
        close?: () => void;
        setHeaderColor?: (color: string) => void;
        setBackgroundColor?: (color: string) => void;
        openLink?: (url: string, options?: { try_instant_view?: boolean }) => void;
        openTelegramLink?: (url: string) => void;
      };
    };
  }
}

type Role = "promotor" | "supervisor" | "cliente";
type AppRole = Role | null;
type PromotorModule = "asistencia" | "evidencias" | "mis_evidencias" | "resumen";
type SupervisorModule = "equipo" | "alertas" | "evidencias" | "resumen";
type ClientModule = "resumen" | "tiendas" | "evidencias" | "incidencias" | "entregables";
type EvidenceGroupMode = "marca" | "tienda" | "promotor" | "estatus";
type EvidencePhase = "ESTADO_ACTUAL" | "ANTES" | "DESPUES";
type CaptureKind = "entrada" | "salida";
type CameraTarget = "entrada" | "salida" | "evidencia" | "reemplazo";
type SupervisorDecision = "APROBADA" | "OBSERVADA" | "RECHAZADA";
type AlertFinalStatus = "RESUELTA" | "DESCARTADA";

type BootstrapResponse = {
  ok: boolean;
  role: Role;
  profile?: { nombre?: string };
};

type StoreItem = {
  tienda_id: string;
  nombre_tienda: string;
  tienda_display?: string;
  cadena?: string;
};

type VisitItem = {
  visita_id: string;
  tienda_id: string;
  tienda_nombre: string;
  tienda_display?: string;
  hora_inicio: string;
  hora_fin: string;
  estado_visita?: string;
  resultado_geocerca_entrada?: string;
  resultado_geocerca_salida?: string;
  promotor_nombre?: string;
};

type EvidenceItem = {
  evidencia_id: string;
  visita_id?: string;
  tipo_evento: string;
  tipo_evidencia: string;
  marca_id?: string;
  marca_nombre: string;
  riesgo: string;
  fecha_hora_fmt: string;
  fecha_hora?: string;
  url_foto: string;
  thumb_url?: string;
  photo_available?: boolean;
  photo_deferred?: boolean;
  photo_storage?: string;
  photo_bytes_estimate?: number;
  descripcion: string;
  tienda_nombre?: string;
  tienda_display?: string;
  tienda_id?: string;
  promotor_id?: string;
  promotor_nombre?: string;
  fase?: string;
  status?: string;
  decision_supervisor?: string;
  motivo_revision?: string;
  revisado_por?: string;
  fecha_revision?: string;
  hallazgos_ai?: string;
  reglas_disparadas?: string;
  resultado_ai?: string;
  score_confianza?: string;
  requiere_revision_supervisor?: string;
  local_review_pending?: boolean;
};

type UiEvidence = EvidenceItem & {
  status?: "ACTIVA" | "ANULADA" | string;
};

type PromotorUsageSummary = {
  today?: { bytes: number; mb: number; gb: number; fotos: number };
  month?: { bytes: number; mb: number; gb: number; fotos: number };
  reference?: { budget_mxn: number; reference_pct: number; estimated_mxn: number; note: string };
};

type DashboardResponse = {
  ok: boolean;
  promotor?: { nombre?: string };
  stores?: StoreItem[];
  openVisits?: VisitItem[];
  visitsToday?: VisitItem[];
  summary?: {
    assignedStores?: number;
    openVisits?: number;
    closedVisits?: number;
    evidenciasHoy?: number;
  };
  usage?: PromotorUsageSummary;
};

type EvidencesTodayResponse = {
  ok: boolean;
  evidencias?: EvidenceItem[];
};

type StartEntryResponse = {
  ok: boolean;
  visita_id: string;
  tienda_id: string;
  tienda_nombre: string;
  tienda_display?: string;
  started_at: string;
  warning?: string;
};

type CloseVisitResponse = {
  ok: boolean;
  visita_id: string;
  closed_at: string;
  warning?: string;
};

type EvidenceRegisterResponse = {
  ok: boolean;
  visita_id: string;
  created: string[];
  count: number;
  warning?: string;
};

type ExternalCameraSessionResponse = {
  ok: boolean;
  capture_url: string;
  expires_at?: string;
};

type ExternalCameraContextResponse = {
  ok: boolean;
  context?: {
    visita_id?: string;
    tienda_id?: string;
    tienda_nombre?: string;
    marca_id?: string;
    marca_nombre?: string;
    tipo_evidencia?: string;
    fase?: string;
    descripcion?: string;
    expires_at?: string;
    marcas?: Array<{ marca_id: string; marca_nombre: string }>;
    tipos?: Array<{ tipo_evidencia: string; descripcion_corta?: string }>;
  };
};

type ExternalCameraUploadResponse = {
  ok: boolean;
  evidencia_id?: string;
  message?: string;
  warning?: string;
  uploaded_count?: number;
};

type ReplaceEvidenceResponse = {
  ok: boolean;
  evidencia_id: string;
  replaced: boolean;
  warning?: string;
};

type MarcaFueraServicioItem = {
  registro_id?: string;
  fecha_hora?: string;
  visita_id?: string;
  tienda_id?: string;
  marca_id: string;
  motivo?: string;
  comentario?: string;
  estatus?: string;
};

type EvidenceContextResponse = {
  ok: boolean;
  visita?: {
    visita_id: string;
    tienda_id: string;
    tienda_nombre: string;
    tienda_display?: string;
  };
  marcas?: Array<{ marca_id: string; marca_nombre: string }>;
  marcas_fuera_servicio?: MarcaFueraServicioItem[];
};

type MarcaFueraServicioResponse = {
  ok: boolean;
  message?: string;
  row?: MarcaFueraServicioItem;
};

type EvidenceRulesResponse = {
  ok: boolean;
  reglas?: Array<{
    marca_id?: string;
    tipo_evidencia: string;
    fotos_requeridas: number;
    requiere_antes_despues: boolean;
    orden?: number;
    obligatoria?: boolean;
    observaciones?: string;
  }>;
};

type SupervisorSummary = {
  promotores: number;
  visitasHoy: number;
  abiertas: number;
  evidenciasHoy: number;
  alertas: number;
};

type SupervisorUsageSummary = {
  today?: { bytes: number; mb: number; gb: number; fotos: number };
  month?: { bytes: number; mb: number; gb: number; fotos: number };
};

type SupervisorPendingClose = {
  open_visits?: number;
  open_alerts?: number;
  pending_reviews?: number;
};

type SupervisorDashboardResponse = {
  ok: boolean;
  supervisor?: { nombre?: string };
  summary?: Partial<SupervisorSummary>;
  usage?: SupervisorUsageSummary;
  pending_close?: SupervisorPendingClose;
};

type SupervisorTeamRow = {
  promotor_id: string;
  external_id: string;
  nombre: string;
  region: string;
  visitas_hoy: number;
  visitas_abiertas: number;
  evidencias_hoy: number;
  alertas_abiertas: number;
  ultima_tienda: string;
  ultima_tienda_display?: string;
  ultima_entrada: string;
  ultima_salida: string;
  ultima_visita_id: string;
  status_general: string;
};

type SupervisorTeamResponse = {
  ok: boolean;
  team?: SupervisorTeamRow[];
};

type SupervisorDayRouteRow = {
  promotor_id?: string;
  visita_id: string;
  tienda_id: string;
  tienda_nombre: string;
  hora_inicio: string;
  hora_fin: string;
  entry_fmt?: string;
  exit_fmt?: string;
  stay_minutes?: number;
  geofence_entry?: string;
  geofence_exit?: string;
  total_evidencias: number;
  total_alertas: number;
  summary_by_brand?: Array<{ marca_id: string; marca_nombre: string; total: number }>;
};

type SupervisorDayRouteResponse = {
  ok: boolean;
  rows?: SupervisorDayRouteRow[];
};

type SupervisorAlert = {
  alerta_id: string;
  fecha_hora: string;
  fecha_hora_fmt: string;
  promotor_id: string;
  promotor_nombre: string;
  visita_id: string;
  evidencia_id: string;
  tipo_alerta: string;
  severidad: string;
  descripcion: string;
  status: string;
  supervisor_id?: string;
  tienda_id?: string;
  tienda_nombre?: string;
  atendida_por?: string;
  fecha_atencion?: string;
  canal_notificacion?: string;
  comentario_cierre?: string;
  origen_cierre?: string;
  url_foto?: string;
  photo_url?: string;
  hallazgos_ai?: string;
  reglas_disparadas?: string;
  tienda_display?: string;
};

type SupervisorAlertsResponse = {
  ok: boolean;
  alerts?: SupervisorAlert[];
};

type PromotorRecentAlert = {
  alerta_id: string;
  tipo_alerta: string;
  status: string;
  fecha_hora?: string;
  fecha_hora_fmt?: string;
  tienda_id?: string;
  tienda_nombre?: string;
  resolved_classification?: string;
};

type PromotorRecentAlertsResponse = {
  ok: boolean;
  rows?: PromotorRecentAlert[];
};

type ClientFilterOption = { id: string; label: string };

type ClientBootstrapResponse = {
  ok: boolean;
  data?: {
    role: "cliente";
    cliente?: {
      cliente_id: string;
      cliente_nombre: string;
      logo_url?: string;
      color_primario?: string;
    };
    access?: {
      nombre_contacto?: string;
      rol_cliente?: string;
    };
  };
};

type ClientDashboardData = {
  period?: { fecha_inicio: string; fecha_fin: string; label: string };
  cliente?: { cliente_id: string; cliente_nombre: string; logo_url?: string; color_primario?: string };
  kpis?: {
    tiendas_visibles: number;
    tiendas_visitadas: number;
    visitas: number;
    cumplimiento_pct: number;
    evidencias: number;
    aprobadas: number;
    observadas: number;
    rechazadas: number;
    alertas: number;
    geocerca_ok_pct: number;
  };
  top_alerts?: Array<{ tipo_alerta: string; total: number }>;
};

type ClientStoreRow = {
  tienda_id: string;
  tienda_nombre: string;
  cadena: string;
  region: string;
  ciudad: string;
  visitas: number;
  ultima_visita: string;
  ultima_visita_fmt: string;
  evidencias: number;
  aprobadas: number;
  observadas: number;
  alertas: number;
  estatus: string;
};

type ClientStoreDetail = {
  store?: { tienda_id: string; nombre_tienda: string; cadena?: string; region?: string; ciudad?: string; direccion?: string };
  summary?: { visitas: number; evidencias: number; aprobadas: number; observadas: number; alertas: number };
  visits?: VisitItem[];
  evidences?: EvidenceItem[];
  alerts?: SupervisorAlert[];
};

type ClientEnvelope<T> = {
  ok: boolean;
  data?: T;
  meta?: { page?: number; page_size?: number; total_rows?: number; total_pages?: number };
  error?: string | null;
};

type SupervisorAlertCloseResponse = {
  ok: boolean;
  alerta_id: string;
  status: string;
};

type SupervisorReviewSnapshot = {
  evidencia_id: string;
  decision_supervisor?: string;
  motivo_revision?: string;
  revisado_por?: string;
  fecha_revision?: string;
  requiere_revision_supervisor?: string;
  status?: string;
};

type SupervisorReviewQueueItem = {
  id: string;
  evidencia_id: string;
  decision_supervisor: SupervisorDecision | "";
  motivo_revision: string;
  restore?: boolean;
  previous: SupervisorReviewSnapshot;
  createdAt: string;
  attempts: number;
  lastError?: string;
};

type SupervisorReviewBatchResponse = {
  ok: boolean;
  accepted?: number;
  rejected?: number;
  results?: Array<{
    ok: boolean;
    client_review_id?: string;
    evidencia_id: string;
    decision_supervisor?: string;
    status?: string;
    restored?: boolean;
    error?: string;
  }>;
};

type SupervisorReviewUndoState = {
  id: string;
  items: SupervisorReviewSnapshot[];
  expiresAt: number;
};

type SupervisorEvidencesResponse = {
  ok: boolean;
  evidences?: EvidenceItem[];
  meta?: {
    total?: number;
    deferred_photos?: number;
    remote_photos?: number;
    payload_mode?: string;
  };
};

type SupervisorEvidencePhotoResponse = {
  ok: boolean;
  evidencia_id: string;
  url_foto?: string;
  thumb_url?: string;
  photo_deferred?: boolean;
  photo_storage?: string;
};

type SupervisorOutOfServiceItem = {
  registro_id: string;
  fecha_hora?: string;
  fecha_hora_fmt?: string;
  visita_id?: string;
  promotor_id?: string;
  promotor_nombre?: string;
  external_id?: string;
  tienda_id?: string;
  tienda_nombre?: string;
  tienda_display?: string;
  marca_id?: string;
  marca_nombre?: string;
  motivo?: string;
  comentario?: string;
  lat?: string;
  lon?: string;
  accuracy?: string;
  estatus?: string;
};

type SupervisorOutOfServiceResponse = {
  ok: boolean;
  rows?: SupervisorOutOfServiceItem[];
};

type PromotorOutOfServiceResponse = {
  ok: boolean;
  rows?: SupervisorOutOfServiceItem[];
};

type EvidenceAuditRow = {
  audit_id?: string;
  fecha_hora?: string;
  accion?: string;
  evidencia_id?: string;
  actor_role?: string;
  actor_id?: string;
  estado_previo?: string;
  estado_nuevo?: string;
  comentario?: string;
};

type EvidenceAuditResponse = {
  ok: boolean;
  rows?: EvidenceAuditRow[];
};

type VisitExpedientResponse = {
  ok: boolean;
  visita?: VisitItem & { stay_minutes?: number; entry_fmt?: string; exit_fmt?: string };
  evidencias?: EvidenceItem[];
  alertas?: SupervisorAlert[];
  summary?: { total_evidencias?: number; total_alertas?: number };
  summary_by_brand?: Array<{
    marca_id?: string;
    marca_nombre?: string;
    total?: number;
    types?: Array<{
      tipo_evidencia?: string;
      total?: number;
      phases?: Array<{ fase?: string; total?: number }>;
    }>;
  }>;
};

type LocationCapture = {
  lat: number;
  lon: number;
  accuracy: number;
  capturedAt: string;
};

type PhotoCapture = {
  name: string;
  dataUrl: string;
  capturedAt: string;
};

type PendingQueueStatus = "PENDIENTE_ENVIO" | "ERROR_ENVIO";
type PendingOpKind = "entry" | "evidence" | "close";
type PendingQueueOp = {
  id: string;
  kind: PendingOpKind;
  createdAt: string;
  status: PendingQueueStatus;
  attempts: number;
  lastError?: string;
  localVisitId?: string;
  visitaId?: string;
  tienda_id: string;
  tienda_nombre: string;
  payload: Record<string, any>;
};

type GalleryAuthorizationDebug = {
  now_local?: string;
  rows_scanned?: number;
  sheet_error?: string;
  reason?: string;
  autorizacion_id?: string;
};

type GalleryAuthorizationInfo = {
  allowed: boolean;
  authorization?: {
    autorizacion_id?: string;
    motivo?: string;
    autorizado_por?: string;
    vigencia_inicio?: string;
    vigencia_fin?: string;
    max_fotos?: number;
    fotos_usadas?: number;
  } | null;
  debug?: GalleryAuthorizationDebug;
};

type GalleryAuthorizationResponse = {
  ok: boolean;
  allowed: boolean;
  mode?: string;
  authorization?: GalleryAuthorizationInfo["authorization"];
  debug?: GalleryAuthorizationDebug;
};


const APP_TIME_ZONE = "America/Mexico_City";
const API_BASE = (import.meta.env.VITE_API_BASE || "https://promobolsillo-telegram.onrender.com").replace(/\/+$/, "");
const MINIAPP_RETURN_PARAM_PREFIX = "review_";
const E011_GROUPS_PER_PAGE = 8;
const E011_THUMBS_PER_GROUP = 18;
const SHEETS_SAFE_PHOTO_CHARS = 32000; const E027_REVIEW_MAX_SIDE = 1280; const E027_REVIEW_JPEG_QUALITY = 0.82;
const PENDING_QUEUE_KEY = "promobolsillo_pending_queue_v1";
const STORE_BRANDS_CACHE_KEY = "promobolsillo_store_brands_v1";
const EXTERNAL_CAMERA_LAST_SUCCESS_KEY = "promobolsillo_external_camera_last_success_v1";
const EXTERNAL_CAMERA_LAST_FINISH_KEY = "promobolsillo_external_camera_last_finish_v1";
const EXTERNAL_CAMERA_LAST_HANDLED_KEY = "promobolsillo_external_camera_last_handled_v1";
const SUPERVISOR_REVIEW_QUEUE_KEY = "promobolsillo_supervisor_review_queue_v1";
const SUPERVISOR_REVIEW_BATCH_SIZE = 60;
const SUPERVISOR_REVIEW_FLUSH_DELAY_MS = 3500;
const SUPERVISOR_REVIEW_UNDO_MS = 8000;

function safeReadLocalStorage(key: string) {
  if (typeof window === "undefined") return "";
  try {
    return window.localStorage.getItem(key) || "";
  } catch {
    return "";
  }
}

function safeWriteLocalStorage(key: string, value: string) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(key, value);
  } catch {}
}

function readPendingQueueStorage(): PendingQueueOp[] {
  const raw = safeReadLocalStorage(PENDING_QUEUE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writePendingQueueStorage(rows: PendingQueueOp[]) {
  safeWriteLocalStorage(PENDING_QUEUE_KEY, JSON.stringify(rows));
}

function readSupervisorReviewQueueStorage(): SupervisorReviewQueueItem[] {
  const raw = safeReadLocalStorage(SUPERVISOR_REVIEW_QUEUE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeSupervisorReviewQueueStorage(rows: SupervisorReviewQueueItem[]) {
  safeWriteLocalStorage(SUPERVISOR_REVIEW_QUEUE_KEY, JSON.stringify(rows));
}

function sortSupervisorReviewQueue(rows: SupervisorReviewQueueItem[]) {
  return [...rows].sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
}

function buildSupervisorReviewQueueId() {
  return `SUPREV-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function readStoreBrandsCacheStorage(): Record<string, Array<{ marca_id: string; marca_nombre: string }>> {
  const raw = safeReadLocalStorage(STORE_BRANDS_CACHE_KEY);
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function writeStoreBrandsCacheStorage(value: Record<string, Array<{ marca_id: string; marca_nombre: string }>>) {
  safeWriteLocalStorage(STORE_BRANDS_CACHE_KEY, JSON.stringify(value));
}

function isLocalVisitId(value?: string) {
  return String(value || "").startsWith("LOCAL-");
}

function buildPendingQueueId(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function isAbortLikeError(err: unknown) {
  const name = err instanceof Error ? err.name : "";
  const message = err instanceof Error ? err.message : String(err || "");
  const normalized = `${name} ${message}`.toLowerCase();
  return normalized.includes("abort")
    || normalized.includes("signal is aborted")
    || normalized.includes("timeout")
    || normalized.includes("tiempo de espera");
}

function friendlyRequestError(err: unknown, fallback: string) {
  if (isAbortLikeError(err)) {
    return "La actualización tardó más de lo esperado. Lo ya guardado no se perdió; espera unos segundos y vuelve a intentar.";
  }
  return err instanceof Error && err.message ? err.message : fallback;
}

function waitMs(ms: number) {
  return new Promise<void>((resolve) => window.setTimeout(resolve, ms));
}

function shouldQueueSubmission(err: unknown) {
  if (typeof navigator !== "undefined" && navigator.onLine === false) return true;
  const message = err instanceof Error ? err.message : String(err || "");
  const text = message.toLowerCase();
  return text.includes("failed to fetch") || text.includes("networkerror") || text.includes("network request failed") || text.includes("abort") || text.includes("network") || text.includes("load failed");
}

function sortPendingQueue(rows: PendingQueueOp[]) {
  return [...rows].sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
}

function formatPendingQueueLabel(item: PendingQueueOp) {
  if (item.kind === "entry") return `Entrada pendiente · ${formatStoreDisplay(item.tienda_id, item.tienda_nombre)}`;
  if (item.kind === "close") return `Salida pendiente · ${formatStoreDisplay(item.tienda_id, item.tienda_nombre)}`;
  const brand = String(item.payload?.marca_nombre || item.payload?.marca_id || "Marca");
  const type = String(item.payload?.tipo_evidencia || "Evidencia");
  return `Evidencia pendiente · ${formatStoreDisplay(item.tienda_id, item.tienda_nombre)} · ${brand} · ${type}`;
}

function getTelegramWebApp() {
  if (typeof window === "undefined") return undefined;
  return window.Telegram?.WebApp;
}

function getInitData() {
  return getTelegramWebApp()?.initData || "";
}

function getTelegramStartParam() {
  if (typeof window === "undefined") return "";
  const webApp = getTelegramWebApp();
  const directParam = webApp?.initDataUnsafe?.start_param || "";
  if (directParam) return String(directParam);
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get("tgWebAppStartParam") || params.get("startapp") || params.get("start_param") || "";
  } catch {
    return "";
  }
}
function decodeStartParamSegment(value: string) {
  const raw = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  try {
    const padded = raw.padEnd(Math.ceil(raw.length / 4) * 4, "=");
    return decodeURIComponent(atob(padded));
  } catch {
    return String(value || "");
  }
}

function parseMiniAppReturnStartParam(startParam: string) {
  const raw = String(startParam || "");
  if (!raw) return { wantsReview: false, visitaId: "" };
  if (raw === "review" || raw === "mis_fotos" || raw === "capture" || raw === "capturar" || raw === "return_capture") return { wantsReview: true, visitaId: "" };
  if (raw.startsWith(MINIAPP_RETURN_PARAM_PREFIX)) {
    return { wantsReview: true, visitaId: decodeStartParamSegment(raw.slice(MINIAPP_RETURN_PARAM_PREFIX.length)) };
  }
  if (raw.startsWith("capture_")) {
    return { wantsReview: true, visitaId: decodeStartParamSegment(raw.slice("capture_".length)) };
  }
  return { wantsReview: false, visitaId: "" };
}
async function postJson<T>(path: string, payload: Record<string, unknown>, timeoutMs = 20000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ initData: getInitData(), ...payload }),
      signal: controller.signal,
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error((json as { error?: string }).error || `Error ${res.status}`);
    return json as T;
  } finally {
    clearTimeout(timeout);
  }
}

async function postExternalJson<T>(path: string, payload: Record<string, unknown>, timeoutMs = 45000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error((json as { error?: string }).error || `Error ${res.status}`);
    return json as T;
  } finally {
    clearTimeout(timeout);
  }
}

function formatHourFromIso(iso: string) {
  if (!iso) return "pendiente";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString("es-MX", {
    timeZone: APP_TIME_ZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function formatDateTimeMaybe(iso?: string) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("es-MX", {
    timeZone: APP_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}


// E016: utilidades de periodo para filtros del supervisor.
function localYmd(date = new Date()) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-CA", {
      timeZone: APP_TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(date).map(({ type, value }) => [type, value]),
  );
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function startOfWeekMondayYmd(date = new Date()) {
  const base = new Date(`${localYmd(date)}T12:00:00Z`);
  const day = base.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  base.setUTCDate(base.getUTCDate() + diff);
  return base.toISOString().slice(0, 10);
}

function endOfWeekSundayYmd(date = new Date()) {
  const base = new Date(`${localYmd(date)}T12:00:00Z`);
  const day = base.getUTCDay();
  const diff = day === 0 ? 0 : 7 - day;
  base.setUTCDate(base.getUTCDate() + diff);
  return base.toISOString().slice(0, 10);
}

function ymdFromAnyDateValue(value?: string) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const mx = raw.match(/^(\d{2})\/(\d{2})\/(20\d{2})$/);
  if (mx) return `${mx[3]}-${mx[2]}-${mx[1]}`;
  const dateOnly = raw.match(/^(20\d{2}-\d{2}-\d{2})$/);
  if (dateOnly) return dateOnly[1];
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return localYmd(parsed);
  const embedded = raw.match(/(20\d{2}-\d{2}-\d{2})/);
  return embedded ? embedded[1] : "";
}

function getEvidenceYmd(item?: EvidenceItem | null) {
  return ymdFromAnyDateValue(item?.fecha_hora || item?.fecha_hora_fmt || "");
}

function getOutOfServiceYmd(item?: SupervisorOutOfServiceItem | null) {
  return ymdFromAnyDateValue(item?.fecha_hora || item?.fecha_hora_fmt || "");
}

function extractStoreDeterminant(value?: string) {
  const match = String(value || "").trim().match(/(\d+)\s*$/);
  return match ? match[1] : "";
}

function formatStoreDisplay(storeId?: string, storeName?: string) {
  const determinante = extractStoreDeterminant(storeId);
  const nombre = (storeName || "").trim();
  if (!determinante) return nombre;
  const prefixed = `${determinante} - `;
  if (nombre.startsWith(prefixed)) return nombre;
  return nombre ? `${prefixed}${nombre}` : determinante;
}

function getStoreDisplayFromItem(item?: { tienda_display?: string; tienda_id?: string; tienda_nombre?: string }) {
  if (!item) return "";
  return item.tienda_display || formatStoreDisplay(item.tienda_id, item.tienda_nombre);
}

function formatPhaseLabel(value?: string) {
  const phase = String(value || "").trim().toUpperCase();
  if (phase === "ANTES") return "Antes";
  if (phase === "DESPUES") return "Después";
  if (phase === "ESTADO_ACTUAL" || phase === "ESTADO ACTUAL" || phase === "NA") return "Estado actual";
  return value || "";
}


function nowMxString() {
  return formatDateTimeMaybe(new Date().toISOString());
}

function getStoreNameById(storeId: string, stores: StoreItem[]) {
  const found = stores.find((store) => store.tienda_id === storeId || store.nombre_tienda === storeId);
  return found ? (found.nombre_tienda || found.tienda_id || "") : "";
}

function getVisitDisplayName(visit: VisitItem, stores: StoreItem[]) {
  const storeName = getStoreNameById(visit.tienda_id, stores) || visit.tienda_nombre || "Visita activa";
  return visit.tienda_display || formatStoreDisplay(visit.tienda_id, storeName);
}

function normalizeBrandLabel(rawLabel: string, fallbackId: string) {
  const label = (rawLabel || "").trim();
  if (!label) return fallbackId || "Marca";
  if (/^(true|false)$/i.test(label)) return fallbackId || "Marca";
  return label;
}

function isOperationalEvidence(item: EvidenceItem) {
  return (item.tipo_evidencia || "").trim().toUpperCase() !== "ASISTENCIA";
}

function isAttendanceEvidence(item: EvidenceItem) {
  return !isOperationalEvidence(item);
}

function isValidRuleType(value: string) {
  const v = (value || "").trim();
  return !!v && !/^(true|false)$/i.test(v);
}

function compactMetaLine(item: EvidenceItem) {
  const parts = [getStoreDisplayFromItem(item), normalizeBrandLabel(item.marca_nombre || "", "Marca"), item.fase ? `Fase: ${item.fase}` : ""].filter(Boolean);
  return parts.join(" · ");
}

function cleanEvidenceDescription(value: string) {
  return (value || "").replace(/^\[[^\]]+\]\s*/, "").trim();
}

function geofenceShortLabel(value?: string) {
  const v = (value || "").trim().toUpperCase();
  if (!v) return "Sin dato";
  if (v === "OK_EN_GEOCERCA") return "En geocerca";
  if (v === "OK_CON_TOLERANCIA_GPS") return "Con tolerancia";
  if (v === "FUERA_DE_GEOCERCA") return "Fuera";
  return v;
}

function geofenceClass(value?: string) {
  const v = (value || "").trim().toUpperCase();
  if (v === "FUERA_DE_GEOCERCA") return "geoRed";
  if (v === "OK_CON_TOLERANCIA_GPS") return "geoAmber";
  if (v === "OK_EN_GEOCERCA") return "geoGreen";
  return "geoNeutral";
}

function severityClass(value?: string) {
  const v = (value || "").trim().toUpperCase();
  if (["ALTA", "ALTO"].includes(v)) return "riskRed";
  if (["MEDIA", "MEDIO"].includes(v)) return "riskAmber";
  return "riskGreen";
}

function statusClass(value?: string) {
  const v = (value || "").trim().toUpperCase();
  if (["ALERTA", "ABIERTA", "RECHAZADA", "DESCARTADA"].includes(v)) return "riskRed";
  if (["OBSERVADA", "PENDIENTE_REVISION", "ABIERTA_CON_ALERTA"].includes(v)) return "riskAmber";
  return "riskGreen";
}

function getSupervisorReviewState(item?: Pick<EvidenceItem, "decision_supervisor" | "status"> | null) {
  const decision = String(item?.decision_supervisor || "").trim().toUpperCase();
  if (["APROBADA", "OBSERVADA", "RECHAZADA"].includes(decision)) return decision;
  const status = String(item?.status || "").trim().toUpperCase();
  if (["APROBADA", "OBSERVADA", "RECHAZADA"].includes(status)) return status;
  return "PENDIENTE";
}

function getSupervisorReviewClass(item?: Pick<EvidenceItem, "decision_supervisor" | "status"> | null) {
  const state = getSupervisorReviewState(item);
  if (state === "APROBADA") return "riskGreen";
  if (state === "OBSERVADA") return "riskAmber";
  if (state === "RECHAZADA") return "riskRed";
  return "riskNeutral";
}

function getSupervisorReviewLabel(value?: string | Pick<EvidenceItem, "decision_supervisor" | "status"> | null) {
  const state = typeof value === "string" ? value.trim().toUpperCase() : getSupervisorReviewState(value);
  if (state === "OBSERVADA") return "COMENTADA";
  if (state === "PENDIENTE") return "PENDIENTE";
  if (state === "APROBADA") return "APROBADA";
  if (state === "RECHAZADA") return "RECHAZADA";
  return state || "PENDIENTE";
}

function isSupervisorPendingEvidence(item?: Pick<EvidenceItem, "decision_supervisor" | "status"> | null) {
  return getSupervisorReviewState(item) === "PENDIENTE";
}

function getEvidenceGroupModeLabel(mode: EvidenceGroupMode) {
  if (mode === "tienda") return "Tienda";
  if (mode === "promotor") return "Promotor";
  if (mode === "estatus") return "Estatus";
  return "Marca";
}

function getEvidenceGroupInfo(item: EvidenceItem, mode: EvidenceGroupMode) {
  const brandLabel = normalizeBrandLabel(item.marca_nombre || "", item.marca_id || "Marca");
  const storeLabel = getStoreDisplayFromItem(item) || item.tienda_nombre || item.tienda_id || "Tienda sin nombre";
  const promotorLabel = item.promotor_nombre || item.promotor_id || "Promotor sin nombre";
  const state = getSupervisorReviewState(item);
  if (mode === "tienda") {
    return { key: `tienda::${item.tienda_id || storeLabel}`, label: storeLabel, subtitle: `${brandLabel} · ${promotorLabel}` };
  }
  if (mode === "promotor") {
    return { key: `promotor::${item.promotor_id || promotorLabel}`, label: promotorLabel, subtitle: `${storeLabel} · ${brandLabel}` };
  }
  if (mode === "estatus") {
    return { key: `estatus::${state}`, label: getSupervisorReviewLabel(state), subtitle: `${brandLabel} · ${storeLabel}` };
  }
  return { key: `marca::${item.marca_id || brandLabel}`, label: brandLabel, subtitle: `${storeLabel} · ${promotorLabel}` };
}

function buildEvidenceGroups(items: EvidenceItem[], mode: EvidenceGroupMode) {
  const groups = new Map<string, {
    brandKey: string;
    brandLabel: string;
    brandSubtitle: string;
    items: EvidenceItem[];
    previewItems: EvidenceItem[];
    total: number;
    pendientes: number;
    aprobadas: number;
    observadas: number;
    rechazadas: number;
    tiendas: string[];
    promotores: string[];
    marcas: string[];
  }>();
  items.forEach((item) => {
    const info = getEvidenceGroupInfo(item, mode);
    const current = groups.get(info.key) || {
      brandKey: info.key,
      brandLabel: info.label,
      brandSubtitle: info.subtitle,
      items: [],
      previewItems: [],
      total: 0,
      pendientes: 0,
      aprobadas: 0,
      observadas: 0,
      rechazadas: 0,
      tiendas: [],
      promotores: [],
      marcas: [],
    };
    current.items.push(item);
    current.total += 1;
    const state = getSupervisorReviewState(item);
    if (state === "APROBADA") current.aprobadas += 1;
    else if (state === "OBSERVADA") current.observadas += 1;
    else if (state === "RECHAZADA") current.rechazadas += 1;
    else current.pendientes += 1;
    const storeLabel = getStoreDisplayFromItem(item) || item.tienda_nombre || item.tienda_id || "";
    const promotorLabel = item.promotor_nombre || item.promotor_id || "";
    const brandLabel = normalizeBrandLabel(item.marca_nombre || "", item.marca_id || "");
    if (storeLabel && !current.tiendas.includes(storeLabel)) current.tiendas.push(storeLabel);
    if (promotorLabel && !current.promotores.includes(promotorLabel)) current.promotores.push(promotorLabel);
    if (brandLabel && !current.marcas.includes(brandLabel)) current.marcas.push(brandLabel);
    groups.set(info.key, current);
  });
  return Array.from(groups.values())
    .map((group) => {
      const sortedItems = [...group.items].sort((a, b) => String(b.fecha_hora || b.fecha_hora_fmt || "").localeCompare(String(a.fecha_hora || a.fecha_hora_fmt || "")));
      return { ...group, items: sortedItems, previewItems: sortedItems.slice(0, 4) };
    })
    .sort((a, b) => {
      if (b.pendientes !== a.pendientes) return b.pendientes - a.pendientes;
      if (b.total !== a.total) return b.total - a.total;
      return a.brandLabel.localeCompare(b.brandLabel);
    });
}

function compressDataUrl(dataUrl: string, maxSide: number, quality: number) {
  return new Promise<string>((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement("canvas");
      let width = img.width;
      let height = img.height;
      const larger = Math.max(width, height);
      if (larger > maxSide) {
        const scale = maxSide / larger;
        width = Math.round(width * scale);
        height = Math.round(height * scale);
      }
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d");
      if (!ctx) {
        reject(new Error("No se pudo procesar la foto"));
        return;
      }
      ctx.drawImage(img, 0, 0, width, height);
      resolve(canvas.toDataURL("image/jpeg", quality));
    };
    img.onerror = () => reject(new Error("No se pudo procesar la foto"));
    img.src = dataUrl;
  });
}

async function compressDataUrlForReview(dataUrl: string) { return await compressDataUrl(dataUrl, E027_REVIEW_MAX_SIDE, E027_REVIEW_JPEG_QUALITY); } async function compressDataUrlToSheetsSafeSize(dataUrl: string, maxChars = SHEETS_SAFE_PHOTO_CHARS) {
  // E017: para piloto, priorizamos velocidad de envío y carga estable en Sheets.
  // Se arranca desde tamaños más ligeros para evitar payloads grandes en cada registro.
  const attempts = [
    { side: 840, quality: 0.78 },
    { side: 720, quality: 0.74 },
    { side: 640, quality: 0.70 },
    { side: 560, quality: 0.66 },
    { side: 480, quality: 0.62 },
    { side: 420, quality: 0.58 },
    { side: 360, quality: 0.54 },
  ];
  let last = dataUrl;
  for (const attempt of attempts) {
    last = await compressDataUrl(dataUrl, attempt.side, attempt.quality);
    if (last.length <= maxChars) return last;
  }
  return last;
}


async function fileToDataUrl(file: File) {
  return await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("No se pudo leer la foto seleccionada."));
    reader.readAsDataURL(file);
  });
}

async function readPhotoForSheets(file: File, prefix = "galeria", reviewQuality = true) { const raw = await fileToDataUrl(file); const dataUrl = reviewQuality ? await compressDataUrlForReview(raw) : await compressDataUrlToSheetsSafeSize(raw);
  const safePrefix = prefix || "galeria";
  return {
    name: `${safePrefix}-${Date.now()}-${file.name || "foto.jpg"}`,
    dataUrl,
    capturedAt: nowMxString(),
  } as PhotoCapture;
}

function getCurrentLocation() {
  return new Promise<LocationCapture>((resolve, reject) => {
    if (typeof navigator === "undefined" || !navigator.geolocation) {
      reject(new Error("Geolocalización no disponible"));
      return;
    }
    navigator.geolocation.getCurrentPosition(
      (position) => {
        resolve({
          lat: position.coords.latitude,
          lon: position.coords.longitude,
          accuracy: position.coords.accuracy,
          capturedAt: nowMxString(),
        });
      },
      () => reject(new Error("No se pudo obtener la ubicación")),
      { enableHighAccuracy: true, timeout: 12000, maximumAge: 0 }
    );
  });
}

const marcaFueraServicioMotivos = [
  "Sin servicio vigente",
  "Visita no corresponde esta semana",
  "Marca de visita quincenal",
  "Sin exhibición",
  "Producto retirado",
  "Sin inventario / sin presencia",
  "No autorizado por tienda",
  "Otro",
];

const promotorTabs: Array<{ key: PromotorModule; label: string }> = [
  { key: "asistencia", label: "Inicio" },
  { key: "evidencias", label: "Capturar" },
  { key: "mis_evidencias", label: "Mis fotos" },
  { key: "resumen", label: "Resumen" },
];

const supervisorTabs: Array<{ key: SupervisorModule; label: string }> = [
  { key: "evidencias", label: "Revisar" },
  { key: "alertas", label: "Alertas" },
  { key: "equipo", label: "Equipo" },
  { key: "resumen", label: "Historial" },
];

const clientTabs: Array<{ key: ClientModule; label: string }> = [
  { key: "resumen", label: "Historial" },
  { key: "tiendas", label: "Tiendas" },
  { key: "evidencias", label: "Evidencia validada" },
  { key: "incidencias", label: "Incidencias" },
  { key: "entregables", label: "Entregables" },
];


function ExternalCameraCapturePage({ token }: { token: string }) {
  const nativeCameraRef = useRef<HTMLInputElement | null>(null);
  const [context, setContext] = useState<ExternalCameraContextResponse["context"] | null>(null);
  const [selectedMarcaId, setSelectedMarcaId] = useState("");
  const [selectedTipo, setSelectedTipo] = useState("");
  const [selectedFase, setSelectedFase] = useState<EvidencePhase>("ESTADO_ACTUAL");
  const [descripcion, setDescripcion] = useState("");
  const [preview, setPreview] = useState("");
  const [status, setStatus] = useState("Preparando cámara de evidencias...");
  const [uploading, setUploading] = useState(false);
  const [uploadedCount, setUploadedCount] = useState(0);
  const [annulledCount, setAnnulledCount] = useState(0);
  const [outOfServiceCount, setOutOfServiceCount] = useState(0);
  const [recentPhotos, setRecentPhotos] = useState<Array<{ evidencia_id?: string; name: string; dataUrl: string; marca: string; tipo: string; fase: string; status: "SUBIENDO" | "REGISTRADA" | "ANULADA" | "ERROR" }>>([]);
  const [finished, setFinished] = useState(false);
  const [outOfServiceMarcaIds, setOutOfServiceMarcaIds] = useState<Record<string, boolean>>({});
  const [error, setError] = useState("");
  const [showAllPhotos, setShowAllPhotos] = useState(false);
  const [outOfServiceOpen, setOutOfServiceOpen] = useState(false);
  const [outOfServiceReason, setOutOfServiceReason] = useState(marcaFueraServicioMotivos[0] || "Sin servicio vigente");
  const [outOfServiceComment, setOutOfServiceComment] = useState("");

  useEffect(() => {
    let alive = true;
    async function loadContext() {
      try {
        const data = await postExternalJson<ExternalCameraContextResponse>("/external-camera/context", { token });
        if (!alive) return;
        const ctx = data.context || null;
        const marcas = ctx?.marcas || [];
        const tipos = ctx?.tipos || [];
        setContext(ctx);
        setSelectedMarcaId(ctx?.marca_id || marcas[0]?.marca_id || "");
        setSelectedTipo(ctx?.tipo_evidencia || tipos[0]?.tipo_evidencia || "");
        const incomingPhase = String(ctx?.fase || "ESTADO_ACTUAL").toUpperCase();
        setSelectedFase((incomingPhase === "ANTES" || incomingPhase === "DESPUES" ? incomingPhase : "ESTADO_ACTUAL") as EvidencePhase);
        setDescripcion(ctx?.descripcion || "");
        setStatus("Lista para capturar. Cambia marca, tipo o estado aquí mismo.");
      } catch (err) {
        if (!alive) return;
        setError(err instanceof Error ? err.message : "No se pudo validar la sesión de cámara.");
        setStatus("");
      }
    }
    void loadContext();
    return () => { alive = false; };
  }, [token]);

  const marcas = context?.marcas || [];
  const tipos = context?.tipos || [];
  const selectedMarca = marcas.find((item) => item.marca_id === selectedMarcaId);
  const marcaNombre = selectedMarca?.marca_nombre || context?.marca_nombre || selectedMarcaId || "Marca";
  const store = context?.tienda_nombre || context?.tienda_id || "Tienda";
  const tipoLabel = selectedTipo || "Tipo";
  const faseLabel = formatPhaseLabel(selectedFase);
  const selectedBrandMarkedOutOfService = !!selectedMarcaId && !!outOfServiceMarcaIds[selectedMarcaId];
  const isExpiredCameraSession = !!error && error.toLowerCase().includes("expir");
  const canCapture = !!token && !error && !!selectedMarcaId && !!selectedTipo && !!context?.visita_id && !uploading && !selectedBrandMarkedOutOfService;
  const registeredPhotos = recentPhotos.filter((item) => item.status === "REGISTRADA");
  const activeRecentPhotos = recentPhotos.filter((item) => item.status !== "ANULADA");
  const lastRegistered = registeredPhotos[0];
  const registeredForCurrentBrandCount = registeredPhotos.filter((item) => item.marca === marcaNombre).length;

  async function handleNativeEvidenceSelection(files: FileList | null) {
    const file = files?.[0];
    if (!file) return;
    if (!selectedMarcaId || !selectedTipo || !context?.visita_id) {
      setStatus("Selecciona marca y tipo antes de capturar.");
      if (nativeCameraRef.current) nativeCameraRef.current.value = "";
      return;
    }
    const tempId = `local-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    try {
      setError("");
      setUploading(true);
      setStatus("Procesando foto...");
      const photo = await readPhotoForSheets(file);
      setPreview(photo.dataUrl);
      setRecentPhotos((prev) => [{ evidencia_id: tempId, name: photo.name, dataUrl: photo.dataUrl, marca: marcaNombre, tipo: selectedTipo, fase: selectedFase, status: "SUBIENDO" as const }, ...prev].slice(0, 24));
      setStatus("Registrando evidencia...");
      const result = await postExternalJson<ExternalCameraUploadResponse>("/external-camera/upload", {
        token,
        photo,
        context: {
          marca_id: selectedMarcaId,
          marca_nombre: marcaNombre,
          tipo_evidencia: selectedTipo,
          fase: selectedFase,
          descripcion: descripcion.trim(),
        },
      }, 120000);
      const evidenceId = result.evidencia_id || tempId;
      setUploadedCount((prev) => prev + 1);
      setRecentPhotos((prev) => prev.map((item) => item.evidencia_id === tempId ? { ...item, evidencia_id: evidenceId, status: "REGISTRADA" as const } : item));
      safeWriteLocalStorage(EXTERNAL_CAMERA_LAST_SUCCESS_KEY, JSON.stringify({
        evidenciaId: evidenceId,
        visitaId: context.visita_id || "",
        registeredAt: new Date().toISOString(),
      }));
      setStatus(result.message || "Foto registrada. Puedes tomar otra, cambiar tipo o cambiar marca.");
    } catch (err) {
      setRecentPhotos((prev) => prev.map((item) => item.evidencia_id === tempId ? { ...item, status: "ERROR" as const } : item));
      setError(err instanceof Error ? err.message : "No se pudo registrar la evidencia.");
      setStatus("");
    } finally {
      setUploading(false);
      if (nativeCameraRef.current) nativeCameraRef.current.value = "";
    }
  }

  async function cancelExternalPhoto(item: { evidencia_id?: string; status: string }) {
    if (!item.evidencia_id || item.status !== "REGISTRADA") return;
    try {
      setError("");
      setStatus("Anulando foto...");
      await postExternalJson<{ ok: boolean; evidencia_id?: string; status?: string }>("/external-camera/cancel", {
        token,
        evidencia_id: item.evidencia_id,
        note: "ANULADA_POR_PROMOTOR_DESDE_CAMARA_EXTERNA",
      }, 45000);
      setRecentPhotos((prev) => prev.map((photo) => photo.evidencia_id === item.evidencia_id ? { ...photo, status: "ANULADA" as const } : photo));
      setAnnulledCount((prev) => prev + 1);
      setStatus("Foto anulada. No aparecerá como evidencia activa.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "No se pudo anular la foto.");
      setStatus("");
    }
  }

  async function markOutOfServiceFromCamera() {
    if (!context?.visita_id || !selectedMarcaId) {
      setStatus("Selecciona marca antes de marcar fuera de servicio.");
      return;
    }
    if (!outOfServiceReason) {
      setStatus("Selecciona un motivo para marcar fuera de servicio.");
      return;
    }
    if (registeredForCurrentBrandCount > 0) {
      const ok = window.confirm(`${marcaNombre} ya tiene ${registeredForCurrentBrandCount} foto${registeredForCurrentBrandCount === 1 ? "" : "s"} registrada${registeredForCurrentBrandCount === 1 ? "" : "s"} en esta visita. ¿Confirmas marcar solo esta marca como fuera de servicio conservando esas fotos?`);
      if (!ok) {
        setStatus("Sin servicio cancelado. No se modificó la marca seleccionada.");
        return;
      }
    }
    try {
      setError("");
      setStatus("Registrando marca fuera de servicio...");
      await postExternalJson<{ ok: boolean; message?: string }>("/external-camera/out-of-service", {
        token,
        marca_id: selectedMarcaId,
        motivo: outOfServiceReason,
        comentario: outOfServiceComment.trim(),
      }, 45000);
      setOutOfServiceMarcaIds((prev) => ({ ...prev, [selectedMarcaId]: true }));
      setOutOfServiceCount((prev) => prev + 1);
      setOutOfServiceOpen(false);
      setOutOfServiceComment("");
      setStatus(`${marcaNombre} marcada fuera de servicio para esta visita.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "No se pudo marcar la marca fuera de servicio.");
      setStatus("");
    }
  }

  function finishCapture() {
    const summary = `Captura finalizada: ${uploadedCount} foto${uploadedCount === 1 ? "" : "s"} registrada${uploadedCount === 1 ? "" : "s"}${annulledCount ? `, ${annulledCount} anulada${annulledCount === 1 ? "" : "s"}` : ""}${outOfServiceCount ? `, ${outOfServiceCount} marca${outOfServiceCount === 1 ? "" : "s"} fuera de servicio` : ""}.`;
    setError("");
    setStatus(summary);
    setPreview("");
    setRecentPhotos([]);
    setShowAllPhotos(false);
    setFinished(true);
    const finishedPayload = {
      uploadedCount,
      annulledCount,
      outOfServiceCount,
      visitaId: context?.visita_id || "",
      finishedAt: new Date().toISOString(),
    };
    safeWriteLocalStorage(EXTERNAL_CAMERA_LAST_FINISH_KEY, JSON.stringify(finishedPayload));
    try {
      sessionStorage.setItem("promobolsillo_external_camera_finished", JSON.stringify(finishedPayload));
    } catch {}
    window.setTimeout(() => {
      try { window.scrollTo({ top: 0, behavior: "smooth" }); } catch {}
    }, 50);
  }
  function closeExternalCameraSafely() {
    setStatus("Volviendo a PromoBolsillo. Si no se cierra automáticamente, toca la X superior.");
    try { window.close(); } catch {}
  }

  const shellStyle: React.CSSProperties = { minHeight: "100vh", background: "linear-gradient(180deg,#f6f8fb 0%,#e8eef5 100%)", fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", color: "#17212b" };
  const cardStyle: React.CSSProperties = { borderRadius: 24, background: "rgba(255,255,255,.97)", border: "1px solid rgba(15,23,42,.08)", boxShadow: "0 16px 42px rgba(15,23,42,.10)" };
  const selectStyle: React.CSSProperties = { width: "100%", border: "1px solid rgba(15,23,42,.14)", borderRadius: 16, padding: "12px 12px", fontWeight: 850, color: "#17212b", background: "#fff", fontSize: 15, boxSizing: "border-box" };
  const labelStyle: React.CSSProperties = { fontSize: 11, fontWeight: 950, color: "#64748b", textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 6 };
  const smallButtonStyle: React.CSSProperties = { border: "1px solid rgba(15,23,42,.12)", borderRadius: 14, background: "#fff", color: "#334155", padding: "10px 12px", fontWeight: 950 };

  if (isExpiredCameraSession) {
    return (
      <div style={shellStyle}>
        <div style={{ maxWidth: 680, margin: "0 auto", padding: 14, display: "grid", gap: 12 }}>
          <div style={{ ...cardStyle, padding: 18, display: "grid", gap: 13 }}>
            <div style={{ fontSize: 11, fontWeight: 950, color: "#16a34a", letterSpacing: ".08em", textTransform: "uppercase" }}>PromoBolsillo Cámara</div>
            <h1 style={{ margin: 0, fontSize: 25, lineHeight: 1.08 }}>Sesión de cámara vencida</h1>
            <div style={{ borderRadius: 18, background: "#fee2e2", color: "#991b1b", padding: 13, fontWeight: 900, lineHeight: 1.35 }}>
              Por seguridad, esta cámara ya no está activa.
            </div>
            <div style={{ borderRadius: 18, background: "#f8fafc", color: "#334155", padding: 13, fontWeight: 850, lineHeight: 1.4 }}>
              Cierra esta pantalla y vuelve a abrir la cámara desde <strong>Capturar evidencias</strong>. Esto evita usar ligas viejas o sesiones restauradas por Telegram/Android.
            </div>
            <button type="button" onClick={closeExternalCameraSafely} style={{ border: 0, borderRadius: 20, background: "#16a34a", color: "#fff", padding: "16px 18px", fontWeight: 1000, fontSize: 17 }}>
              Cerrar y volver a PromoBolsillo
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (finished) {
    return (
      <div style={shellStyle}>
        <div style={{ maxWidth: 680, margin: "0 auto", padding: 14, display: "grid", gap: 12 }}>
          <div style={{ ...cardStyle, padding: 18, display: "grid", gap: 12 }}>
            <div style={{ fontSize: 11, fontWeight: 950, color: "#16a34a", letterSpacing: ".08em", textTransform: "uppercase" }}>PromoBolsillo Cámara</div>
            <h1 style={{ margin: 0, fontSize: 27, lineHeight: 1.05 }}>Captura finalizada</h1>
            <div style={{ color: "#64748b", fontWeight: 900 }}>{store}</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8 }}>
              <div style={{ borderRadius: 18, background: "#f8fafc", padding: 12, textAlign: "center" }}><strong style={{ fontSize: 26 }}>{uploadedCount}</strong><br /><span style={{ fontSize: 12, fontWeight: 900, color: "#64748b" }}>fotos</span></div>
              <div style={{ borderRadius: 18, background: "#f8fafc", padding: 12, textAlign: "center" }}><strong style={{ fontSize: 26 }}>{annulledCount}</strong><br /><span style={{ fontSize: 12, fontWeight: 900, color: "#64748b" }}>anuladas</span></div>
              <div style={{ borderRadius: 18, background: "#f8fafc", padding: 12, textAlign: "center" }}><strong style={{ fontSize: 26 }}>{outOfServiceCount}</strong><br /><span style={{ fontSize: 12, fontWeight: 900, color: "#64748b" }}>sin servicio</span></div>
            </div>
            <div style={{ borderRadius: 18, background: "#dcfce7", color: "#166534", padding: 13, fontWeight: 900 }}>
              Listo. Las fotos ya fueron enviadas al registro de evidencias.
            </div>
            <button type="button" onClick={closeExternalCameraSafely} style={{ border: 0, borderRadius: 20, background: "#16a34a", color: "#fff", padding: "16px 18px", fontWeight: 1000, fontSize: 17 }}>
              Volver y revisar evidencias
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={shellStyle}>
      <div style={{ maxWidth: 680, margin: "0 auto", padding: 12, display: "grid", gap: 10 }}>
        <div style={{ ...cardStyle, padding: 14, position: "sticky", top: 0, zIndex: 5 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
            <div>
              <div style={{ fontSize: 11, fontWeight: 950, color: "#16a34a", letterSpacing: ".08em", textTransform: "uppercase" }}>PromoBolsillo Cámara</div>
              <h1 style={{ margin: "5px 0 4px", fontSize: 23, lineHeight: 1.05 }}>Captura de evidencias</h1>
              <div style={{ color: "#64748b", fontWeight: 900, lineHeight: 1.2 }}>{store}</div>
            </div>
            <div style={{ textAlign: "right", minWidth: 74 }}>
              <div style={{ fontSize: 28, fontWeight: 1000, color: "#0f172a", lineHeight: 1 }}>{uploadedCount}</div>
              <div style={{ fontSize: 11, fontWeight: 900, color: "#64748b" }}>fotos</div>
            </div>
          </div>
        </div>

        <div style={{ ...cardStyle, padding: 12, display: "grid", gap: 10 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            <label style={{ display: "grid", minWidth: 0 }}>
              <span style={labelStyle}>Marca</span>
              <select style={selectStyle} value={selectedMarcaId} onChange={(e) => setSelectedMarcaId(e.target.value)}>
                <option value="">Selecciona marca</option>
                {marcas.map((item) => <option key={item.marca_id} value={item.marca_id}>{item.marca_nombre}</option>)}
              </select>
            </label>
            <label style={{ display: "grid", minWidth: 0 }}>
              <span style={labelStyle}>Tipo</span>
              <select style={selectStyle} value={selectedTipo} onChange={(e) => setSelectedTipo(e.target.value)}>
                <option value="">Selecciona tipo</option>
                {tipos.map((item) => <option key={item.tipo_evidencia} value={item.tipo_evidencia}>{item.tipo_evidencia}</option>)}
              </select>
            </label>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 8 }}>
            <label style={{ display: "grid" }}>
              <span style={labelStyle}>Estado</span>
              <select style={selectStyle} value={selectedFase} onChange={(e) => setSelectedFase(e.target.value as EvidencePhase)}>
                <option value="ESTADO_ACTUAL">Estado actual</option>
                <option value="ANTES">Antes</option>
                <option value="DESPUES">Después</option>
              </select>
            </label>
            <input style={selectStyle} value={descripcion} onChange={(e) => setDescripcion(e.target.value)} placeholder="Comentario opcional: precio visible, anaquel completo..." />
          </div>
          <div style={{ borderRadius: 16, background: "#f8fafc", border: "1px solid rgba(15,23,42,.08)", padding: 10, fontWeight: 850, color: "#334155", lineHeight: 1.3, fontSize: 13 }}>
            Capturando: <strong>{marcaNombre}</strong> · {tipoLabel} · {faseLabel}
          </div>
          <div style={{ borderTop: "1px solid rgba(15,23,42,.08)", paddingTop: 10, display: "grid", gap: 8 }}>
            <button type="button" onClick={() => setOutOfServiceOpen((prev) => !prev)} style={{ ...smallButtonStyle, textAlign: "left", background: selectedBrandMarkedOutOfService ? "#ecfdf5" : "#fff" }}>
              ¿Esta marca está fuera de servicio?
            </button>
            <div style={{ fontSize: 12, color: "#64748b", fontWeight: 800 }}>
              Aplica solo a la marca seleccionada: <strong>{marcaNombre}</strong>.
            </div>
            {selectedBrandMarkedOutOfService ? (
              <div style={{ borderRadius: 14, background: "#dcfce7", color: "#166534", padding: 10, fontWeight: 900 }}>
                Esta marca ya quedó marcada fuera de servicio en esta visita. Cambia de marca para seguir capturando evidencias.
              </div>
            ) : null}
            {outOfServiceOpen ? (
              <div style={{ display: "grid", gap: 8 }}>
                <div style={{ fontWeight: 850, color: "#334155" }}>Marca: {marcaNombre}</div>
                <select style={selectStyle} value={outOfServiceReason} onChange={(e) => setOutOfServiceReason(e.target.value)}>
                  {marcaFueraServicioMotivos.map((motivo) => <option key={motivo} value={motivo}>{motivo}</option>)}
                </select>
                <input style={selectStyle} value={outOfServiceComment} onChange={(e) => setOutOfServiceComment(e.target.value)} placeholder="Comentario opcional" />
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                  <button type="button" onClick={() => void markOutOfServiceFromCamera()} style={{ border: 0, borderRadius: 16, background: "#0f172a", color: "#fff", padding: "12px 14px", fontWeight: 950 }}>
                    Registrar sin servicio
                  </button>
                  <button type="button" onClick={() => setOutOfServiceOpen(false)} style={smallButtonStyle}>Cancelar</button>
                </div>
              </div>
            ) : null}
          </div>
        </div>

        {error ? <div style={{ borderRadius: 18, background: "#fee2e2", color: "#991b1b", padding: 13, fontWeight: 900 }}>{error}</div> : null}
        {status ? <div style={{ borderRadius: 18, background: status.includes("registrada") || status.includes("finalizada") || status.includes("anulada") || status.includes("fuera de servicio") ? "#dcfce7" : "#e0f2fe", color: status.includes("registrada") || status.includes("finalizada") || status.includes("anulada") || status.includes("fuera de servicio") ? "#166534" : "#075985", padding: 13, fontWeight: 900 }}>{status}</div> : null}

        <div style={{ ...cardStyle, padding: 12, display: "grid", gap: 10 }}>
          {preview ? <img src={preview} alt="Última evidencia" style={{ width: "100%", maxHeight: "58vh", objectFit: "contain", borderRadius: 22, border: "1px solid rgba(15,23,42,.10)", background: "#0f172a" }} /> : (
            <div style={{ minHeight: 260, borderRadius: 22, border: "1px dashed rgba(15,23,42,.18)", background: "linear-gradient(135deg,#f8fafc,#e2e8f0)", display: "grid", placeItems: "center", textAlign: "center", padding: 20, color: "#475569", fontWeight: 900 }}>
              Selecciona marca/tipo y toca Capturar evidencia.
            </div>
          )}
          <button
            type="button"
            disabled={!canCapture}
            onClick={() => nativeCameraRef.current?.click()}
            style={{ border: 0, borderRadius: 22, background: !canCapture ? "#94a3b8" : "#22c55e", color: "#fff", padding: "18px 20px", fontWeight: 1000, fontSize: 20, boxShadow: "0 12px 24px rgba(34,197,94,.26)" }}
          >
            {uploading ? "Registrando..." : "Capturar evidencia"}
          </button>
          <input ref={nativeCameraRef} type="file" accept="image/*" capture="environment" style={{ display: "none" }} onChange={(event) => void handleNativeEvidenceSelection(event.target.files)} />
          {!canCapture && !uploading ? <div style={{ color: "#92400e", background: "#fffbeb", borderRadius: 14, padding: 12, fontWeight: 850 }}>{selectedBrandMarkedOutOfService ? "Esta marca está marcada fuera de servicio. Cambia de marca para seguir capturando." : "Selecciona marca y tipo para habilitar la cámara."}</div> : null}
          {lastRegistered ? (
            <button type="button" onClick={() => void cancelExternalPhoto(lastRegistered)} style={{ ...smallButtonStyle, color: "#991b1b", background: "#fff7f7" }}>
              Deshacer última foto
            </button>
          ) : null}
        </div>

        {activeRecentPhotos.length ? (
          <div style={{ ...cardStyle, padding: 12, display: "grid", gap: 10 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
              <div style={{ fontWeight: 950 }}>Últimas capturas</div>
              <button type="button" onClick={() => setShowAllPhotos((prev) => !prev)} style={{ border: 0, background: "transparent", color: "#2563eb", fontWeight: 950 }}>
                {showAllPhotos ? "Ver menos" : "Ver todas"}
              </button>
            </div>
            <div style={showAllPhotos ? { display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 8 } : { display: "flex", gap: 8, overflowX: "auto", paddingBottom: 4 }}>
              {activeRecentPhotos.slice(0, showAllPhotos ? 24 : 8).map((item, index) => (
                <div key={`${item.evidencia_id || item.name}-${index}`} style={{ minWidth: showAllPhotos ? undefined : 112, borderRadius: 16, overflow: "hidden", background: "#f8fafc", border: "1px solid rgba(15,23,42,.08)" }}>
                  <img src={item.dataUrl} alt={item.name} style={{ width: "100%", aspectRatio: "1", objectFit: "cover", display: "block" }} />
                  <div style={{ padding: 7, fontSize: 10, fontWeight: 850, color: item.status === "ERROR" ? "#991b1b" : "#475569", lineHeight: 1.2 }}>
                    {item.marca}<br />{item.tipo}<br />{item.status === "SUBIENDO" ? "Subiendo..." : item.status === "ERROR" ? "Error" : "Registrada"}
                  </div>
                  {item.status === "REGISTRADA" ? (
                    <button type="button" onClick={() => void cancelExternalPhoto(item)} style={{ width: "100%", border: 0, background: "#fee2e2", color: "#991b1b", padding: "7px 6px", fontWeight: 900, fontSize: 11 }}>
                      Anular
                    </button>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, paddingBottom: 16 }}>
          <button type="button" onClick={() => nativeCameraRef.current?.click()} disabled={!canCapture} style={smallButtonStyle}>
            Tomar otra
          </button>
          <button type="button" onClick={finishCapture} style={{ border: 0, borderRadius: 18, background: "#0f172a", color: "#fff", padding: "14px 16px", fontWeight: 950 }}>
            Finalizar
          </button>
        </div>
      </div>
    </div>
  );
}

// E014E_FIX3_EXPORT_AND_STYLE_FIX: corrige export default function App y estilos del header/boton.
// E014E_REZGO_LOGO_HEADER: Logo oficial REZGO + frase Pasión por la movilidad.
const REZGO_HORIZONTAL_LOGO_E014E = "data:image/jpeg;base64,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";

export default function App() {
  const tg = getTelegramWebApp();
  const externalCameraParams = typeof window !== "undefined" ? new URLSearchParams(window.location.search) : null;
  const isExternalCameraMode = externalCameraParams?.get("external_camera") === "1";
  const externalCameraToken = externalCameraParams?.get("token") || "";
  const miniAppReturnTarget = parseMiniAppReturnStartParam(getTelegramStartParam());

  const [role, setRole] = useState<AppRole>(null);
  const [actorLabel, setActorLabel] = useState("Operador");
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState("");
  const [detectedExternalId, setDetectedExternalId] = useState("");
  const [statusMsg, setStatusMsg] = useState("");
  const [statusMsgDuration, setStatusMsgDuration] = useState(6800);
  const manualRefreshInFlightRef = useRef(false);
  const promotorResumeRefreshInFlightRef = useRef(false);
  const promotorWasHiddenRef = useRef(false);
  const supervisorReviewSyncInFlightRef = useRef(false);
  const supervisorReviewFlushTimerRef = useRef<number | null>(null);
  const supervisorReviewUndoTimerRef = useRef<number | null>(null);

  const [stores, setStores] = useState<StoreItem[]>([]);
  const [visits, setVisits] = useState<VisitItem[]>([]);
  const [selectedStoreId, setSelectedStoreId] = useState("");
  const [selectedVisitId, setSelectedVisitId] = useState("");
  const [promotorModule, setPromotorModule] = useState<PromotorModule>("asistencia");

  const [entryLocation, setEntryLocation] = useState<LocationCapture | null>(null);
  const [, setExitLocation] = useState<LocationCapture | null>(null);
  const [entryPhoto, setEntryPhoto] = useState<PhotoCapture | null>(null);
  const [, setExitPhoto] = useState<PhotoCapture | null>(null);
  const [capturingLocation, setCapturingLocation] = useState<CaptureKind | null>(null);
  const [, setCapturingPhoto] = useState<CameraTarget | null>(null);

  const [evidenceBrandId, setEvidenceBrandId] = useState("");
  const [evidenceBrandLabel, setEvidenceBrandLabel] = useState("");
  const [evidenceType, setEvidenceType] = useState("");
  const [evidencePhase, setEvidencePhase] = useState<EvidencePhase>("ESTADO_ACTUAL");
  const [evidenceQty, setEvidenceQty] = useState(1);
  const [evidenceDescription, setEvidenceDescription] = useState("");
  const [evidencePhotos, setEvidencePhotos] = useState<PhotoCapture[]>([]);
  const [availableBrands, setAvailableBrands] = useState<Array<{ marca_id: string; marca_nombre: string }>>([]);
  const [brandsOutOfService, setBrandsOutOfService] = useState<Record<string, MarcaFueraServicioItem>>({});
  const [showOutOfServicePanel, setShowOutOfServicePanel] = useState(false);
  const [outOfServiceReason, setOutOfServiceReason] = useState("Marca de visita quincenal");
  const [outOfServiceComment, setOutOfServiceComment] = useState("");
  const [brandRules, setBrandRules] = useState<Array<{ tipo_evidencia: string; fotos_requeridas: number; requiere_antes_despues: boolean; orden?: number; obligatoria?: boolean; observaciones?: string }>>([]);
  const [selectedVisitStoreName, setSelectedVisitStoreName] = useState("");

  const [allEvidenceRows, setAllEvidenceRows] = useState<UiEvidence[]>([]);
  const [promotorOutOfServiceRows, setPromotorOutOfServiceRows] = useState<SupervisorOutOfServiceItem[]>([]);
  const [promotorEvidenceViewFilter, setPromotorEvidenceViewFilter] = useState<"fotos" | "fuera" | "todo">("fotos");
  const [promotorUsage, setPromotorUsage] = useState<PromotorUsageSummary>({});
  const [selectedEvidenceId, setSelectedEvidenceId] = useState("");
  const [selectedPromotorOutOfServiceId, setSelectedPromotorOutOfServiceId] = useState("");
  const [noteDraft, setNoteDraft] = useState("");
  const [promotorRecentAlerts, setPromotorRecentAlerts] = useState<PromotorRecentAlert[]>([]);
  const [pendingQueue, setPendingQueue] = useState<PendingQueueOp[]>([]);
  const [syncingPendingQueue, setSyncingPendingQueue] = useState(false);
  const [storeBrandsCache, setStoreBrandsCache] = useState<Record<string, Array<{ marca_id: string; marca_nombre: string }>>>(readStoreBrandsCacheStorage());
  const [attendanceGalleryAuth, setAttendanceGalleryAuth] = useState<GalleryAuthorizationInfo>({ allowed: false, authorization: null, debug: { reason: "NO_STORE" } });
  const [evidenceGalleryAuth, setEvidenceGalleryAuth] = useState<GalleryAuthorizationInfo>({ allowed: false, authorization: null, debug: { reason: "NO_VISIT" } });
  const [replaceGalleryAuth, setReplaceGalleryAuth] = useState<GalleryAuthorizationInfo>({ allowed: false, authorization: null, debug: { reason: "NO_EVIDENCE" } });
  const [evidenceFilterStore, setEvidenceFilterStore] = useState("");
  const [evidenceFilterBrand, setEvidenceFilterBrand] = useState("");
  const [evidenceFilterType, setEvidenceFilterType] = useState("");
  const [evidenceFilterPhase, setEvidenceFilterPhase] = useState("");
  const localEvidencePreviewsRef = useRef<Record<string, string>>({}); // E009A_LOCAL_PREVIEW_AFTER_REGISTER

  const [supervisorModule, setSupervisorModule] = useState<SupervisorModule>("evidencias");
  const [supervisorSummary, setSupervisorSummary] = useState<SupervisorSummary>({ promotores: 0, visitasHoy: 0, abiertas: 0, evidenciasHoy: 0, alertas: 0 });
  const [supervisorUsage, setSupervisorUsage] = useState<SupervisorUsageSummary>({});
  const [supervisorPendingClose, setSupervisorPendingClose] = useState<SupervisorPendingClose>({});
  const [supervisorTeam, setSupervisorTeam] = useState<SupervisorTeamRow[]>([]);
  const [selectedTeamPromotorId, setSelectedTeamPromotorId] = useState("");
  const [supervisorDayRoute, setSupervisorDayRoute] = useState<SupervisorDayRouteRow[]>([]);
  const [dayRouteLoading, setDayRouteLoading] = useState(false);
  const [selectedRouteVisitId, setSelectedRouteVisitId] = useState("");
  const [supervisorAlerts, setSupervisorAlerts] = useState<SupervisorAlert[]>([]);
  const [selectedAlertId, setSelectedAlertId] = useState("");
  const [alertStatusFilter, setAlertStatusFilter] = useState("");
  const [alertSeverityFilter, setAlertSeverityFilter] = useState("");
  const [alertPromotorFilter, setAlertPromotorFilter] = useState("");
  const [alertFinalStatus, setAlertFinalStatus] = useState<AlertFinalStatus>("RESUELTA");
  const [supervisorEvidences, setSupervisorEvidences] = useState<EvidenceItem[]>([]);
  const [selectedSupEvidenceId, setSelectedSupEvidenceId] = useState("");
  const [selectedSupEvidenceIds, setSelectedSupEvidenceIds] = useState<string[]>([]);
  const [supervisorPhotoCache, setSupervisorPhotoCache] = useState<Record<string, { thumb?: string; full?: string }>>({});
  const [supervisorPhotoLoadingIds, setSupervisorPhotoLoadingIds] = useState<string[]>([]);
  const [supervisorQueueVisibleCount, setSupervisorQueueVisibleCount] = useState(24);
  const [supervisorReviewQueue, setSupervisorReviewQueue] = useState<SupervisorReviewQueueItem[]>(() => sortSupervisorReviewQueue(readSupervisorReviewQueueStorage()));
  const [supervisorReviewSyncing, setSupervisorReviewSyncing] = useState(false);
  const [supervisorReviewLastError, setSupervisorReviewLastError] = useState("");
  const [lastSupervisorReviewUndo, setLastSupervisorReviewUndo] = useState<SupervisorReviewUndoState | null>(null);
  const [supervisorEvidenceAudit, setSupervisorEvidenceAudit] = useState<EvidenceAuditRow[]>([]);
  const [supervisorOutOfServiceRows, setSupervisorOutOfServiceRows] = useState<SupervisorOutOfServiceItem[]>([]);
  const [supReviewContentFilter, setSupReviewContentFilter] = useState<"evidencias" | "fuera" | "todo">("evidencias");
  const [supEvidencePromotorFilter, setSupEvidencePromotorFilter] = useState("");
  const [supEvidenceStoreFilter, setSupEvidenceStoreFilter] = useState("");
  const [supEvidenceBrandFilter, setSupEvidenceBrandFilter] = useState("");
  const [supEvidenceTypeFilter, setSupEvidenceTypeFilter] = useState("");
  const [supEvidencePhaseFilter, setSupEvidencePhaseFilter] = useState("");
  const [supEvidenceRiskFilter, setSupEvidenceRiskFilter] = useState("");
  const [supEvidenceStatusFilter, setSupEvidenceStatusFilter] = useState("PENDIENTE");
  const [supEvidenceOnlyPending, setSupEvidenceOnlyPending] = useState(false);
  const [reviewActionInProgress, setReviewActionInProgress] = useState<SupervisorDecision | null>(null);
  const [supEvidenceDatePreset, setSupEvidenceDatePreset] = useState<"hoy" | "semana" | "rango">("hoy");
  const [supEvidenceDateStart, setSupEvidenceDateStart] = useState(localYmd());
  const [supEvidenceDateEnd, setSupEvidenceDateEnd] = useState(localYmd());
  const [draftSupEvidenceDatePreset, setDraftSupEvidenceDatePreset] = useState<"hoy" | "semana" | "rango">("hoy");
  const [draftSupEvidenceDateStart, setDraftSupEvidenceDateStart] = useState(localYmd());
  const [draftSupEvidenceDateEnd, setDraftSupEvidenceDateEnd] = useState(localYmd());
  const [draftSupReviewContentFilter, setDraftSupReviewContentFilter] = useState<"evidencias" | "fuera" | "todo">("evidencias");
  const [draftSupEvidenceStatusFilter, setDraftSupEvidenceStatusFilter] = useState("PENDIENTE");
  const [supEvidenceGroupMode, setSupEvidenceGroupMode] = useState<EvidenceGroupMode>("marca");
  const [activeSupEvidenceGroupKey, setActiveSupEvidenceGroupKey] = useState("");
  const [supEvidenceGroupPage, setSupEvidenceGroupPage] = useState(1);
  const [reviewDecision, setReviewDecision] = useState<SupervisorDecision>("APROBADA");
  const [reviewNote, setReviewNote] = useState("");
  const [alertCloseNote, setAlertCloseNote] = useState("");
  const [expedient, setExpedient] = useState<VisitExpedientResponse | null>(null);
  const [expedientLoading, setExpedientLoading] = useState(false);

  const [clientModule, setClientModule] = useState<ClientModule>("resumen");
  const [clientBranding, setClientBranding] = useState<{ cliente_nombre?: string; logo_url?: string; color_primario?: string }>({});
  const [clientFilterOptions, setClientFilterOptions] = useState<{
    cadenas: ClientFilterOption[];
    regiones: ClientFilterOption[];
    tiendas: ClientFilterOption[];
    marcas: ClientFilterOption[];
    tipos_evidencia: ClientFilterOption[];
    riesgos: ClientFilterOption[];
    decisiones: ClientFilterOption[];
    severidades: ClientFilterOption[];
    estatus_alerta: ClientFilterOption[];
  }>({ cadenas: [], regiones: [], tiendas: [], marcas: [], tipos_evidencia: [], riesgos: [], decisiones: [], severidades: [], estatus_alerta: [] });
  const [clientFilters, setClientFilters] = useState({
    fecha_inicio: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, "0")}-01`,
    fecha_fin: new Date().toISOString().slice(0, 10),
    cadena: "",
    region: "",
    tienda_id: "",
    marca_id: "",
    tipo_evidencia: "",
    fase: "",
    decision_supervisor: "",
    riesgo: "",
    tipo_alerta: "",
    severidad: "",
    status: "",
  });
  const [clientDashboard, setClientDashboard] = useState<ClientDashboardData>({});
  const [clientStores, setClientStores] = useState<ClientStoreRow[]>([]);
  const [selectedClientStoreId, setSelectedClientStoreId] = useState("");
  const [clientStoreDetail, setClientStoreDetail] = useState<ClientStoreDetail | null>(null);
  const [clientEvidences, setClientEvidences] = useState<EvidenceItem[]>([]);
  const [clientEvidenceGroupMode, setClientEvidenceGroupMode] = useState<EvidenceGroupMode>("marca");
  const [activeClientEvidenceGroupKey, setActiveClientEvidenceGroupKey] = useState("");
  const [clientEvidenceGroupPage, setClientEvidenceGroupPage] = useState(1);
  const [clientIncidents, setClientIncidents] = useState<SupervisorAlert[]>([]);
  const [clientDeliverablesMessage, setClientDeliverablesMessage] = useState("");
  const [imageViewerSrc, setImageViewerSrc] = useState("");
  const [imageViewerEvidenceId, setImageViewerEvidenceId] = useState("");
  const [imageViewerScale, setImageViewerScale] = useState(1);
  const [imageViewerOffset, setImageViewerOffset] = useState({ x: 0, y: 0 });
  const [imageViewerDragging, setImageViewerDragging] = useState(false);
  const [cameraModal, setCameraModal] = useState<{ open: boolean; target: CameraTarget | null; facing: "user" | "environment" }>({ open: false, target: null, facing: "environment" });
  const [cameraZoom, setCameraZoom] = useState<{ supported: boolean; min: number; max: number; step: number; value: number }>({ supported: false, min: 1, max: 1, step: 0.1, value: 1 });
  const [captureGuardMsg, setCaptureGuardMsg] = useState("");
  const cameraVideoRef = useRef<HTMLVideoElement | null>(null);
  const cameraStreamRef = useRef<MediaStream | null>(null);
  const supervisorQueueTopRef = useRef<HTMLDivElement | null>(null);
  const supervisorQueueBottomRef = useRef<HTMLDivElement | null>(null);
  const supervisorReviewDetailRef = useRef<HTMLElement | null>(null);
  const promotorListTopRef = useRef<HTMLDivElement | null>(null);
  const promotorListBottomRef = useRef<HTMLDivElement | null>(null);
  const promotorDetailRef = useRef<HTMLDivElement | null>(null);
  const promotorGalleryScrollRef = useRef<HTMLDivElement | null>(null);
  const lastImageTapRef = useRef<{ src: string; at: number }>({ src: "", at: 0 });
  const imageViewerTouchRef = useRef<{ distance: number; startScale: number; dragging: boolean; dragStartX: number; dragStartY: number; originX: number; originY: number }>({ distance: 0, startScale: 1, dragging: false, dragStartX: 0, dragStartY: 0, originX: 0, originY: 0 });
  const attendancePhotoRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const entryGalleryInputRef = useRef<HTMLInputElement | null>(null);
  const evidenceGalleryInputRef = useRef<HTMLInputElement | null>(null);
  const replaceGalleryInputRef = useRef<HTMLInputElement | null>(null);
  const entryNativeCameraInputRef = useRef<HTMLInputElement | null>(null);
  const evidenceNativeCameraInputRef = useRef<HTMLInputElement | null>(null);
  const replaceNativeCameraInputRef = useRef<HTMLInputElement | null>(null);


  // E024 mantiene helpers heredados usados por flujos anteriores, aunque la pantalla de captura quedó simplificada.
  void showOutOfServicePanel;
  void setOutOfServiceReason;
  void evidenceGalleryAuth;
  void evidenceGalleryInputRef;
  void evidenceNativeCameraInputRef;

  useEffect(() => {
    if (tg) {
      tg.ready?.();
      tg.expand?.();
      tg.setHeaderColor?.("#f4f5f7");
      tg.setBackgroundColor?.("#eef1f4");
    }
  }, [tg]);

  useEffect(() => {
    if (!statusMsg) return;
    const t = setTimeout(() => setStatusMsg(""), statusMsgDuration);
    return () => clearTimeout(t);
  }, [statusMsg, statusMsgDuration]);

  useEffect(() => {
    setPendingQueue(sortPendingQueue(readPendingQueueStorage()));
  }, []);

  useEffect(() => {
    writeStoreBrandsCacheStorage(storeBrandsCache);
  }, [storeBrandsCache]);

  useEffect(() => {
    const onOnline = () => {
      if (role === "promotor") {
        void syncPendingQueue(false);
      }
      if (role === "supervisor") {
        void syncSupervisorReviewQueue(false);
      }
    };
    window.addEventListener("online", onOnline);
    return () => window.removeEventListener("online", onOnline);
  }, [role, pendingQueue, supervisorReviewQueue]);

  useEffect(() => {
    const markPromotorHidden = () => {
      if (role === "promotor") promotorWasHiddenRef.current = true;
    };
    const resumeVisibleRole = () => {
      if (document.visibilityState !== "visible") {
        markPromotorHidden();
        return;
      }
      if (role === "promotor" && promotorWasHiddenRef.current) {
        promotorWasHiddenRef.current = false;
        window.setTimeout(() => { void refreshPromotorAfterCameraReturn(); }, 350);
      }
      if (role === "supervisor") {
        void syncSupervisorReviewQueue(false);
        void loadSupervisorDashboard();
        void loadSupervisorTeam();
        void loadSupervisorAlerts();
        void loadSupervisorEvidences();
        void loadSupervisorOutOfService();
      }
    };
    const onFocus = () => resumeVisibleRole();
    const onBlur = () => markPromotorHidden();
    document.addEventListener("visibilitychange", resumeVisibleRole);
    window.addEventListener("focus", onFocus);
    window.addEventListener("blur", onBlur);
    return () => {
      document.removeEventListener("visibilitychange", resumeVisibleRole);
      window.removeEventListener("focus", onFocus);
      window.removeEventListener("blur", onBlur);
    };
  }, [role, alertStatusFilter, alertSeverityFilter, alertPromotorFilter, supEvidencePromotorFilter, supEvidenceStoreFilter, supEvidenceBrandFilter, supEvidenceTypeFilter, supEvidenceRiskFilter]);

  useEffect(() => () => { void stopCameraStream(); }, []);

  const pendingVisits = useMemo<VisitItem[]>(() => {
    const visitMap = new Map<string, VisitItem>();
    visits.forEach((visit) => visitMap.set(visit.visita_id, { ...visit }));
    for (const item of sortPendingQueue(pendingQueue)) {
      if (item.kind === "entry" && item.localVisitId) {
        visitMap.set(item.localVisitId, {
          visita_id: item.localVisitId,
          tienda_id: item.tienda_id,
          tienda_nombre: item.tienda_nombre,
          tienda_display: formatStoreDisplay(item.tienda_id, item.tienda_nombre),
          hora_inicio: item.createdAt,
          hora_fin: "",
          estado_visita: item.status,
          resultado_geocerca_entrada: "PENDIENTE_ENVIO",
          resultado_geocerca_salida: "",
        });
      }
      if (item.kind === "close" && item.visitaId) {
        const existing = visitMap.get(item.visitaId);
        if (existing) {
          visitMap.set(item.visitaId, {
            ...existing,
            hora_fin: item.createdAt,
            estado_visita: item.status,
            resultado_geocerca_salida: existing.resultado_geocerca_salida || "PENDIENTE_ENVIO",
          });
        }
      }
    }
    return Array.from(visitMap.values()).sort((a, b) => String(a.hora_inicio).localeCompare(String(b.hora_inicio)));
  }, [visits, pendingQueue]);
  const openVisits = useMemo(() => pendingVisits.filter((v) => !v.hora_fin), [pendingVisits]);
  const exitVisit = useMemo(() => openVisits.find((v) => v.visita_id === selectedVisitId) || openVisits[0] || null, [openVisits, selectedVisitId]);
  const hasOpenVisit = Boolean(exitVisit);
  const selectedVisitHasNoBrands = Boolean(selectedVisitId && selectedVisitStoreName && availableBrands.length === 0);
  const selectedBrandOutOfService = evidenceBrandId ? brandsOutOfService[evidenceBrandId] : null;
  const promotorOutOfServiceVisibleRows = useMemo(() => {
    return promotorOutOfServiceRows.filter((item) => {
      const storeLabel = item.tienda_display || item.tienda_nombre || item.tienda_id || "";
      const brandLabel = item.marca_nombre || item.marca_id || "";
      const byStore = !evidenceFilterStore || storeLabel === evidenceFilterStore;
      const byBrand = !evidenceFilterBrand || brandLabel === evidenceFilterBrand;
      return byStore && byBrand;
    });
  }, [promotorOutOfServiceRows, evidenceFilterStore, evidenceFilterBrand]);

  const selectedPromotorOutOfService = useMemo(() => {
    if (promotorEvidenceViewFilter === "fotos") return null;
    if (promotorEvidenceViewFilter === "todo" && selectedEvidenceId) return null;
    return promotorOutOfServiceVisibleRows.find((item) => item.registro_id === selectedPromotorOutOfServiceId) || (promotorEvidenceViewFilter === "fuera" ? promotorOutOfServiceVisibleRows[0] : null) || null;
  }, [promotorEvidenceViewFilter, promotorOutOfServiceVisibleRows, selectedPromotorOutOfServiceId, selectedEvidenceId]);

  const evidenceTypeOptions = useMemo(() => {
    return brandRules
      .filter((item, index, arr) => !!item.tipo_evidencia && arr.findIndex((row) => row.tipo_evidencia === item.tipo_evidencia) === index)
      .sort((a, b) => Number(a.orden || 999) - Number(b.orden || 999) || String(a.tipo_evidencia).localeCompare(String(b.tipo_evidencia)));
  }, [brandRules]);
  const evidencePhaseOptions = useMemo(() => ["ESTADO_ACTUAL", "ANTES", "DESPUES"] as EvidencePhase[], []);

  // E024 usa selección de marca/tipo dentro de cámara externa; estas opciones se conservan para lógica heredada.
  void evidenceTypeOptions;
  void evidencePhaseOptions;

  const pendingEvidenceRows = useMemo<UiEvidence[]>(() => {
    const rows: UiEvidence[] = [];
    for (const item of pendingQueue) {
      if (item.kind === "entry") {
        const photo = item.payload?.entryPhoto as PhotoCapture | undefined;
        if (!photo?.dataUrl) continue;
        rows.push({
          evidencia_id: `PEND-${item.id}`,
          visita_id: item.localVisitId,
          tipo_evento: "ASISTENCIA_ENTRADA",
          tipo_evidencia: "ASISTENCIA",
          marca_nombre: "",
          riesgo: "PENDIENTE",
          fecha_hora_fmt: formatDateTimeMaybe(item.createdAt),
          fecha_hora: item.createdAt,
          url_foto: photo.dataUrl,
          descripcion: "Pendiente por enviar",
          tienda_id: item.tienda_id,
          tienda_nombre: item.tienda_nombre,
          tienda_display: formatStoreDisplay(item.tienda_id, item.tienda_nombre),
          status: item.status,
        });
      }
      if (item.kind === "evidence") {
        const photos = Array.isArray(item.payload?.fotos) ? (item.payload.fotos as PhotoCapture[]) : [];
        photos.forEach((photo, idx) => {
          rows.push({
            evidencia_id: `PEND-${item.id}-${idx}`,
            visita_id: item.visitaId,
            tipo_evento: "EVIDENCIA_PENDIENTE",
            tipo_evidencia: String(item.payload?.tipo_evidencia || "Evidencia"),
            marca_id: item.payload?.marca_id,
            marca_nombre: normalizeBrandLabel(String(item.payload?.marca_nombre || ""), String(item.payload?.marca_id || "Marca")),
            riesgo: "PENDIENTE",
            fecha_hora_fmt: formatDateTimeMaybe(item.createdAt),
            fecha_hora: item.createdAt,
            url_foto: photo.dataUrl,
            descripcion: item.status === "ERROR_ENVIO" ? `Error pendiente: ${item.lastError || "Reintentar envío"}` : "Pendiente por enviar",
            tienda_id: item.tienda_id,
            tienda_nombre: item.tienda_nombre,
            tienda_display: formatStoreDisplay(item.tienda_id, item.tienda_nombre),
            fase: item.payload?.fase,
            status: item.status,
          });
        });
      }
    }
    return rows;
  }, [pendingQueue]);

  const mergedEvidenceRows = useMemo(() => [...pendingEvidenceRows, ...allEvidenceRows], [pendingEvidenceRows, allEvidenceRows]);
  const attendanceGallery = useMemo(() => mergedEvidenceRows.filter((item) => !isOperationalEvidence(item)), [mergedEvidenceRows]);
  const operationalGallery = useMemo(() => mergedEvidenceRows.filter((item) => isOperationalEvidence(item) && String(item.status || "ACTIVA").toUpperCase() !== "ANULADA"), [mergedEvidenceRows]);

  const currentVisitEvidenceGallery = useMemo(() => {
    return operationalGallery.filter((item) => !selectedVisitId || item.visita_id === selectedVisitId);
  }, [operationalGallery, selectedVisitId]);

  const evidenceFilterOptions = useMemo(() => {
    const storeRows = operationalGallery;
    const brandRows = evidenceFilterStore ? storeRows.filter((item) => getStoreDisplayFromItem(item) === evidenceFilterStore) : storeRows;
    const typeRows = evidenceFilterBrand ? brandRows.filter((item) => normalizeBrandLabel(item.marca_nombre || "", "Marca") === evidenceFilterBrand) : brandRows;
    const phaseRows = evidenceFilterType ? typeRows.filter((item) => (item.tipo_evidencia || "") === evidenceFilterType) : typeRows;
    return {
      stores: Array.from(new Set(storeRows.map((item) => getStoreDisplayFromItem(item)).filter(Boolean))).sort(),
      brands: Array.from(new Set(brandRows.map((item) => normalizeBrandLabel(item.marca_nombre || "", "Marca")).filter(Boolean))).sort(),
      types: Array.from(new Set(typeRows.map((item) => item.tipo_evidencia || "").filter(Boolean))).sort(),
      phases: Array.from(new Set(phaseRows.map((item) => item.fase || "").filter(Boolean))).sort(),
    };
  }, [operationalGallery, evidenceFilterStore, evidenceFilterBrand, evidenceFilterType]);

  const filteredOperationalGallery = useMemo(() => {
    return operationalGallery.filter((item) => {
      const byStore = !evidenceFilterStore || getStoreDisplayFromItem(item) === evidenceFilterStore;
      const byBrand = !evidenceFilterBrand || normalizeBrandLabel(item.marca_nombre || "", "Marca") === evidenceFilterBrand;
      const byType = !evidenceFilterType || (item.tipo_evidencia || "") === evidenceFilterType;
      const byPhase = !evidenceFilterPhase || (item.fase || "") === evidenceFilterPhase;
      return byStore && byBrand && byType && byPhase;
    });
  }, [operationalGallery, evidenceFilterStore, evidenceFilterBrand, evidenceFilterType, evidenceFilterPhase]);

  const selectedEvidence = useMemo(() => {
    if (promotorEvidenceViewFilter === "fuera") return null;
    if (promotorEvidenceViewFilter === "todo" && selectedPromotorOutOfServiceId) return null;
    return filteredOperationalGallery.find((item) => item.evidencia_id === selectedEvidenceId) || (promotorEvidenceViewFilter === "fotos" ? filteredOperationalGallery[0] : null) || null;
  }, [filteredOperationalGallery, selectedEvidenceId, promotorEvidenceViewFilter, selectedPromotorOutOfServiceId]);


  useEffect(() => {
    if (promotorEvidenceViewFilter === "fuera") setSelectedEvidenceId("");
    if (promotorEvidenceViewFilter === "fotos") setSelectedPromotorOutOfServiceId("");
  }, [promotorEvidenceViewFilter]);
  function isStoredPhotoUnavailable(value?: string) {
    const textValue = String(value || "").trim();
    return !textValue || textValue === "[DRIVE_UPLOAD_FAILED]" || textValue.startsWith("[") || textValue.startsWith("PHOTO_STORAGE:");
  }

  function rememberLocalEvidencePreviews(entries: Record<string, string>) {
    const cleanEntries = Object.entries(entries).filter(([id, dataUrl]) => Boolean(id && dataUrl));
    if (!cleanEntries.length) return;
    localEvidencePreviewsRef.current = { ...localEvidencePreviewsRef.current, ...Object.fromEntries(cleanEntries) };
  }

  function withLocalEvidencePreviews<T extends UiEvidence | EvidenceItem>(rows: T[]): T[] {
    const previewMap = localEvidencePreviewsRef.current;
    return rows.map((row) => {
      const localPreview = previewMap[row.evidencia_id];
      if (localPreview && isStoredPhotoUnavailable(row.url_foto)) {
        return { ...row, url_foto: localPreview, descripcion: row.descripcion || "Vista previa local" };
      }
      return row;
    });
  }

  useEffect(() => {
    if (role !== "promotor") return;
    if (!selectedStoreId) {
      setAttendanceGalleryAuth({ allowed: false, authorization: null, debug: { reason: "NO_STORE" } });
      return;
    }
    void loadGalleryAuthorization("attendance", {
      tienda_id: selectedStoreId,
      visita_id: "",
      marca_id: "",
      tipo_evidencia: "ASISTENCIA",
    }, setAttendanceGalleryAuth);
  }, [role, selectedStoreId]);

  useEffect(() => {
    if (role !== "promotor") return;
    if (!selectedVisitId) {
      setEvidenceGalleryAuth({ allowed: false, authorization: null, debug: { reason: "NO_VISIT" } });
      return;
    }
    const selectedVisit = pendingVisits.find((item) => item.visita_id === selectedVisitId);
    void loadGalleryAuthorization("evidence", {
      tienda_id: selectedVisit?.tienda_id || "",
      visita_id: isLocalVisitId(selectedVisitId) ? "" : selectedVisitId,
      marca_id: evidenceBrandId,
      tipo_evidencia: evidenceType,
    }, setEvidenceGalleryAuth);
  }, [role, selectedVisitId, evidenceBrandId, evidenceType, pendingVisits]);

  useEffect(() => {
    if (role !== "promotor") return;
    if (!selectedEvidence || String(selectedEvidence.status || "").toUpperCase().startsWith("PEND")) {
      setReplaceGalleryAuth({ allowed: false, authorization: null, debug: { reason: "NO_EVIDENCE" } });
      return;
    }
    void loadGalleryAuthorization("replace", {
      tienda_id: selectedEvidence.tienda_id || "",
      visita_id: selectedEvidence.visita_id || "",
      marca_id: selectedEvidence.marca_id || "",
      tipo_evidencia: selectedEvidence.tipo_evidencia || "",
    }, setReplaceGalleryAuth);
  }, [role, selectedEvidence]);


  const supervisorPromotorOptions = useMemo(() => supervisorTeam.map((item) => ({ id: item.promotor_id, nombre: item.nombre })), [supervisorTeam]);

  const visibleSupervisorEvidencesBase = useMemo(() => supervisorEvidences, [supervisorEvidences]);

  const supervisorDateBounds = useMemo(() => {
    if (supEvidenceDatePreset === "semana") return { start: startOfWeekMondayYmd(), end: endOfWeekSundayYmd(), label: "Semana actual" };
    if (supEvidenceDatePreset === "rango") return { start: supEvidenceDateStart, end: supEvidenceDateEnd || supEvidenceDateStart, label: `${supEvidenceDateStart || "Inicio"} a ${supEvidenceDateEnd || "Fin"}` };
    return { start: localYmd(), end: localYmd(), label: "Hoy" };
  }, [supEvidenceDatePreset, supEvidenceDateStart, supEvidenceDateEnd]);

  const filteredSupervisorEvidences = useMemo(() => visibleSupervisorEvidencesBase.filter((item) => {
    const itemYmd = getEvidenceYmd(item);
    const byDate = !itemYmd || (itemYmd >= supervisorDateBounds.start && itemYmd <= supervisorDateBounds.end);
    const byPromotor = !supEvidencePromotorFilter || item.promotor_id === supEvidencePromotorFilter;
    const byStore = !supEvidenceStoreFilter || getStoreDisplayFromItem(item) === supEvidenceStoreFilter;
    const byBrand = !supEvidenceBrandFilter || normalizeBrandLabel(item.marca_nombre || "", "Marca") === supEvidenceBrandFilter;
    const byType = !supEvidenceTypeFilter || (item.tipo_evidencia || "") === supEvidenceTypeFilter;
    const byPhase = !supEvidencePhaseFilter || String(item.fase || "").toUpperCase() === supEvidencePhaseFilter;
    const byRisk = !supEvidenceRiskFilter || (item.riesgo || "") === supEvidenceRiskFilter;
    const byStatus = !supEvidenceStatusFilter || getSupervisorReviewState(item) === supEvidenceStatusFilter;
    const byPending = !supEvidenceOnlyPending || isSupervisorPendingEvidence(item);
    return byDate && byPromotor && byStore && byBrand && byType && byPhase && byRisk && byStatus && byPending;
  }), [
    visibleSupervisorEvidencesBase,
    supervisorDateBounds,
    supEvidencePromotorFilter,
    supEvidenceStoreFilter,
    supEvidenceBrandFilter,
    supEvidenceTypeFilter,
    supEvidencePhaseFilter,
    supEvidenceRiskFilter,
    supEvidenceStatusFilter,
    supEvidenceOnlyPending,
  ]);

  const visibleSupervisorQueueItems = useMemo(
    () => filteredSupervisorEvidences.slice(0, supervisorQueueVisibleCount),
    [filteredSupervisorEvidences, supervisorQueueVisibleCount],
  );

  const filteredSupervisorOutOfServiceRows = useMemo(() => supervisorOutOfServiceRows.filter((item) => {
    const itemYmd = getOutOfServiceYmd(item);
    const byDate = !itemYmd || (itemYmd >= supervisorDateBounds.start && itemYmd <= supervisorDateBounds.end);
    const byPromotor = !supEvidencePromotorFilter || item.promotor_id === supEvidencePromotorFilter;
    const byStore = !supEvidenceStoreFilter || (item.tienda_display || item.tienda_nombre || item.tienda_id || "") === supEvidenceStoreFilter;
    const byBrand = !supEvidenceBrandFilter || (item.marca_nombre || item.marca_id || "") === supEvidenceBrandFilter;
    return byDate && byPromotor && byStore && byBrand;
  }), [supervisorOutOfServiceRows, supervisorDateBounds, supEvidencePromotorFilter, supEvidenceStoreFilter, supEvidenceBrandFilter]);

  const supervisorReviewVisibleCount = filteredSupervisorEvidences.length + (supReviewContentFilter !== "evidencias" ? filteredSupervisorOutOfServiceRows.length : 0);

  const supervisorEvidenceFilterOptions = useMemo(() => {
    const dateRows = visibleSupervisorEvidencesBase.filter((item) => {
      const itemYmd = getEvidenceYmd(item);
      return !itemYmd || (itemYmd >= supervisorDateBounds.start && itemYmd <= supervisorDateBounds.end);
    });
    const promotorRows = supEvidencePromotorFilter ? dateRows.filter((item) => item.promotor_id === supEvidencePromotorFilter) : dateRows;
    const storeRows = supEvidenceStoreFilter ? promotorRows.filter((item) => getStoreDisplayFromItem(item) === supEvidenceStoreFilter) : promotorRows;
    const brandRows = supEvidenceBrandFilter ? storeRows.filter((item) => normalizeBrandLabel(item.marca_nombre || "", "Marca") === supEvidenceBrandFilter) : storeRows;
    const typeRows = supEvidenceTypeFilter ? brandRows.filter((item) => (item.tipo_evidencia || "") === supEvidenceTypeFilter) : brandRows;
    const phaseRows = supEvidencePhaseFilter ? typeRows.filter((item) => String(item.fase || "").toUpperCase() === supEvidencePhaseFilter) : typeRows;
    return {
      stores: Array.from(new Set(promotorRows.map((item) => getStoreDisplayFromItem(item)).filter(Boolean))).sort(),
      brands: Array.from(new Set(storeRows.map((item) => normalizeBrandLabel(item.marca_nombre || "", "Marca")).filter(Boolean))).sort(),
      types: Array.from(new Set(brandRows.map((item) => item.tipo_evidencia || "").filter(Boolean))).sort(),
      phases: Array.from(new Set(typeRows.map((item) => String(item.fase || "").toUpperCase()).filter(Boolean))).sort(),
      risks: Array.from(new Set(phaseRows.map((item) => item.riesgo || "").filter(Boolean))).sort(),
      statuses: Array.from(new Set(phaseRows.map((item) => getSupervisorReviewState(item)).filter(Boolean))).sort(),
    };
  }, [
    visibleSupervisorEvidencesBase,
    supervisorDateBounds,
    supEvidencePromotorFilter,
    supEvidenceStoreFilter,
    supEvidenceBrandFilter,
    supEvidenceTypeFilter,
    supEvidencePhaseFilter,
  ]);

  const supervisorEvidenceSummary = useMemo(() => {
    return filteredSupervisorEvidences.reduce((acc, item) => {
      const state = getSupervisorReviewState(item);
      acc.total += 1;
      if (state === "APROBADA") acc.aprobadas += 1;
      else if (state === "OBSERVADA") acc.observadas += 1;
      else if (state === "RECHAZADA") acc.rechazadas += 1;
      else acc.pendientes += 1;
      return acc;
    }, { total: 0, pendientes: 0, aprobadas: 0, observadas: 0, rechazadas: 0 });
  }, [filteredSupervisorEvidences]);

  const groupedSupervisorEvidences = useMemo(() => buildEvidenceGroups(filteredSupervisorEvidences, supEvidenceGroupMode), [filteredSupervisorEvidences, supEvidenceGroupMode]);

  const supervisorEvidenceGroupPageCount = useMemo(() => Math.max(1, Math.ceil(groupedSupervisorEvidences.length / E011_GROUPS_PER_PAGE)), [groupedSupervisorEvidences]);
  const supervisorEvidenceGroupSafePage = Math.min(supEvidenceGroupPage, supervisorEvidenceGroupPageCount);
  const pagedSupervisorEvidenceGroups = useMemo(() => {
    const start = (supervisorEvidenceGroupSafePage - 1) * E011_GROUPS_PER_PAGE;
    return groupedSupervisorEvidences.slice(start, start + E011_GROUPS_PER_PAGE);
  }, [groupedSupervisorEvidences, supervisorEvidenceGroupSafePage]);
  const activeSupervisorEvidenceGroup = useMemo(() => groupedSupervisorEvidences.find((item) => item.brandKey === activeSupEvidenceGroupKey) || groupedSupervisorEvidences[0] || null, [groupedSupervisorEvidences, activeSupEvidenceGroupKey]);

  const clientEvidenceGroups = useMemo(() => buildEvidenceGroups(clientEvidences, clientEvidenceGroupMode), [clientEvidences, clientEvidenceGroupMode]);
  const clientEvidenceGroupPageCount = useMemo(() => Math.max(1, Math.ceil(clientEvidenceGroups.length / E011_GROUPS_PER_PAGE)), [clientEvidenceGroups]);
  const clientEvidenceGroupSafePage = Math.min(clientEvidenceGroupPage, clientEvidenceGroupPageCount);
  const pagedClientEvidenceGroups = useMemo(() => {
    const start = (clientEvidenceGroupSafePage - 1) * E011_GROUPS_PER_PAGE;
    return clientEvidenceGroups.slice(start, start + E011_GROUPS_PER_PAGE);
  }, [clientEvidenceGroups, clientEvidenceGroupSafePage]);
  const activeClientEvidenceGroup = useMemo(() => clientEvidenceGroups.find((item) => item.brandKey === activeClientEvidenceGroupKey) || clientEvidenceGroups[0] || null, [clientEvidenceGroups, activeClientEvidenceGroupKey]);

  const activeViewerSupervisorEvidence = useMemo(() => imageViewerEvidenceId ? filteredSupervisorEvidences.find((item) => item.evidencia_id === imageViewerEvidenceId) || null : null, [filteredSupervisorEvidences, imageViewerEvidenceId]);
  const activeViewerSupervisorEvidenceSequence = useMemo(() => activeSupervisorEvidenceGroup?.items?.length ? activeSupervisorEvidenceGroup.items : filteredSupervisorEvidences, [activeSupervisorEvidenceGroup, filteredSupervisorEvidences]);
  const activeViewerSupervisorEvidenceIndex = useMemo(() => activeViewerSupervisorEvidence ? activeViewerSupervisorEvidenceSequence.findIndex((item) => item.evidencia_id === activeViewerSupervisorEvidence.evidencia_id) : -1, [activeViewerSupervisorEvidenceSequence, activeViewerSupervisorEvidence]);

  const selectedTeamMember = useMemo(() => supervisorTeam.find((item) => item.promotor_id === selectedTeamPromotorId) || supervisorTeam[0] || null, [supervisorTeam, selectedTeamPromotorId]);
  const selectedAlert = useMemo(() => supervisorAlerts.find((item) => item.alerta_id === selectedAlertId) || supervisorAlerts[0] || null, [supervisorAlerts, selectedAlertId]);
  const selectedSupervisorEvidence = useMemo(() => filteredSupervisorEvidences.find((item) => item.evidencia_id === selectedSupEvidenceId) || filteredSupervisorEvidences[0] || null, [filteredSupervisorEvidences, selectedSupEvidenceId]);

  function refreshPendingQueue() {
    setPendingQueue(sortPendingQueue(readPendingQueueStorage()));
  }

  function upsertPendingOperation(operation: PendingQueueOp) {
    const current = readPendingQueueStorage().filter((item) => item.id !== operation.id);
    const next = sortPendingQueue([...current, operation]);
    writePendingQueueStorage(next);
    setPendingQueue(next);
  }

  function removePendingOperation(operationId: string) {
    const next = readPendingQueueStorage().filter((item) => item.id !== operationId);
    writePendingQueueStorage(next);
    setPendingQueue(sortPendingQueue(next));
  }

  function patchPendingOperation(operationId: string, patch: Partial<PendingQueueOp>) {
    const next = readPendingQueueStorage().map((item) => (item.id === operationId ? { ...item, ...patch } : item));
    writePendingQueueStorage(next);
    setPendingQueue(sortPendingQueue(next));
  }

  function replacePendingVisitId(previousVisitId: string, nextVisitId: string) {
    const next = readPendingQueueStorage().map((item) => {
      const updated: PendingQueueOp = { ...item };
      if (updated.localVisitId === previousVisitId) updated.localVisitId = nextVisitId;
      if (updated.visitaId === previousVisitId) updated.visitaId = nextVisitId;
      if (updated.payload?.visita_id === previousVisitId) {
        updated.payload = { ...updated.payload, visita_id: nextVisitId };
      }
      return updated;
    });
    writePendingQueueStorage(next);
    setPendingQueue(sortPendingQueue(next));
  }

  async function syncPendingQueue(showStatus = true) {
    if (!getInitData()) return;
    const queue = sortPendingQueue(readPendingQueueStorage());
    if (!queue.length || syncingPendingQueue) return;
    try {
      setSyncingPendingQueue(true);
      let synced = 0;
      for (const item of queue) {
        try {
          if (item.kind === "entry") {
            const payload = item.payload || {};
            const response = await postJson<StartEntryResponse>("/miniapp/promotor/start-entry", {
              tienda_id: payload.tienda_id,
              lat: payload.lat,
              lon: payload.lon,
              accuracy: payload.accuracy,
              foto_nombre: payload.foto_nombre,
              foto_data_url: payload.foto_data_url,
            });
            const previousVisitId = item.localVisitId || payload.localVisitId;
            removePendingOperation(item.id);
            if (previousVisitId) replacePendingVisitId(previousVisitId, response.visita_id);
            synced += 1;
            continue;
          }

          if (item.kind === "evidence") {
            const visitId = item.visitaId || item.payload?.visita_id || "";
            if (!visitId || isLocalVisitId(visitId)) continue;
            await postJson<EvidenceRegisterResponse>("/miniapp/promotor/evidence-register", {
              visita_id: visitId,
              marca_id: item.payload?.marca_id,
              marca_nombre: item.payload?.marca_nombre,
              tipo_evidencia: item.payload?.tipo_evidencia,
              fase: item.payload?.fase,
              descripcion: item.payload?.descripcion,
              fotos: item.payload?.fotos,
            });
            removePendingOperation(item.id);
            synced += 1;
            continue;
          }

          if (item.kind === "close") {
            const visitId = item.visitaId || item.payload?.visita_id || "";
            if (!visitId || isLocalVisitId(visitId)) continue;
            await postJson<CloseVisitResponse>("/miniapp/promotor/close-visit", { visita_id: visitId });
            removePendingOperation(item.id);
            synced += 1;
          }
        } catch (err) {
          if (shouldQueueSubmission(err)) {
            patchPendingOperation(item.id, { attempts: item.attempts + 1, status: "PENDIENTE_ENVIO", lastError: "" });
            break;
          }
          patchPendingOperation(item.id, { attempts: item.attempts + 1, status: "ERROR_ENVIO", lastError: err instanceof Error ? err.message : "No se pudo enviar" });
        }
      }
      if (synced) {
        await loadPromotorDashboard();
        await loadEvidencesToday();
        await loadPromotorOutOfService();
        await loadPromotorRecentAlerts();
        if (showStatus) {
          setStatusMsgDuration(7000);
          setStatusMsg(`${synced} registro(s) pendiente(s) enviados.`);
        }
      } else if (showStatus && readPendingQueueStorage().length) {
        setStatusMsgDuration(7000);
        setStatusMsg("Pendientes conservados. Se reenviarán cuando vuelva la conexión.");
      }
      refreshPendingQueue();
    } finally {
      setSyncingPendingQueue(false);
    }
  }

  const expedientAttendance = useMemo(() => (expedient?.evidencias || []).filter(isAttendanceEvidence), [expedient]);
  const expedientOperational = useMemo(() => (expedient?.evidencias || []).filter((item) => isOperationalEvidence(item) && String(item.status || "ACTIVA").toUpperCase() !== "ANULADA"), [expedient]);

  async function loadBootstrap() {
    const initData = getInitData();
    if (!initData) {
      setDetectedExternalId("");
      setError("Vista local de referencia. Abre la Mini App desde Telegram para usar la operación en línea.");
      setLoading(false);
      return;
    }
    const data = await postJson<BootstrapResponse>("/miniapp/bootstrap", {});
    setDetectedExternalId("");
    if (data.role) setRole(data.role);
    if (data.profile?.nombre) setActorLabel(data.profile.nombre);
  }

  async function loadPromotorDashboard() {
    const dashboard = await postJson<DashboardResponse>("/miniapp/promotor/dashboard", {});
    if (dashboard.promotor?.nombre) setActorLabel(dashboard.promotor.nombre);
    setStores(dashboard.stores || []);
    setPromotorUsage(dashboard.usage || {});
    const nextVisits = dashboard.visitsToday || [];
    const nextOpenVisits = nextVisits.filter((visit) => !visit.hora_fin);
    setVisits(nextVisits);
    if (!nextOpenVisits.length) {
      setSelectedVisitId("");
      setExitLocation(null);
      setExitPhoto(null);
      return;
    }
    const currentStillExists = nextOpenVisits.find((v) => v.visita_id === selectedVisitId);
    const returnVisit = miniAppReturnTarget.visitaId ? nextOpenVisits.find((v) => v.visita_id === miniAppReturnTarget.visitaId) : null;
    const nextSelectedVisitId = returnVisit?.visita_id || (currentStillExists ? currentStillExists.visita_id : nextOpenVisits[0].visita_id);
    setSelectedVisitId(nextSelectedVisitId);
    if (miniAppReturnTarget.wantsReview) {
      setPromotorModule("mis_evidencias");
      setPromotorEvidenceViewFilter("todo");
      setStatusMsgDuration(9000);
      setStatusMsg("Regresaste de la cámara. Actualizando Mis fotos de la visita...");
      void loadEvidencesToday();
      void loadPromotorOutOfService();
      if (nextSelectedVisitId) void loadEvidenceContext(nextSelectedVisitId);
    }
  }

  async function loadEvidencesToday(timeoutMs = 45000) {
    const data = await postJson<EvidencesTodayResponse>("/miniapp/promotor/evidences-today", {}, timeoutMs);
    const rows = withLocalEvidencePreviews((data.evidencias || []).map((item) => ({ ...item, status: item.status || ("ACTIVA" as const) })));
    const operationalRows = rows.filter((item) => isOperationalEvidence(item) && String(item.status || "ACTIVA").toUpperCase() !== "ANULADA");
    setAllEvidenceRows(rows);
    if (operationalRows.length && !operationalRows.find((r) => r.evidencia_id === selectedEvidenceId)) setSelectedEvidenceId(operationalRows[0].evidencia_id);
    if (!operationalRows.length) setSelectedEvidenceId("");
  }

  async function loadPromotorOutOfService() {
    const data = await postJson<PromotorOutOfServiceResponse>("/miniapp/promotor/out-of-service-today", {});
    setPromotorOutOfServiceRows(data.rows || []);
  }

  async function loadEvidenceContext(visitaId: string) {
    if (!visitaId) {
      setAvailableBrands([]);
      setBrandsOutOfService({});
      setBrandRules([]);
      setSelectedVisitStoreName("");
      return;
    }
    const offlineVisit = pendingVisits.find((item) => item.visita_id === visitaId);
    if (isLocalVisitId(visitaId) && offlineVisit) {
      const cachedBrands = storeBrandsCache[offlineVisit.tienda_id] || [];
      setAvailableBrands(cachedBrands);
      setBrandsOutOfService({});
      setSelectedVisitStoreName(getVisitDisplayName(offlineVisit, stores));
      return;
    }
    try {
      const ctx = await postJson<EvidenceContextResponse>("/miniapp/promotor/evidence-context", { visita_id: visitaId });
      setAvailableBrands(ctx.marcas || []);
      setBrandsOutOfService(Object.fromEntries((ctx.marcas_fuera_servicio || []).filter((item) => item.marca_id).map((item) => [item.marca_id, item])));
      setSelectedVisitStoreName(ctx.visita?.tienda_display || ctx.visita?.tienda_nombre || "");
      if (ctx.visita?.tienda_id && ctx.marcas?.length) {
        setStoreBrandsCache((prev) => ({ ...prev, [ctx.visita!.tienda_id]: ctx.marcas || [] }));
      }
    } catch {
      setAvailableBrands([]);
      setBrandsOutOfService({});
      setSelectedVisitStoreName("");
    }
  }

  async function loadRulesForBrand(brandId: string, brandLabel: string) {
    try {
      if (!brandId && !brandLabel) {
        setBrandRules([]);
        setEvidenceType("");
        setEvidencePhase("ESTADO_ACTUAL");
        setEvidenceQty(1);
        return;
      }
      const rules = await postJson<EvidenceRulesResponse>("/miniapp/promotor/evidence-rules", { marca_id: brandId, marca_nombre: brandLabel });
      const usableRules = (rules.reglas || []).filter((rule) => isValidRuleType(rule.tipo_evidencia));
      setBrandRules(usableRules);
      if (usableRules.length) {
        const selectedRule = usableRules.find((item) => item.tipo_evidencia === evidenceType) || usableRules[0];
        if (!evidenceType || !usableRules.find((item) => item.tipo_evidencia === evidenceType)) setEvidenceType(selectedRule.tipo_evidencia);
        setEvidenceQty(selectedRule.fotos_requeridas || 1);
      } else {
        setEvidenceType("");
        setEvidencePhase("ESTADO_ACTUAL");
        setEvidenceQty(1);
      }
    } catch {
      setBrandRules([]);
      setEvidenceType("");
      setEvidencePhase("ESTADO_ACTUAL");
      setEvidenceQty(1);
    }
  }

  async function loadSupervisorDashboard() {
    const data = await postJson<SupervisorDashboardResponse>("/miniapp/supervisor/dashboard", {});
    if (data.supervisor?.nombre) setActorLabel(data.supervisor.nombre);
    setSupervisorSummary({
      promotores: data.summary?.promotores || 0,
      visitasHoy: data.summary?.visitasHoy || 0,
      abiertas: data.summary?.abiertas || 0,
      evidenciasHoy: data.summary?.evidenciasHoy || 0,
      alertas: data.summary?.alertas || 0,
    });
    setSupervisorUsage(data.usage || {});
    setSupervisorPendingClose(data.pending_close || {});
  }

  async function loadSupervisorTeam() {
    const data = await postJson<SupervisorTeamResponse>("/miniapp/supervisor/team", {});
    const rows = data.team || [];
    setSupervisorTeam(rows);
    if (rows.length && !rows.find((row) => row.promotor_id === selectedTeamPromotorId)) setSelectedTeamPromotorId(rows[0].promotor_id);
  }

  async function loadSupervisorDayRoute(promotorId: string) {
    if (!promotorId) {
      setSupervisorDayRoute([]);
      setSelectedRouteVisitId("");
      return;
    }
    try {
      setDayRouteLoading(true);
      const data = await postJson<SupervisorDayRouteResponse>("/miniapp/supervisor/day-route", { promotor_id: promotorId });
      const rows = data.rows || [];
      setSupervisorDayRoute(rows);
      setSelectedRouteVisitId((current) => (rows.some((item) => item.visita_id === current) ? current : ""));
    } catch (err) {
      setSupervisorDayRoute([]);
      setSelectedRouteVisitId("");
      setStatusMsg(err instanceof Error ? err.message : "No se pudo cargar la ruta del día.");
    } finally {
      setDayRouteLoading(false);
    }
  }

  async function loadSupervisorAlerts() {
    const data = await postJson<SupervisorAlertsResponse>("/miniapp/supervisor/alerts", { status: alertStatusFilter, severidad: alertSeverityFilter, promotor_id: alertPromotorFilter });
    const rows = data.alerts || [];
    setSupervisorAlerts(rows);
    if (rows.length && !rows.find((row) => row.alerta_id === selectedAlertId)) setSelectedAlertId(rows[0].alerta_id);
    if (!rows.length) {
      setSelectedAlertId("");
      setExpedient(null);
    }
  }

  function snapshotSupervisorEvidence(item: EvidenceItem): SupervisorReviewSnapshot {
    return {
      evidencia_id: item.evidencia_id,
      decision_supervisor: item.decision_supervisor || "",
      motivo_revision: item.motivo_revision || "",
      revisado_por: item.revisado_por || "",
      fecha_revision: item.fecha_revision || "",
      requiere_revision_supervisor: item.requiere_revision_supervisor || "",
      status: item.status || "PENDIENTE_REVISION",
    };
  }

  function applySupervisorReviewTarget(
    item: EvidenceItem,
    queueItem: SupervisorReviewQueueItem,
  ): EvidenceItem {
    if (queueItem.restore) {
      return {
        ...item,
        decision_supervisor: queueItem.previous.decision_supervisor || "",
        motivo_revision: queueItem.previous.motivo_revision || "",
        revisado_por: queueItem.previous.revisado_por || "",
        fecha_revision: queueItem.previous.fecha_revision || "",
        requiere_revision_supervisor: queueItem.previous.requiere_revision_supervisor || "TRUE",
        status: queueItem.previous.status || "PENDIENTE_REVISION",
        local_review_pending: true,
      };
    }
    return {
      ...item,
      decision_supervisor: queueItem.decision_supervisor,
      motivo_revision: queueItem.motivo_revision,
      revisado_por: "Pendiente de sincronizar",
      fecha_revision: new Date().toISOString(),
      requiere_revision_supervisor: queueItem.decision_supervisor === "APROBADA" ? "FALSE" : "TRUE",
      status: queueItem.decision_supervisor,
      local_review_pending: true,
    };
  }

  function applyQueuedSupervisorReviewsToRows(rows: EvidenceItem[]) {
    const queue = sortSupervisorReviewQueue(readSupervisorReviewQueueStorage());
    if (!queue.length) return rows;
    const latestByEvidence = new Map<string, SupervisorReviewQueueItem>();
    for (const item of queue) latestByEvidence.set(item.evidencia_id, item);
    return rows.map((row) => {
      const queued = latestByEvidence.get(row.evidencia_id);
      return queued ? applySupervisorReviewTarget(row, queued) : row;
    });
  }

  function persistSupervisorReviewQueue(rows: SupervisorReviewQueueItem[]) {
    const sorted = sortSupervisorReviewQueue(rows);
    writeSupervisorReviewQueueStorage(sorted);
    setSupervisorReviewQueue(sorted);
  }

  function scheduleSupervisorReviewSync(delayMs = SUPERVISOR_REVIEW_FLUSH_DELAY_MS) {
    if (typeof window === "undefined") return;
    if (supervisorReviewFlushTimerRef.current != null) {
      window.clearTimeout(supervisorReviewFlushTimerRef.current);
    }
    supervisorReviewFlushTimerRef.current = window.setTimeout(() => {
      supervisorReviewFlushTimerRef.current = null;
      void syncSupervisorReviewQueue(false);
    }, delayMs);
  }

  function enqueueSupervisorReviewItems(items: SupervisorReviewQueueItem[]) {
    const current = readSupervisorReviewQueueStorage();
    const next = [...current];
    for (const item of items) {
      const existingIndex = next.findIndex((row) => row.evidencia_id === item.evidencia_id);
      if (existingIndex >= 0) next.splice(existingIndex, 1);
      next.push(item);
    }
    persistSupervisorReviewQueue(next);
    if (next.length >= 10) scheduleSupervisorReviewSync(250);
    else scheduleSupervisorReviewSync();
  }

  async function syncSupervisorReviewQueue(showStatus = false) {
    if (role !== "supervisor" || !getInitData()) return;
    if (supervisorReviewSyncInFlightRef.current) return;
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      if (showStatus) setStatusMsg("Las revisiones están protegidas y se enviarán al recuperar conexión.");
      return;
    }

    const queue = sortSupervisorReviewQueue(readSupervisorReviewQueueStorage());
    if (!queue.length) {
      setSupervisorReviewQueue([]);
      setSupervisorReviewLastError("");
      return;
    }

    const batch = queue.slice(0, SUPERVISOR_REVIEW_BATCH_SIZE);
    const sentIds = new Set(batch.map((item) => item.id));

    try {
      supervisorReviewSyncInFlightRef.current = true;
      setSupervisorReviewSyncing(true);
      setSupervisorReviewLastError("");

      const response = await postJson<SupervisorReviewBatchResponse>(
        "/miniapp/supervisor/evidence-review-batch",
        {
          reviews: batch.map((item) => ({
            client_review_id: item.id,
            evidencia_id: item.evidencia_id,
            decision_supervisor: item.decision_supervisor,
            motivo_revision: item.motivo_revision,
            restore: !!item.restore,
            previous_decision_supervisor: item.previous.decision_supervisor || "",
            previous_motivo_revision: item.previous.motivo_revision || "",
            previous_revisado_por: item.previous.revisado_por || "",
            previous_fecha_revision: item.previous.fecha_revision || "",
            previous_requiere_revision_supervisor: item.previous.requiere_revision_supervisor || "TRUE",
            previous_status: item.previous.status || "PENDIENTE_REVISION",
          })),
        },
        90000,
      );

      const resultById = new Map(
        (response.results || [])
          .filter((item) => item.client_review_id)
          .map((item) => [String(item.client_review_id), item]),
      );

      const storedNow = readSupervisorReviewQueueStorage();
      const next: SupervisorReviewQueueItem[] = [];

      for (const item of storedNow) {
        if (!sentIds.has(item.id)) {
          next.push(item);
          continue;
        }
        const result = resultById.get(item.id);
        if (result?.ok) continue;
        next.push({
          ...item,
          attempts: item.attempts + 1,
          lastError: result?.error || "No se confirmó la revisión",
        });
      }

      persistSupervisorReviewQueue(next);

      setSupervisorEvidences((previous) => previous.map((item) => {
        const stillQueued = next.some((queued) => queued.evidencia_id === item.evidencia_id);
        return stillQueued ? item : { ...item, local_review_pending: false };
      }));

      if (!next.length) {
        setSupervisorReviewLastError("");
        if (showStatus) setStatusMsg("Todas las revisiones quedaron sincronizadas.");
        void loadSupervisorDashboard().catch(() => undefined);
      } else {
        scheduleSupervisorReviewSync(1200);
      }
    } catch (err) {
      const message = friendlyRequestError(
        err,
        "Las revisiones siguen protegidas y se enviarán automáticamente.",
      );
      setSupervisorReviewLastError(message);
      const storedNow = readSupervisorReviewQueueStorage().map((item) => (
        sentIds.has(item.id)
          ? { ...item, attempts: item.attempts + 1, lastError: message }
          : item
      ));
      persistSupervisorReviewQueue(storedNow);
      scheduleSupervisorReviewSync(8000);
      if (showStatus) setStatusMsg(message);
    } finally {
      supervisorReviewSyncInFlightRef.current = false;
      setSupervisorReviewSyncing(false);
    }
  }

  function preloadFollowingSupervisorPhotos(currentEvidenceId: string, count = 2) {
    const currentIndex = filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === currentEvidenceId);
    if (currentIndex < 0) return;
    for (let offset = 1; offset <= count; offset += 1) {
      const next = filteredSupervisorEvidences[currentIndex + offset];
      if (next) void ensureSupervisorEvidencePhoto(next, { silent: true });
    }
  }

  async function loadSupervisorEvidences() {
    const data = await postJson<SupervisorEvidencesResponse>("/miniapp/supervisor/evidences", {
      promotor_id: supEvidencePromotorFilter,
    }, 60000);
    const rows = applyQueuedSupervisorReviewsToRows(data.evidences || []);
    setSupervisorEvidences(rows);
    setSupervisorQueueVisibleCount(24);
    setSupervisorPhotoCache((previous) => {
      const next: Record<string, { thumb?: string; full?: string }> = {};
      for (const row of rows) {
        const cached = previous[row.evidencia_id] || {};
        const thumb = row.thumb_url || cached.thumb || "";
        const full = row.url_foto || cached.full || "";
        if (thumb || full) next[row.evidencia_id] = { thumb, full };
      }
      return next;
    });
    if (rows.length && !rows.find((row) => row.evidencia_id === selectedSupEvidenceId)) setSelectedSupEvidenceId(rows[0].evidencia_id);
    if (!rows.length) setSelectedSupEvidenceId("");
  }

  function getSupervisorThumbSrc(item: EvidenceItem | null | undefined) {
    if (!item) return "";
    const cached = supervisorPhotoCache[item.evidencia_id];
    return cached?.thumb || item.thumb_url || cached?.full || item.url_foto || "";
  }

  function getSupervisorFullSrc(item: EvidenceItem | null | undefined) {
    if (!item) return "";
    const cached = supervisorPhotoCache[item.evidencia_id];
    return cached?.full || item.url_foto || cached?.thumb || item.thumb_url || "";
  }

  function isSupervisorPhotoLoading(evidenceId: string) {
    return supervisorPhotoLoadingIds.includes(evidenceId);
  }

  async function ensureSupervisorEvidencePhoto(item: EvidenceItem, options?: { silent?: boolean }) {
    const existing = getSupervisorFullSrc(item);
    if (existing && !item.photo_deferred) return existing;
    const cached = supervisorPhotoCache[item.evidencia_id]?.full;
    if (cached) return cached;
    if (isSupervisorPhotoLoading(item.evidencia_id)) return "";

    setSupervisorPhotoLoadingIds((previous) => previous.includes(item.evidencia_id) ? previous : [...previous, item.evidencia_id]);
    try {
      const data = await postJson<SupervisorEvidencePhotoResponse>(
        "/miniapp/supervisor/evidence-photo",
        { evidencia_id: item.evidencia_id },
        60000,
      );
      const full = data.url_foto || "";
      const thumb = data.thumb_url || full;
      setSupervisorPhotoCache((previous) => ({
        ...previous,
        [item.evidencia_id]: {
          thumb: thumb || previous[item.evidencia_id]?.thumb || "",
          full: full || previous[item.evidencia_id]?.full || "",
        },
      }));
      return full;
    } catch (err) {
      if (!options?.silent) setStatusMsg(friendlyRequestError(err, "No se pudo cargar esta fotografía."));
      return "";
    } finally {
      setSupervisorPhotoLoadingIds((previous) => previous.filter((id) => id !== item.evidencia_id));
    }
  }

  async function openSupervisorEvidenceViewer(item: EvidenceItem) {
    const src = getSupervisorFullSrc(item) || await ensureSupervisorEvidencePhoto(item);
    if (!src) return;
    openImageViewer(src, item.evidencia_id);
  }

  async function loadSupervisorOutOfService() {
    const data = await postJson<SupervisorOutOfServiceResponse>("/miniapp/supervisor/out-of-service", {
      promotor_id: supEvidencePromotorFilter,
    });
    setSupervisorOutOfServiceRows(data.rows || []);
  }

  async function openVisitExpedient(visitaId: string) {
    if (!visitaId) return;
    try {
      setExpedientLoading(true);
      const data = await postJson<VisitExpedientResponse>("/miniapp/supervisor/visit-expedient", { visita_id: visitaId });
      setExpedient(data);
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo abrir el expediente.");
    } finally {
      setExpedientLoading(false);
    }
  }

  async function loadPromotorRecentAlerts() {
    try {
      const data = await postJson<PromotorRecentAlertsResponse>("/miniapp/promotor/alerts-recent", {});
      setPromotorRecentAlerts(data.rows || []);
    } catch {
      setPromotorRecentAlerts([]);
    }
  }

  async function loadSupervisorEvidenceAudit(evidenciaId: string) {
    if (!evidenciaId) {
      setSupervisorEvidenceAudit([]);
      return;
    }
    try {
      const data = await postJson<EvidenceAuditResponse>("/miniapp/supervisor/evidence-audit", { evidencia_id: evidenciaId });
      setSupervisorEvidenceAudit(data.rows || []);
    } catch {
      setSupervisorEvidenceAudit([]);
    }
  }

  async function loadClientBootstrap() {
    const data = await postJson<ClientEnvelope<ClientBootstrapResponse["data"]>>("/miniapp/cliente/bootstrap", {});
    const payload = data.data;
    if (!payload) return;
    if (payload.cliente) setClientBranding(payload.cliente);
    if (payload.access?.nombre_contacto) setActorLabel(payload.access.nombre_contacto);
  }

  async function loadClientFilterOptions() {
    const data = await postJson<ClientEnvelope<any>>("/miniapp/cliente/filter-options", clientFilters);
    setClientFilterOptions({
      cadenas: data.data?.cadenas || [],
      regiones: data.data?.regiones || [],
      tiendas: data.data?.tiendas || [],
      marcas: data.data?.marcas || [],
      tipos_evidencia: data.data?.tipos_evidencia || [],
      riesgos: data.data?.riesgos || [],
      decisiones: data.data?.decisiones || [],
      severidades: data.data?.severidades || [],
      estatus_alerta: data.data?.estatus_alerta || [],
    });
  }

  async function loadClientDashboard() {
    const data = await postJson<ClientEnvelope<ClientDashboardData>>("/miniapp/cliente/dashboard", { filters: clientFilters });
    setClientDashboard(data.data || {});
    if (data.data?.cliente) setClientBranding(data.data.cliente);
  }

  async function loadClientStores() {
    const data = await postJson<ClientEnvelope<{ rows: ClientStoreRow[] }>>("/miniapp/cliente/stores", { filters: clientFilters, pagination: { page: 1, page_size: 100 } });
    const rows = data.data?.rows || [];
    setClientStores(rows);
    if (rows.length && !rows.some((row) => row.tienda_id === selectedClientStoreId)) setSelectedClientStoreId(rows[0].tienda_id);
    if (!rows.length) {
      setSelectedClientStoreId("");
      setClientStoreDetail(null);
    }
  }

  async function loadClientStoreDetail(storeId: string) {
    if (!storeId) {
      setClientStoreDetail(null);
      return;
    }
    const data = await postJson<ClientEnvelope<ClientStoreDetail>>("/miniapp/cliente/store-detail", { tienda_id: storeId, filters: clientFilters });
    setClientStoreDetail(data.data || null);
  }

  async function loadClientEvidences() {
    const data = await postJson<ClientEnvelope<{ rows: EvidenceItem[] }>>("/miniapp/cliente/evidences", { filters: clientFilters, pagination: { page: 1, page_size: 80 } });
    setClientEvidences(data.data?.rows || []);
  }

  async function loadClientIncidents() {
    const data = await postJson<ClientEnvelope<{ rows: SupervisorAlert[] }>>("/miniapp/cliente/incidents", { filters: clientFilters, pagination: { page: 1, page_size: 80 } });
    setClientIncidents(data.data?.rows || []);
  }

  async function loadClientDeliverables() {
    const data = await postJson<ClientEnvelope<{ enabled: boolean; message: string }>>("/miniapp/cliente/deliverables", { filters: clientFilters });
    setClientDeliverablesMessage(data.data?.message || "Entregables no disponibles por ahora.");
  }

  async function initialize() {
    if (isExternalCameraMode) {
      setLoading(false);
      return;
    }
    try {
      setLoading(true);
      setError("");
      setDetectedExternalId("");
      await loadBootstrap();
    } catch (err) {
      setRole(null);
      const nextError = err instanceof Error ? err.message : "No se pudo cargar la operación.";
      setError(nextError);
      const match = String(nextError).match(/external_id detectado:\s*([^\s.]+)/i);
      setDetectedExternalId(match?.[1] || "");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { void initialize(); }, []);

  useEffect(() => {
    if (role === "promotor") {
      void loadPromotorDashboard();
      void loadEvidencesToday();
      void loadPromotorOutOfService();
      void loadPromotorRecentAlerts();
      void loadRulesForBrand("", "");
      void syncPendingQueue(false);
    }
    if (role === "supervisor") {
      setSupervisorReviewQueue(sortSupervisorReviewQueue(readSupervisorReviewQueueStorage()));
      void loadSupervisorDashboard();
      void loadSupervisorTeam();
      void loadSupervisorAlerts();
      void loadSupervisorEvidences();
      void loadSupervisorOutOfService();
      window.setTimeout(() => { void syncSupervisorReviewQueue(false); }, 500);
    }
    if (role === "cliente") {
      void loadClientBootstrap();
      void loadClientFilterOptions();
      void loadClientDashboard();
      void loadClientStores();
      void loadClientEvidences();
      void loadClientIncidents();
      void loadClientDeliverables();
    }
  }, [role]);

  useEffect(() => { if (role === "promotor") { setEvidenceBrandId(""); setEvidenceBrandLabel(""); setEvidenceType(""); setEvidencePhase("ESTADO_ACTUAL"); void loadEvidenceContext(selectedVisitId); } }, [selectedVisitId, role]);
  useEffect(() => { if (role === "promotor") void loadRulesForBrand(evidenceBrandId, evidenceBrandLabel); }, [evidenceBrandId, evidenceBrandLabel, role]);
  useEffect(() => { if (role === "supervisor") void loadSupervisorAlerts(); }, [alertStatusFilter, alertSeverityFilter, alertPromotorFilter]);
  useEffect(() => { if (role === "supervisor") { void loadSupervisorEvidences(); void loadSupervisorOutOfService(); } }, [supEvidencePromotorFilter, role]);
  useEffect(() => {
    if (role !== "supervisor") return;
    void loadSupervisorDayRoute(selectedTeamPromotorId);
    setExpedient(null);
  }, [role, selectedTeamPromotorId]);

  useEffect(() => {
    if (role !== "supervisor") return;
    void loadSupervisorEvidenceAudit(selectedSupEvidenceId);
  }, [selectedSupEvidenceId, role]);

  useEffect(() => {
    if (role !== "supervisor" || !selectedSupervisorEvidence) return;
    void ensureSupervisorEvidencePhoto(selectedSupervisorEvidence, { silent: true });
    preloadFollowingSupervisorPhotos(selectedSupervisorEvidence.evidencia_id, 2);
  }, [role, selectedSupEvidenceId]);


  useEffect(() => {
    if (role !== "supervisor") return;
    if (!supervisorAlerts.length) {
      setSelectedAlertId("");
      setExpedient(null);
      return;
    }
    const stillExists = supervisorAlerts.some((item) => item.alerta_id === selectedAlertId);
    if (!stillExists) setSelectedAlertId(supervisorAlerts[0].alerta_id);
  }, [supervisorAlerts, selectedAlertId, role]);

  useEffect(() => {
    if (role !== "supervisor") return;
    if (!supEvidencePromotorFilter) return;
    const hasVisibleEvidenceForPromotor = supervisorEvidences.some((item) => item.promotor_id === supEvidencePromotorFilter);
    if (!hasVisibleEvidenceForPromotor) {
      setSelectedSupEvidenceId("");
      setSelectedSupEvidenceIds([]);
      setExpedient(null);
    }
  }, [supEvidencePromotorFilter, supervisorEvidences, role]);

  useEffect(() => {
    if (role !== "supervisor") return;
    setSelectedSupEvidenceIds((prev) => prev.filter((id) => supervisorEvidences.some((item) => item.evidencia_id === id)));
  }, [supervisorEvidences, role]);

  useEffect(() => {
    if (role !== "supervisor") return;
    setSelectedSupEvidenceIds((prev) => prev.filter((id) => filteredSupervisorEvidences.some((item) => item.evidencia_id === id)));
  }, [filteredSupervisorEvidences, role]);

  useEffect(() => {
    if (role !== "supervisor") return;
    setSupEvidenceGroupPage(1);
    setSupervisorQueueVisibleCount(24);
    setActiveSupEvidenceGroupKey("");
  }, [role, supEvidenceGroupMode, supEvidencePromotorFilter, supEvidenceStoreFilter, supEvidenceBrandFilter, supEvidenceTypeFilter, supEvidencePhaseFilter, supEvidenceRiskFilter, supEvidenceStatusFilter, supEvidenceOnlyPending, supEvidenceDatePreset, supEvidenceDateStart, supEvidenceDateEnd]);

  useEffect(() => {
    if (role !== "cliente") return;
    setClientEvidenceGroupPage(1);
    setActiveClientEvidenceGroupKey("");
  }, [role, clientEvidenceGroupMode, clientFilters.fecha_inicio, clientFilters.fecha_fin, clientFilters.cadena, clientFilters.region, clientFilters.tienda_id, clientFilters.marca_id, clientFilters.tipo_evidencia, clientFilters.decision_supervisor, clientFilters.riesgo]);

  useEffect(() => {
    if (role !== "supervisor") return;
    if (supEvidenceStoreFilter && !supervisorEvidenceFilterOptions.stores.includes(supEvidenceStoreFilter)) setSupEvidenceStoreFilter("");
    if (supEvidenceBrandFilter && !supervisorEvidenceFilterOptions.brands.includes(supEvidenceBrandFilter)) setSupEvidenceBrandFilter("");
    if (supEvidenceTypeFilter && !supervisorEvidenceFilterOptions.types.includes(supEvidenceTypeFilter)) setSupEvidenceTypeFilter("");
    if (supEvidencePhaseFilter && !supervisorEvidenceFilterOptions.phases.includes(supEvidencePhaseFilter)) setSupEvidencePhaseFilter("");
    if (supEvidenceRiskFilter && !supervisorEvidenceFilterOptions.risks.includes(supEvidenceRiskFilter)) setSupEvidenceRiskFilter("");
  }, [role, supervisorEvidenceFilterOptions, supEvidenceStoreFilter, supEvidenceBrandFilter, supEvidenceTypeFilter, supEvidencePhaseFilter, supEvidenceRiskFilter]);

  useEffect(() => {
    if (role !== "cliente") return;
    void loadClientFilterOptions();
    void loadClientDashboard();
    void loadClientStores();
    void loadClientEvidences();
    void loadClientIncidents();
    void loadClientDeliverables();
  }, [role, clientFilters]);


  useEffect(() => {
    if (role !== "cliente") return;
    if (!selectedClientStoreId) {
      setClientStoreDetail(null);
      return;
    }
    void loadClientStoreDetail(selectedClientStoreId);
  }, [role, selectedClientStoreId]);

  async function loadGalleryAuthorization(mode: string, payload: Record<string, unknown>, setter: React.Dispatch<React.SetStateAction<GalleryAuthorizationInfo>>) {
    try {
      const response = await postJson<GalleryAuthorizationResponse>("/miniapp/promotor/gallery-authorization", { mode, ...payload });
      setter({ allowed: !!response.allowed, authorization: response.authorization || null, debug: response.debug || { reason: response.allowed ? "MATCH" : "NO_MATCH" } });
    } catch (err) {
      setter({ allowed: false, authorization: null, debug: { reason: "REQUEST_ERROR", sheet_error: err instanceof Error ? err.message : "No se pudo validar la autorización" } });
    }
  }

  async function handleGallerySelection(target: "attendance-entry" | "evidence" | "replace", fileList: FileList | null) {
    try {
      const files = Array.from(fileList || []).filter(Boolean);
      if (!files.length) return;
      if (target === "attendance-entry") { const photo = await readPhotoForSheets(files[0], "galeria", false);
        setEntryPhoto(photo);
        setStatusMsg("Foto desde galería lista.");
        return;
      }
      if (target === "replace") {
        const photo = await readPhotoForSheets(files[0]);
        setStatusMsg("Reemplazando foto...");
        setStatusMsgDuration(7000);
        await replaceEvidencePhotoPayload(photo.name, photo.dataUrl, "GALERIA_AUTORIZADA");
        return;
      }
      const nextPhotos = [] as PhotoCapture[];
      for (const file of files.slice(0, 24)) {
        nextPhotos.push(await readPhotoForSheets(file));
      }
      setEvidencePhotos((prev) => [...prev, ...nextPhotos].slice(0, 24));
      setStatusMsg(`${nextPhotos.length} foto(s) desde galería agregada(s).`);
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo leer la foto seleccionada.");
    }
  }

  async function handleNativeCameraSelection(target: CameraTarget, fileList: FileList | null) {
    try {
      const files = Array.from(fileList || []).filter(Boolean);
      if (!files.length) return;
      if (target === "entrada") {
        if (!selectedStoreId) return setStatusMsg("Selecciona una tienda antes de capturar ubicación o tomar foto.");
        const photo = await readPhotoForSheets(files[0], "captura", false); setEntryPhoto(photo); setStatusMsg("Foto de entrada lista.");
        return;
      }
      if (target === "reemplazo") {
        const photo = await readPhotoForSheets(files[0], "captura");
        setStatusMsg("Reemplazando foto...");
        setStatusMsgDuration(7000);
        await replaceEvidencePhotoPayload(photo.name, photo.dataUrl, "CAMARA");
        return;
      }
      if (!selectedVisitId) return setStatusMsg("Selecciona una tienda/visita antes de tomar foto.");
      const nextPhotos = [] as PhotoCapture[];
      for (const file of files.slice(0, 24)) {
        nextPhotos.push(await readPhotoForSheets(file, "captura"));
      }
      setEvidencePhotos((prev) => [...prev, ...nextPhotos].slice(0, 24));
      setStatusMsg(`${nextPhotos.length} foto(s) de cámara agregada(s).`);
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo leer la foto tomada.");
    }
  }

  function galleryReasonLabel(info: GalleryAuthorizationInfo) {
    const reason = String(info.debug?.reason || "").toUpperCase();
    if (info.allowed) return `Autorizada${info.authorization?.autorizacion_id ? ` · ${info.authorization.autorizacion_id}` : ""}`;
    if (reason === "NO_STORE") return "Selecciona una tienda.";
    if (reason === "NO_VISIT") return "Selecciona una visita.";
    if (reason === "NO_EVIDENCE") return "Selecciona una evidencia.";
    if (reason === "OUTSIDE_WINDOW_BEFORE" || reason === "OUTSIDE_WINDOW_AFTER") return "Fuera de vigencia.";
    if (reason === "MAX_REACHED") return "Límite de fotos alcanzado.";
    if (reason === "SHEET_ERROR" || reason === "REQUEST_ERROR") return info.debug?.sheet_error || "No se pudo leer la autorización.";
    return "Sin autorización activa.";
  }

  async function stopCameraStream() {
    try {
      cameraStreamRef.current?.getTracks().forEach((track) => track.stop());
    } finally {
      cameraStreamRef.current = null;
      setCameraZoom({ supported: false, min: 1, max: 1, step: 0.1, value: 1 });
    }
  }

  function showCaptureGuard(message: string) {
    setCaptureGuardMsg(message);
    setStatusMsg(message);
    setStatusMsgDuration(8500);
    window.setTimeout(() => {
      setCaptureGuardMsg((current) => current === message ? "" : current);
    }, 8500);
  }

  function setupCameraZoom(stream: MediaStream, facing: "user" | "environment") {
    try {
      const track = stream.getVideoTracks?.()[0];
      const capabilities = (track?.getCapabilities?.() || {}) as any;
      const settings = (track?.getSettings?.() || {}) as any;
      const min = Number(capabilities?.zoom?.min ?? 1);
      const max = Number(capabilities?.zoom?.max ?? min);
      const step = Number(capabilities?.zoom?.step ?? 0.1);
      const value = Number(settings?.zoom ?? min);
      const supported = facing === "environment" && Number.isFinite(min) && Number.isFinite(max) && max > min;
      setCameraZoom({ supported, min: supported ? min : 1, max: supported ? max : 1, step: supported && Number.isFinite(step) && step > 0 ? step : 0.1, value: supported && Number.isFinite(value) ? value : 1 });
    } catch {
      setCameraZoom({ supported: false, min: 1, max: 1, step: 0.1, value: 1 });
    }
  }

  async function changeCameraZoom(nextValue: number) {
    const value = Math.max(cameraZoom.min, Math.min(cameraZoom.max, nextValue));
    setCameraZoom((prev) => ({ ...prev, value }));
    try {
      const track = cameraStreamRef.current?.getVideoTracks?.()[0];
      await track?.applyConstraints?.({ advanced: [{ zoom: value }] } as any);
    } catch {
      setStatusMsg("El zoom no está disponible en este equipo o Telegram lo limitó.");
      setStatusMsgDuration(5200);
    }
  }

  async function openCamera(target: CameraTarget, facing: "user" | "environment") {
    // E020C: cámara integrada mejorada. Selfie mantiene cámara frontal; evidencia/reemplazo usa trasera, sin abrir galería.
    if (target === "entrada" && !selectedStoreId) {
      showCaptureGuard("Primero selecciona una tienda. Después podrás capturar ubicación y tomar selfie.");
      return;
    }
    if (target === "evidencia" && !selectedVisitId) {
      showCaptureGuard("Primero selecciona una visita/tienda activa. Después podrás tomar foto de evidencia.");
      return;
    }
    try {
      setCapturingPhoto(target);
      setCaptureGuardMsg("");
      await stopCameraStream();
      const evidenceVideo = facing === "environment" || target === "evidencia" || target === "reemplazo";
      const attempts: MediaStreamConstraints[] = evidenceVideo ? [
        { video: { facingMode: { ideal: "environment" }, width: { ideal: 1920 }, height: { ideal: 1080 }, aspectRatio: { ideal: 1.7777778 } } as any, audio: false },
        { video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 }, aspectRatio: { ideal: 1.7777778 } } as any, audio: false },
        { video: { facingMode: "environment" }, audio: false },
        { video: true, audio: false },
      ] : [
        { video: { facingMode: { ideal: "user" }, width: { ideal: 1280 }, height: { ideal: 720 } } as any, audio: false },
        { video: { facingMode: "user" }, audio: false },
        { video: true, audio: false },
      ];
      let stream: MediaStream | null = null;
      let lastError: unknown = null;
      for (const constraints of attempts) {
        try {
          stream = await navigator.mediaDevices.getUserMedia(constraints);
          break;
        } catch (error) {
          lastError = error;
        }
      }
      if (!stream) throw lastError || new Error("No se pudo abrir la cámara.");
      cameraStreamRef.current = stream;
      setupCameraZoom(stream, evidenceVideo ? "environment" : "user");
      setCameraModal({ open: true, target, facing: evidenceVideo ? "environment" : "user" });
      window.setTimeout(() => {
        if (cameraVideoRef.current) {
          cameraVideoRef.current.srcObject = stream;
          void cameraVideoRef.current.play().catch(() => undefined);
        }
      }, 10);
    } catch (err) {
      const rawMessage = err instanceof Error ? err.message : String(err || "");
      const friendly = rawMessage.includes("Could not start video source")
        ? "No se pudo iniciar la cámara. Cierra otras apps que la estén usando e inténtalo de nuevo."
        : (rawMessage || "No se pudo abrir la cámara.");
      setStatusMsg(friendly);
      setStatusMsgDuration(7200);
    } finally {
      setCapturingPhoto(null);
    }
  }


  async function captureFromCameraModal() {
    const video = cameraVideoRef.current;
    if (!video || !cameraModal.target) return;
    const canvas = document.createElement("canvas");
    const width = video.videoWidth || 1280;
    const height = video.videoHeight || 720;
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.drawImage(video, 0, 0, width, height);
    const raw = canvas.toDataURL("image/jpeg", 0.92); const dataUrl = (cameraModal.target === "entrada" || cameraModal.target === "salida") ? await compressDataUrlToSheetsSafeSize(raw) : await compressDataUrlForReview(raw); const payload: PhotoCapture = { name: `captura-${Date.now()}.jpg`, dataUrl, capturedAt: nowMxString() };
    if (cameraModal.target === "entrada") {
      setEntryPhoto(payload);
      setStatusMsg("Foto de entrada lista.");
    } else if (cameraModal.target === "salida") {
      setExitPhoto(payload);
      setStatusMsg("Foto de salida lista.");
    } else if (cameraModal.target === "reemplazo") {
      setStatusMsg("Reemplazando foto...");
      setStatusMsgDuration(7000);
      await replaceEvidencePhotoPayload(payload.name, payload.dataUrl);
    } else {
      setEvidencePhotos((prev) => [...prev, payload].slice(0, 24));
      setStatusMsg("Foto de evidencia agregada.");
    }
    setCameraModal({ open: false, target: null, facing: "environment" });
    await stopCameraStream();
  }

  async function closeCameraModal() {
    setCameraModal({ open: false, target: null, facing: "environment" });
    await stopCameraStream();
  }

  function openImageViewer(src?: string, evidenceId = "") {
    if (!src) return;
    setImageViewerScale(1);
    setImageViewerOffset({ x: 0, y: 0 });
    setImageViewerDragging(false);
    imageViewerTouchRef.current = { distance: 0, startScale: 1, dragging: false, dragStartX: 0, dragStartY: 0, originX: 0, originY: 0 };
    setImageViewerEvidenceId(evidenceId);
    setImageViewerSrc(src);
  }

  function closeImageViewer() {
    setImageViewerSrc("");
    setImageViewerEvidenceId("");
    setImageViewerScale(1);
    setImageViewerOffset({ x: 0, y: 0 });
    setImageViewerDragging(false);
    imageViewerTouchRef.current = { distance: 0, startScale: 1, dragging: false, dragStartX: 0, dragStartY: 0, originX: 0, originY: 0 };
  }

  useEffect(() => {
    if (!imageViewerSrc) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeImageViewer();
      if (event.key === "+" || event.key === "=") zoomImageViewer(imageViewerScale + 0.25);
      if (event.key === "-" || event.key === "_") zoomImageViewer(imageViewerScale - 0.25);
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [imageViewerSrc, imageViewerScale]);

  function zoomImageViewer(nextScale: number) {
    const normalized = Math.min(4, Math.max(1, Number(nextScale.toFixed(2))));
    setImageViewerScale(normalized);
    if (normalized <= 1.02) {
      setImageViewerOffset({ x: 0, y: 0 });
      setImageViewerDragging(false);
    }
  }

  function startImageViewerDrag(clientX: number, clientY: number) {
    if (imageViewerScale <= 1) return;
    imageViewerTouchRef.current = {
      ...imageViewerTouchRef.current,
      dragging: true,
      dragStartX: clientX,
      dragStartY: clientY,
      originX: imageViewerOffset.x,
      originY: imageViewerOffset.y,
    };
    setImageViewerDragging(true);
  }

  function moveImageViewerDrag(clientX: number, clientY: number) {
    if (!imageViewerTouchRef.current.dragging || imageViewerScale <= 1) return;
    const dx = clientX - imageViewerTouchRef.current.dragStartX;
    const dy = clientY - imageViewerTouchRef.current.dragStartY;
    setImageViewerOffset({
      x: imageViewerTouchRef.current.originX + dx,
      y: imageViewerTouchRef.current.originY + dy,
    });
  }

  function endImageViewerDrag() {
    imageViewerTouchRef.current = { ...imageViewerTouchRef.current, dragging: false };
    setImageViewerDragging(false);
  }

  function handleImageViewerWheel(event: React.WheelEvent<HTMLImageElement>) {
    event.preventDefault();
    const delta = event.deltaY < 0 ? 0.18 : -0.18;
    zoomImageViewer(imageViewerScale + delta);
  }

  function handleImageViewerMouseDown(event: React.MouseEvent<HTMLImageElement>) {
    event.preventDefault();
    event.stopPropagation();
    startImageViewerDrag(event.clientX, event.clientY);
  }

  function handleImageViewerMouseMove(event: React.MouseEvent<HTMLImageElement>) {
    if (!imageViewerDragging) return;
    event.preventDefault();
    moveImageViewerDrag(event.clientX, event.clientY);
  }

  function handleImageViewerMouseUp() {
    endImageViewerDrag();
  }

  function handleImageViewerTouchStart(event: React.TouchEvent<HTMLElement>) {
    if (event.touches.length === 2) {
      const a = event.touches[0];
      const b = event.touches[1];
      const distance = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
      imageViewerTouchRef.current = { ...imageViewerTouchRef.current, distance, startScale: imageViewerScale, dragging: false };
      return;
    }
    if (event.touches.length === 1 && imageViewerScale > 1) {
      const touch = event.touches[0];
      startImageViewerDrag(touch.clientX, touch.clientY);
    }
  }

  function handleImageViewerTouchMove(event: React.TouchEvent<HTMLElement>) {
    if (event.touches.length === 2) {
      event.preventDefault();
      const a = event.touches[0];
      const b = event.touches[1];
      const distance = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
      const base = imageViewerTouchRef.current.distance || distance;
      const ratio = distance / Math.max(base, 1);
      zoomImageViewer(imageViewerTouchRef.current.startScale * ratio);
      return;
    }
    if (event.touches.length === 1 && imageViewerScale > 1) {
      event.preventDefault();
      const touch = event.touches[0];
      if (!imageViewerTouchRef.current.dragging) {
        startImageViewerDrag(touch.clientX, touch.clientY);
      }
      moveImageViewerDrag(touch.clientX, touch.clientY);
    }
  }

  function handleImageViewerTouchEnd() {
    if (imageViewerScale < 1.02) {
      setImageViewerScale(1);
      setImageViewerOffset({ x: 0, y: 0 });
    }
    endImageViewerDrag();
  }

  function handleImageTap(src?: string) {
    if (!src) return;
    const now = Date.now();
    if (lastImageTapRef.current.src === src && now - lastImageTapRef.current.at < 280) {
      openImageViewer(src);
      lastImageTapRef.current = { src: "", at: 0 };
      return;
    }
    lastImageTapRef.current = { src, at: now };
  }

  function focusAttendanceForVisit(visitaId: string) {
    const targetEvidence = attendanceGallery.find((item) => item.visita_id === visitaId);
    if (!targetEvidence) return;
    window.setTimeout(() => {
      attendancePhotoRefs.current[targetEvidence.evidencia_id]?.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 120);
  }

  function removeEvidencePhotoAt(index: number) {
    setEvidencePhotos((prev) => prev.filter((_, i) => i !== index));
  }

  function clearEvidencePhotos() {
    setEvidencePhotos([]);
  }

  async function captureLocation(kind: CaptureKind) {
    if (kind === "entrada" && !selectedStoreId) {
      showCaptureGuard("Primero selecciona una tienda. Después podrás capturar ubicación y tomar selfie.");
      return;
    }
    if (kind === "salida" && !exitVisit) {
      setStatusMsg("Selecciona una visita abierta antes de registrar salida.");
      return;
    }
    setStatusMsg(kind === "entrada" ? "Se solicitará tu ubicación para registrar la entrada." : "Se solicitará tu ubicación para registrar la salida.");
    try {
      setCapturingLocation(kind);
      const location = await getCurrentLocation();
      if (kind === "entrada") {
        setEntryLocation(location);
        setStatusMsg("Ubicación de entrada capturada.");
      } else {
        setExitLocation(location);
        setStatusMsg("Ubicación de salida capturada.");
      }
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo obtener la ubicación.");
    } finally {
      setCapturingLocation(null);
    }
  }



  async function createEntry() {
    let queuedPayload: Record<string, unknown> | null = null;
    try {
      if (!selectedStoreId) return setStatusMsg("Selecciona una tienda.");
      if (!entryLocation) return setStatusMsg("Captura la ubicación de entrada.");
      if (!entryPhoto) return setStatusMsg("Captura la foto de entrada.");
      if (!getInitData()) return setStatusMsg("Esta acción real solo funciona desde Telegram.");
      const selectedStore = stores.find((store) => store.tienda_id === selectedStoreId);
      const selectedStoreLabel = selectedStore ? formatStoreDisplay(selectedStore.tienda_id, selectedStore.nombre_tienda) : "la tienda seleccionada";
      if (typeof window !== "undefined" && !window.confirm(`¿Deseas registrar entrada en ${selectedStoreLabel}?`)) return;
      setSyncing(true);
      setStatusMsgDuration(7000);
      setStatusMsg("Registrando entrada... estamos validando geocerca y guardando la visita.");
      queuedPayload = {
        tienda_id: selectedStoreId,
        lat: entryLocation.lat,
        lon: entryLocation.lon,
        accuracy: entryLocation.accuracy,
        foto_nombre: entryPhoto.name,
        foto_data_url: entryPhoto.dataUrl,
        source: String(entryPhoto.name || "").startsWith("galeria-") ? "GALERIA_AUTORIZADA" : "CAMARA",
      };
      const response = await postJson<StartEntryResponse>("/miniapp/promotor/start-entry", queuedPayload);
      setStatusMsgDuration(6800);
      setStatusMsg(response.warning === "attendance_photo_too_large_for_sheets" ? "Entrada registrada. La visita quedó guardada, pero la foto no cupo completa en Sheets." : `Entrada registrada en ${response.tienda_display || response.tienda_nombre}`);
      setEntryLocation(null);
      setEntryPhoto(null);
      setExitLocation(null);
      setExitPhoto(null);
      const optimisticVisit: VisitItem = {
        visita_id: response.visita_id,
        tienda_id: response.tienda_id,
        tienda_nombre: response.tienda_nombre,
        tienda_display: response.tienda_display,
        hora_inicio: response.started_at,
        hora_fin: "",
        estado_visita: "ABIERTA",
      };
      setVisits((prev) => [optimisticVisit, ...prev.filter((visit) => visit.visita_id !== response.visita_id)]);
      setSelectedVisitId(response.visita_id);
      window.setTimeout(() => {
        void loadPromotorDashboard().catch(() => undefined);
        void loadEvidencesToday().catch(() => undefined);
        void loadPromotorOutOfService().catch(() => undefined);
      }, 1000);
    } catch (err) {
      if (shouldQueueSubmission(err) && selectedStoreId && entryLocation && entryPhoto && queuedPayload) {
        const selectedStore = stores.find((store) => store.tienda_id === selectedStoreId);
        const localVisitId = buildPendingQueueId("LOCAL");
        upsertPendingOperation({
          id: buildPendingQueueId("QENTRY"),
          kind: "entry",
          createdAt: new Date().toISOString(),
          status: "PENDIENTE_ENVIO",
          attempts: 0,
          localVisitId,
          tienda_id: selectedStoreId,
          tienda_nombre: selectedStore?.nombre_tienda || selectedStoreId,
          payload: {
            ...queuedPayload,
            entryPhoto,
            localVisitId,
          },
        });
        setSelectedVisitId(localVisitId);
        setEntryLocation(null);
        setEntryPhoto(null);
        setStatusMsgDuration(7500);
        setStatusMsg("Entrada guardada localmente. Se enviará cuando vuelva la conexión.");
        return;
      }
      const message = err instanceof Error ? err.message : "No se pudo registrar la entrada real.";
      if (message.includes("Ya tienes una visita abierta")) {
        setEntryPhoto(null);
        setStatusMsgDuration(7000);
      }
      setStatusMsg(message);
    } finally {
      setSyncing(false);
    }
  }

  async function closeVisit() {
    try {
      if (!exitVisit) return setStatusMsg("No hay visita abierta para registrar salida.");
      if (!getInitData()) return setStatusMsg("Esta acción real solo funciona desde Telegram.");
      if (typeof window !== "undefined" && !window.confirm(`¿Deseas registrar salida en ${getVisitDisplayName(exitVisit, stores)}?`)) return;
      setSyncing(true);
      const payload = { visita_id: exitVisit.visita_id };
      const response = await postJson<CloseVisitResponse>("/miniapp/promotor/close-visit", payload);
      setStatusMsg(response.warning === "attendance_photo_too_large_for_sheets" ? "Salida registrada. La visita quedó guardada, pero la foto no cupo completa en Sheets." : "Salida registrada correctamente.");
      setExitLocation(null);
      setExitPhoto(null);
      await loadPromotorDashboard();
      await loadEvidencesToday();
    } catch (err) {
      if (shouldQueueSubmission(err) && exitVisit) {
        upsertPendingOperation({
          id: buildPendingQueueId("QCLOSE"),
          kind: "close",
          createdAt: new Date().toISOString(),
          status: "PENDIENTE_ENVIO",
          attempts: 0,
          visitaId: exitVisit.visita_id,
          tienda_id: exitVisit.tienda_id,
          tienda_nombre: exitVisit.tienda_nombre,
          payload: { visita_id: exitVisit.visita_id },
        });
        setVisits((prev) => prev.map((item) => item.visita_id === exitVisit.visita_id ? { ...item, hora_fin: new Date().toISOString(), estado_visita: "PENDIENTE_ENVIO" } : item));
        setStatusMsgDuration(7500);
        setStatusMsg("Salida guardada localmente. Se enviará cuando vuelva la conexión.");
        return;
      }
      const message = err instanceof Error ? err.message : "No se pudo registrar la salida real.";
      if (message.includes("Faltan fotos DESPUES") || message.includes("No puedes registrar salida todavía")) {
        setPromotorModule("evidencias");
        setStatusMsgDuration(7500);
      }
      setStatusMsg(message);
    } finally {
      setSyncing(false);
    }
  }

  async function markSelectedBrandOutOfService() {
    try {
      if (!selectedVisitId) return setStatusMsg("Selecciona una visita activa.");
      if (!evidenceBrandId) return setStatusMsg("Selecciona una marca.");
      if (!outOfServiceReason.trim()) return setStatusMsg("Selecciona el motivo de fuera de servicio.");
      if (!getInitData()) return setStatusMsg("Esta acción real solo funciona desde Telegram.");
      const brandLabel = evidenceBrandLabel || availableBrands.find((item) => item.marca_id === evidenceBrandId)?.marca_nombre || evidenceBrandId;
      if (typeof window !== "undefined" && !window.confirm(`¿Marcar ${brandLabel} como fuera de servicio en esta visita?`)) return;
      setSyncing(true);
      let geo: Partial<LocationCapture> = {};
      try {
        geo = await getCurrentLocation();
      } catch {
        geo = {};
      }
      const response = await postJson<MarcaFueraServicioResponse>("/miniapp/promotor/marca-fuera-servicio", {
        visita_id: selectedVisitId,
        marca_id: evidenceBrandId,
        motivo: outOfServiceReason.trim(),
        comentario: outOfServiceComment.trim(),
        lat: geo.lat ?? "",
        lon: geo.lon ?? "",
        accuracy: geo.accuracy ?? "",
      });
      const row = response.row || { marca_id: evidenceBrandId, motivo: outOfServiceReason.trim(), comentario: outOfServiceComment.trim(), estatus: "ACTIVA" };
      setBrandsOutOfService((prev) => ({ ...prev, [evidenceBrandId]: row }));
      setEvidencePhotos([]);
      setEvidenceType("");
      setEvidenceDescription("");
      setOutOfServiceComment("");
      setShowOutOfServicePanel(false);
      setStatusMsg("Marca marcada fuera de servicio para esta visita. No contará como evidencia pendiente de esta marca.");
      await loadEvidenceContext(selectedVisitId);
      await loadPromotorOutOfService().catch(() => undefined);
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo marcar la marca fuera de servicio.");
    } finally {
      setSyncing(false);
    }
  }

  async function startExternalCameraForEvidence() {
    try {
      if (!selectedVisitId) {
        showCaptureGuard("Primero selecciona una visita/tienda activa. Después podrás tomar fotos de evidencia.");
        return;
      }
      if (selectedVisitHasNoBrands) return setStatusMsg("Esta tienda no tiene marcas activas para capturar evidencia.");
      if (!getInitData()) return setStatusMsg("Esta acción real solo funciona desde Telegram.");
      setSyncing(true);
      setStatusMsgDuration(9000);
      setStatusMsg("Abriendo espacio de cámara de evidencias...");
      const data = await postJson<ExternalCameraSessionResponse>("/miniapp/promotor/external-camera-session", {
        visita_id: selectedVisitId,
        marca_id: evidenceBrandId || "",
        marca_nombre: evidenceBrandLabel || "",
        tipo_evidencia: evidenceType || "",
        fase: evidencePhase || "ESTADO_ACTUAL",
        descripcion: evidenceDescription.trim(),
      });
      if (!data.capture_url) throw new Error("No se recibió URL de cámara externa.");
      if (tg?.openLink) {
        tg.openLink(data.capture_url, { try_instant_view: false });
      } else {
        window.open(data.capture_url, "_blank", "noopener,noreferrer");
      }
      setStatusMsg("Se abrió el espacio de cámara. Captura todas las fotos necesarias y finaliza para volver a PromoBolsillo.");
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo abrir el módulo de cámara.");
    } finally {
      setSyncing(false);
    }
  }

  async function saveEvidenceFlow() {
    let queuedPayload: Record<string, unknown> | null = null;
    try {
      if (!selectedVisitId) return setStatusMsg("Selecciona una visita activa.");
      if (!evidenceBrandLabel.trim()) return setStatusMsg("Selecciona una marca.");
      if (selectedBrandOutOfService) return setStatusMsg("Esta marca ya fue marcada fuera de servicio para esta visita.");
      if (!evidenceType.trim()) return setStatusMsg("Selecciona o escribe el tipo de evidencia.");
      if (!evidencePhotos.length) return setStatusMsg("Agrega al menos una foto de evidencia.");
      if (evidencePhotos.length < evidenceQty) return setStatusMsg(`Debes cargar al menos ${evidenceQty} foto(s).`);
      setSyncing(true);
      setStatusMsgDuration(7000);
      setStatusMsg("Guardando evidencia... puedes continuar cuando veas la confirmación.");
      const evidenceSource = evidencePhotos.some((photo) => !String(photo.name || "").startsWith("captura-")) ? "GALERIA_AUTORIZADA" : "CAMARA";
      const localPhotosForPreview = evidencePhotos.map((photo) => ({ ...photo }));
      const savedVisitId = selectedVisitId;
      const savedVisit = pendingVisits.find((item) => item.visita_id === savedVisitId);
      const savedBrandId = evidenceBrandId;
      const savedBrandLabel = evidenceBrandLabel;
      const savedEvidenceType = evidenceType;
      const savedEvidencePhase = evidencePhase;
      const savedDescription = evidenceDescription.trim();
      queuedPayload = {
        visita_id: selectedVisitId,
        marca_id: evidenceBrandId,
        marca_nombre: evidenceBrandLabel,
        tipo_evidencia: evidenceType,
        fase: evidencePhase,
        descripcion: evidenceDescription.trim(),
        source: evidenceSource,
        fotos: localPhotosForPreview.map((photo) => ({ name: photo.name, dataUrl: photo.dataUrl, capturedAt: photo.capturedAt })),
      };
      const e027UploadTimeoutMs = Math.min(
        240000,
        Math.max(60000, 45000 + localPhotosForPreview.length * 20000),
      );
      const result = await postJson<EvidenceRegisterResponse>(
        "/miniapp/promotor/evidence-register",
        queuedPayload,
        e027UploadTimeoutMs,
      );
      const createdIds = Array.isArray(result.created) ? result.created.filter(Boolean) : [];
      if (createdIds.length) {
        const previewEntries: Record<string, string> = {};
        createdIds.forEach((id, index) => {
          const photo = localPhotosForPreview[index] || localPhotosForPreview[0];
          if (photo?.dataUrl) previewEntries[id] = photo.dataUrl;
        });
        rememberLocalEvidencePreviews(previewEntries);
        const optimisticRows: UiEvidence[] = createdIds.map((id, index) => {
          const photo = localPhotosForPreview[index] || localPhotosForPreview[0];
          return {
            evidencia_id: id,
            visita_id: savedVisitId,
            tipo_evento: "EVIDENCIA_OPERATIVA",
            tipo_evidencia: savedEvidenceType,
            marca_id: savedBrandId,
            marca_nombre: savedBrandLabel,
            riesgo: "PENDIENTE",
            fecha_hora_fmt: formatDateTimeMaybe(photo?.capturedAt || new Date().toISOString()),
            fecha_hora: photo?.capturedAt || new Date().toISOString(),
            url_foto: photo?.dataUrl || "",
            descripcion: savedDescription || "Vista previa local",
            tienda_id: savedVisit?.tienda_id || "",
            tienda_nombre: savedVisit?.tienda_nombre || selectedVisitStoreName || "",
            tienda_display: savedVisit ? formatStoreDisplay(savedVisit.tienda_id, savedVisit.tienda_nombre) : selectedVisitStoreName,
            fase: savedEvidencePhase,
            status: "ACTIVA",
          };
        });
        setAllEvidenceRows((prev) => withLocalEvidencePreviews([...optimisticRows, ...prev.filter((row) => !createdIds.includes(row.evidencia_id))]));
        setSelectedEvidenceId(createdIds[0]);
        setPromotorModule("mis_evidencias");
      }
      setEvidenceType("");
      setEvidencePhase("ESTADO_ACTUAL");
      setEvidenceQty(1);
      setEvidenceDescription("");
      setEvidencePhotos([]);
      // E017: ya mostramos las nuevas evidencias con vista previa local.
      // Evitamos bloquear al promotor esperando un refresco completo de Sheets.
      window.setTimeout(() => { void loadEvidencesToday().catch(() => undefined); }, 1200);
      if (createdIds[0]) {
        setSelectedEvidenceId(createdIds[0]);
        setPromotorModule("mis_evidencias");
      }
      if ((result as any).postprocess_warning) {
        setStatusMsg("Evidencia registrada. Puedes continuar capturando; el análisis seguirá en segundo plano.");
      } else {
        setStatusMsg(result.warning === "evidence_photo_too_large_for_sheets" ? "Evidencia registrada; una foto quedó optimizada para Sheets." : "Evidencia registrada. Puedes continuar con la siguiente captura.");
      }
    } catch (err) {
      if (shouldQueueSubmission(err) && queuedPayload) {
        const visit = pendingVisits.find((item) => item.visita_id === selectedVisitId);
        upsertPendingOperation({
          id: buildPendingQueueId("QEVID"),
          kind: "evidence",
          createdAt: new Date().toISOString(),
          status: "PENDIENTE_ENVIO",
          attempts: 0,
          visitaId: selectedVisitId,
          tienda_id: visit?.tienda_id || "",
          tienda_nombre: visit?.tienda_nombre || selectedVisitStoreName || "",
          payload: queuedPayload,
        });
        setEvidenceType("");
        setEvidencePhase("ESTADO_ACTUAL");
        setEvidenceQty(1);
        setEvidenceDescription("");
        setEvidencePhotos([]);
        setStatusMsgDuration(7500);
        setStatusMsg("Evidencia guardada localmente. Se enviará cuando vuelva la conexión.");
        return;
      }
      const message = err instanceof Error ? err.message : "No se pudo registrar la evidencia.";
      if (message.includes("Primero debes registrar al menos 1 foto ANTES")) {
        setStatusMsgDuration(7500);
      }
      setStatusMsg(message);
    } finally {
      setSyncing(false);
    }
  }
  // E024 conserva funciones de captura heredada para no romper rutas de galería/reemplazo.
  void removeEvidencePhotoAt;
  void clearEvidencePhotos;
  void markSelectedBrandOutOfService;
  void saveEvidenceFlow;


  async function replaceEvidencePhotoPayload(fileName: string, dataUrl: string, source = "CAMARA") {
    if (!selectedEvidence) return;
    setSyncing(true);
    try {
      const result = await postJson<ReplaceEvidenceResponse>(
        "/miniapp/promotor/replace-evidence",
        {
          evidencia_id: selectedEvidence.evidencia_id,
          foto_nombre: fileName,
          foto_data_url: dataUrl,
          source,
        },
        60000,
      );
      await loadEvidencesToday();
      setStatusMsg(result.warning === "evidence_photo_too_large_for_sheets" ? "La evidencia se reemplazó, pero la foto no cupo completa en Sheets." : "Evidencia reemplazada.");
      setStatusMsgDuration(6800);
    } finally {
      setSyncing(false);
    }
  }

  async function markEvidenceAsCancelled() {
    const evidenceToCancel = selectedEvidence;
    try {
      if (!evidenceToCancel) return setStatusMsg("Selecciona una evidencia.");
      const confirmed = typeof window === "undefined" ? true : window.confirm(`¿Realmente deseas anular esta foto?

${getStoreDisplayFromItem(evidenceToCancel)}
${normalizeBrandLabel(evidenceToCancel.marca_nombre || "", "Marca")}
${evidenceToCancel.tipo_evidencia}
${evidenceToCancel.fecha_hora_fmt}`);
      if (!confirmed) return;
      setStatusMsg("Anulando evidencia...");
      setStatusMsgDuration(7000);
      setSyncing(true);
      await postJson("/miniapp/promotor/cancel-evidence", { evidencia_id: evidenceToCancel.evidencia_id, note: noteDraft || "" });
      setNoteDraft("");
      // E014B: actualización inmediata para evitar confusión visual.
      setAllEvidenceRows((prev) => prev.filter((row) => row.evidencia_id !== evidenceToCancel.evidencia_id));
      setSelectedEvidenceId("");
      closeImageViewer();
      await loadEvidencesToday();
      setStatusMsg("Evidencia anulada. La foto se retiró de la vista.");
    } catch (err) {
      await loadEvidencesToday().catch(() => undefined);
      setStatusMsg(err instanceof Error ? err.message : "No se pudo anular la evidencia.");
    } finally {
      setSyncing(false);
    }
  }


  async function saveNote() {
    try {
      if (!selectedEvidence || !noteDraft.trim()) return setStatusMsg("Escribe una nota y selecciona una evidencia.");
      setSyncing(true);
      await postJson("/miniapp/promotor/evidence-note", { evidencia_id: selectedEvidence.evidencia_id, note: noteDraft.trim() });
      setNoteDraft("");
      await loadEvidencesToday();
      setStatusMsg("Nota guardada.");
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo guardar la nota.");
    } finally {
      setSyncing(false);
    }
  }

  async function closeSelectedAlert() {
    try {
      if (!selectedAlert) return setStatusMsg("Selecciona una alerta.");
      setSyncing(true);
      await postJson<SupervisorAlertCloseResponse>("/miniapp/supervisor/alert-close", {
        alerta_id: selectedAlert.alerta_id,
        comentario_cierre: alertCloseNote.trim(),
        origen_cierre: "SUPERVISOR",
        status: alertFinalStatus,
      });
      setAlertCloseNote("");
      await loadSupervisorDashboard();
      await loadSupervisorAlerts();
      setStatusMsg(`Alerta ${alertFinalStatus.toLowerCase()}.`);
    } catch (err) {
      setStatusMsg(err instanceof Error ? err.message : "No se pudo cerrar la alerta.");
    } finally {
      setSyncing(false);
    }
  }

  async function applyEvidenceReviewBatch(
    evidenceIds: string[],
    decision: SupervisorDecision,
    note: string,
    options?: {
      clearSelection?: boolean;
      successMessage?: string;
      focusEvidenceId?: string;
      autoAdvance?: boolean;
    },
  ) {
    if (!evidenceIds.length) {
      setStatusMsg("Selecciona al menos una evidencia.");
      return;
    }

    const trimmedNote = note.trim();
    if ((decision === "OBSERVADA" || decision === "RECHAZADA") && !trimmedNote) {
      setStatusMsg(
        decision === "OBSERVADA"
          ? "Agrega un comentario para comentar la evidencia."
          : "Agrega un motivo para rechazar la evidencia.",
      );
      return;
    }

    const visibleBefore = filteredSupervisorEvidences;
    const focusId = options?.focusEvidenceId || evidenceIds[0] || "";
    const focusIndex = focusId
      ? visibleBefore.findIndex((item) => item.evidencia_id === focusId)
      : -1;
    const reviewedSet = new Set(evidenceIds);
    const nextCandidate = visibleBefore
      .slice(Math.max(0, focusIndex + 1))
      .find((item) => !reviewedSet.has(item.evidencia_id))
      || [...visibleBefore]
        .slice(0, Math.max(0, focusIndex))
        .reverse()
        .find((item) => !reviewedSet.has(item.evidencia_id))
      || null;

    const sourceItems = supervisorEvidences.filter((item) => reviewedSet.has(item.evidencia_id));
    const snapshots = sourceItems.map(snapshotSupervisorEvidence);
    const queuedItems: SupervisorReviewQueueItem[] = snapshots.map((previous) => ({
      id: buildSupervisorReviewQueueId(),
      evidencia_id: previous.evidencia_id,
      decision_supervisor: decision,
      motivo_revision: trimmedNote,
      previous,
      createdAt: new Date().toISOString(),
      attempts: 0,
      lastError: "",
    }));

    setReviewActionInProgress(decision);

    setSupervisorEvidences((previous) => previous.map((item) => {
      if (!reviewedSet.has(item.evidencia_id)) return item;
      const queueItem = queuedItems.find((queued) => queued.evidencia_id === item.evidencia_id);
      return queueItem ? applySupervisorReviewTarget(item, queueItem) : item;
    }));

    enqueueSupervisorReviewItems(queuedItems);

    if (options?.clearSelection) setSelectedSupEvidenceIds([]);
    setReviewNote("");

    if (nextCandidate) {
      setSelectedSupEvidenceId(nextCandidate.evidencia_id);
      void ensureSupervisorEvidencePhoto(nextCandidate, { silent: true });
      preloadFollowingSupervisorPhotos(nextCandidate.evidencia_id, 2);
      if (imageViewerEvidenceId) {
        void openSupervisorEvidenceViewer(nextCandidate);
      }
    } else if (imageViewerEvidenceId) {
      closeImageViewer();
    }

    const undoState: SupervisorReviewUndoState = {
      id: `UNDO-${Date.now()}`,
      items: snapshots,
      expiresAt: Date.now() + SUPERVISOR_REVIEW_UNDO_MS,
    };
    setLastSupervisorReviewUndo(undoState);
    if (supervisorReviewUndoTimerRef.current != null) {
      window.clearTimeout(supervisorReviewUndoTimerRef.current);
    }
    supervisorReviewUndoTimerRef.current = window.setTimeout(() => {
      setLastSupervisorReviewUndo((current) => (
        current?.id === undoState.id ? null : current
      ));
      supervisorReviewUndoTimerRef.current = null;
    }, SUPERVISOR_REVIEW_UNDO_MS);

    const decisionLabel = decision === "OBSERVADA"
      ? "comentada"
      : decision === "RECHAZADA"
        ? "rechazada"
        : "aprobada";
    setStatusMsgDuration(5000);
    setStatusMsg(
      options?.successMessage
      || `${evidenceIds.length} evidencia(s) ${decisionLabel}(s). Puedes continuar.`,
    );

    window.setTimeout(() => setReviewActionInProgress(null), 120);
  }

  function undoLastSupervisorReview() {
    const undo = lastSupervisorReviewUndo;
    if (!undo?.items.length) return;

    const restoreItems: SupervisorReviewQueueItem[] = undo.items.map((previous) => ({
      id: buildSupervisorReviewQueueId(),
      evidencia_id: previous.evidencia_id,
      decision_supervisor: "",
      motivo_revision: "",
      restore: true,
      previous,
      createdAt: new Date().toISOString(),
      attempts: 0,
      lastError: "",
    }));

    const restoreMap = new Map(restoreItems.map((item) => [item.evidencia_id, item]));
    setSupervisorEvidences((previous) => previous.map((item) => {
      const restore = restoreMap.get(item.evidencia_id);
      return restore ? applySupervisorReviewTarget(item, restore) : item;
    }));
    enqueueSupervisorReviewItems(restoreItems);

    const restoredFocus = undo.items[0]?.evidencia_id || "";
    if (restoredFocus) setSelectedSupEvidenceId(restoredFocus);
    setLastSupervisorReviewUndo(null);
    if (supervisorReviewUndoTimerRef.current != null) {
      window.clearTimeout(supervisorReviewUndoTimerRef.current);
      supervisorReviewUndoTimerRef.current = null;
    }
    setStatusMsg("Última acción deshecha. El cambio se sincronizará automáticamente.");
  }

  async function reviewSelectedEvidence() {
    if (!selectedSupervisorEvidence) return setStatusMsg("Selecciona una evidencia.");
    await applyEvidenceReviewBatch([selectedSupervisorEvidence.evidencia_id], reviewDecision, reviewNote, {
      successMessage: `Evidencia ${reviewDecision.toLowerCase()}.`,
      focusEvidenceId: selectedSupervisorEvidence.evidencia_id,
      autoAdvance: !!imageViewerEvidenceId,
    });
  }

  async function quickReviewEvidence(item: EvidenceItem, decision: SupervisorDecision) {
    const requiresNote = decision === "OBSERVADA" || decision === "RECHAZADA";
    const prompted = requiresNote ? window.prompt(decision === "OBSERVADA" ? "Escribe el comentario:" : "Escribe el motivo del rechazo:", reviewNote || item.motivo_revision || "") : (reviewNote || item.motivo_revision || "");
    if (requiresNote && !String(prompted || "").trim()) return setStatusMsg(decision === "OBSERVADA" ? "Se canceló el comentario por falta de texto." : "Se canceló el rechazo por falta de motivo.");
    setSelectedSupEvidenceId(item.evidencia_id);
    await applyEvidenceReviewBatch([item.evidencia_id], decision, String(prompted || ""), {
      successMessage: `${item.tipo_evidencia || "Evidencia"} ${decision === "OBSERVADA" ? "comentada" : decision === "RECHAZADA" ? "rechazada" : "aprobada"}.`,
      focusEvidenceId: item.evidencia_id,
      autoAdvance: imageViewerEvidenceId === item.evidencia_id,
    });
  }

  function toggleSupervisorEvidenceSelection(evidenceId: string) {
    setSelectedSupEvidenceId(evidenceId);
    setSelectedSupEvidenceIds((prev) => prev.includes(evidenceId) ? prev.filter((id) => id !== evidenceId) : [...prev, evidenceId]);
  }

  function selectAllVisibleSupervisorEvidences() {
    const ids = visibleSupervisorQueueItems.map((item) => item.evidencia_id);
    setSelectedSupEvidenceIds(ids);
    if (ids[0]) setSelectedSupEvidenceId(ids[0]);
  }

  function selectBrandSupervisorEvidences(brandKey: string) {
    const group = groupedSupervisorEvidences.find((item) => item.brandKey === brandKey);
    const ids = group?.items.map((item) => item.evidencia_id) || [];
    setSelectedSupEvidenceIds(ids);
    if (ids[0]) setSelectedSupEvidenceId(ids[0]);
  }

  async function runBatchEvidenceReview(decision: SupervisorDecision) {
    await applyEvidenceReviewBatch(selectedSupEvidenceIds, decision, reviewNote, { clearSelection: true });
  }

  async function runBrandEvidenceReview(brandKey: string, decision: SupervisorDecision) {
    const group = groupedSupervisorEvidences.find((item) => item.brandKey === brandKey);
    if (!group?.items.length) return setStatusMsg("No hay evidencias visibles en ese grupo.");
    const requiresNote = decision === "OBSERVADA" || decision === "RECHAZADA";
    const prompted = requiresNote ? window.prompt(decision === "OBSERVADA" ? `Comentario general para ${group.brandLabel}:` : `Motivo general de rechazo para ${group.brandLabel}:`, reviewNote || "") : reviewNote;
    if (requiresNote && !String(prompted || "").trim()) return setStatusMsg(decision === "OBSERVADA" ? "Se canceló el comentario masivo por falta de texto." : "Se canceló el rechazo masivo por falta de motivo.");
    setSelectedSupEvidenceId(group.items[0].evidencia_id);
    await applyEvidenceReviewBatch(group.items.map((item) => item.evidencia_id), decision, String(prompted || ""), {
      successMessage: `${group.items.length} evidencia(s) de ${group.brandLabel} ${decision === "OBSERVADA" ? "observadas" : decision === "RECHAZADA" ? "rechazadas" : "aprobadas"}.`,
      focusEvidenceId: group.items[0].evidencia_id,
    });
  }

  function applySupervisorMainFilters() {
    setSupEvidenceDatePreset(draftSupEvidenceDatePreset);
    setSupEvidenceDateStart(draftSupEvidenceDateStart || localYmd());
    setSupEvidenceDateEnd(draftSupEvidenceDateEnd || draftSupEvidenceDateStart || localYmd());
    setSupReviewContentFilter(draftSupReviewContentFilter);
    setSupEvidenceStatusFilter(draftSupEvidenceStatusFilter);
    setSupEvidenceOnlyPending(false);
    setSelectedSupEvidenceIds([]);
    setSupEvidenceGroupPage(1);
    setActiveSupEvidenceGroupKey("");
    setStatusMsgDuration(4200);
    setStatusMsg("Sincronizando resultados con los filtros seleccionados...");
    void refreshCurrentRoleData();
  }

  function clearSupervisorEvidenceFilters() {
    setSupEvidencePromotorFilter("");
    setSupEvidenceStoreFilter("");
    setSupEvidenceBrandFilter("");
    setSupEvidenceTypeFilter("");
    setSupEvidencePhaseFilter("");
    setSupEvidenceRiskFilter("");
    setSupEvidenceStatusFilter("PENDIENTE");
    setSupEvidenceOnlyPending(false);
    setSupEvidenceDatePreset("hoy");
    setSupEvidenceDateStart(localYmd());
    setSupEvidenceDateEnd(localYmd());
    setSupReviewContentFilter("evidencias");
    setDraftSupEvidenceDatePreset("hoy");
    setDraftSupEvidenceDateStart(localYmd());
    setDraftSupEvidenceDateEnd(localYmd());
    setDraftSupReviewContentFilter("evidencias");
    setDraftSupEvidenceStatusFilter("PENDIENTE");
  }

  function scrollElementIntoView(ref: { current: HTMLElement | HTMLDivElement | null }, block: ScrollLogicalPosition = "start") {
    ref.current?.scrollIntoView({ behavior: "smooth", block });
  }

  function scrollHorizontalRefToEnd(ref: { current: HTMLDivElement | null }) {
    const node = ref.current;
    if (!node) return;
    node.scrollTo({ left: node.scrollWidth, behavior: "smooth" });
  }

  function scrollHorizontalRefToStart(ref: { current: HTMLDivElement | null }) {
    const node = ref.current;
    if (!node) return;
    node.scrollTo({ left: 0, behavior: "smooth" });
  }

  function focusSupervisorEvidence(item: EvidenceItem) {
    setSelectedSupEvidenceId(item.evidencia_id);
    void ensureSupervisorEvidencePhoto(item, { silent: true });
    window.setTimeout(() => scrollElementIntoView(supervisorReviewDetailRef, "start"), 40);
  }

  function focusPromotorEvidence(item: EvidenceItem) {
    setSelectedPromotorOutOfServiceId("");
    setSelectedEvidenceId(item.evidencia_id);
    if (promotorModule !== "mis_evidencias") setPromotorModule("mis_evidencias");
    window.setTimeout(() => scrollElementIntoView(promotorDetailRef, "start"), 60);
  }

  function focusPromotorOutOfService(item: SupervisorOutOfServiceItem) {
    setSelectedEvidenceId("");
    setSelectedPromotorOutOfServiceId(item.registro_id);
    if (promotorModule !== "mis_evidencias") setPromotorModule("mis_evidencias");
    window.setTimeout(() => scrollElementIntoView(promotorDetailRef, "start"), 60);
  }

  function getFreshExternalCameraMarker() {
    const marker = safeReadLocalStorage(EXTERNAL_CAMERA_LAST_SUCCESS_KEY);
    if (!marker) return { raw: "", isFresh: false };
    const handled = safeReadLocalStorage(EXTERNAL_CAMERA_LAST_HANDLED_KEY);
    if (marker === handled) return { raw: marker, isFresh: false };
    try {
      const parsed = JSON.parse(marker) as { registeredAt?: string };
      const registeredAt = parsed?.registeredAt ? new Date(parsed.registeredAt).getTime() : 0;
      const isRecent = registeredAt > 0 && Date.now() - registeredAt < 20 * 60 * 1000;
      return { raw: marker, isFresh: isRecent };
    } catch {
      return { raw: marker, isFresh: true };
    }
  }

  async function refreshPromotorAfterCameraReturn() {
    if (role !== "promotor" || promotorResumeRefreshInFlightRef.current) return;
    promotorResumeRefreshInFlightRef.current = true;
    const marker = getFreshExternalCameraMarker();
    try {
      setPromotorModule("mis_evidencias");
      setPromotorEvidenceViewFilter("fotos");
      setStatusMsgDuration(12000);
      setStatusMsg(marker.isFresh
        ? "Foto registrada correctamente. Actualizando Mis fotos..."
        : "Actualizando Mis fotos...");

      let loaded = false;
      let lastError: unknown = null;
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
          await loadEvidencesToday(60000);
          loaded = true;
          break;
        } catch (err) {
          lastError = err;
          if (attempt === 0) await waitMs(1600);
        }
      }

      void loadPromotorDashboard().catch(() => undefined);
      void loadPromotorOutOfService().catch(() => undefined);

      if (loaded) {
        setStatusMsg(marker.isFresh
          ? "Foto registrada correctamente. Ya está disponible en Mis fotos."
          : "Mis fotos se actualizaron correctamente.");
      } else {
        setStatusMsg(marker.isFresh
          ? "La foto quedó registrada. Mis fotos está tardando en actualizarse; puedes continuar y revisarla en unos segundos."
          : friendlyRequestError(lastError, "Mis fotos está tardando en actualizarse."));
      }
      if (marker.raw) safeWriteLocalStorage(EXTERNAL_CAMERA_LAST_HANDLED_KEY, marker.raw);
    } finally {
      promotorResumeRefreshInFlightRef.current = false;
    }
  }

  async function refreshCurrentRoleData() {
    if (manualRefreshInFlightRef.current) return;
    manualRefreshInFlightRef.current = true;
    try {
      setSyncing(true);
      if (role === "promotor") {
        const errors: unknown[] = [];
        try { await loadPromotorDashboard(); } catch (err) { errors.push(err); }
        try { await loadEvidencesToday(60000); } catch (err) { errors.push(err); }
        try { await loadPromotorOutOfService(); } catch (err) { errors.push(err); }
        if (errors.length >= 3) {
          setStatusMsg(friendlyRequestError(errors[0], "No se pudo actualizar en este momento."));
        } else if (errors.length) {
          setStatusMsg("La información principal se actualizó; una sección tardó más de lo esperado.");
        } else {
          setStatusMsg("Información actualizada.");
        }
        return;
      }
      if (role === "supervisor") {
        await loadSupervisorDashboard();
        await loadSupervisorTeam();
        await loadSupervisorAlerts();
        await loadSupervisorEvidences();
        await loadSupervisorOutOfService();
      }
      if (role === "cliente") {
        await loadClientBootstrap();
        await loadClientFilterOptions();
        await loadClientDashboard();
        await loadClientStores();
        await loadClientEvidences();
        await loadClientIncidents();
        await loadClientDeliverables();
        if (selectedClientStoreId) await loadClientStoreDetail(selectedClientStoreId);
      }
      setStatusMsg("Información actualizada.");
    } catch (err) {
      setStatusMsg(friendlyRequestError(err, "No se pudo recargar."));
    } finally {
      manualRefreshInFlightRef.current = false;
      setSyncing(false);
    }
  }

  async function moveSupervisorEvidenceViewer(step: number) {
    if (activeViewerSupervisorEvidenceIndex < 0) return;
    const next = activeViewerSupervisorEvidenceSequence[activeViewerSupervisorEvidenceIndex + step];
    if (!next) return;
    setSelectedSupEvidenceId(next.evidencia_id);
    await openSupervisorEvidenceViewer(next);
  }

  // E013 keeps legacy supervisor helpers referenced so TypeScript noUnusedLocals stays clean while the main UX is simplified.
  void supervisorEvidenceAudit;
  void supEvidenceGroupMode;
  void setSupEvidenceGroupMode;
  void pagedSupervisorEvidenceGroups;
  void reviewSelectedEvidence;
  void toggleSupervisorEvidenceSelection;
  void selectAllVisibleSupervisorEvidences;
  void selectBrandSupervisorEvidences;
  void runBatchEvidenceReview;
  void runBrandEvidenceReview;

  if (isExternalCameraMode) {
    return <ExternalCameraCapturePage token={externalCameraToken} />;
  }

  if (loading) {
    return (
      <div style={styles.page}>
        <style>{globalCss}</style>
        <div className="shell">
          <div className="card loadingCard">
            <div className="loadingRow">
              <RefreshCw className="spin" size={18} />
              <span>Cargando operación...</span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={styles.page}>
      <style>{globalCss + `
.e020CaptureGuard { margin: 10px 0 8px; border: 1px solid rgba(245,158,11,.45); background: rgba(255,247,237,.95); color: #7c2d12; border-radius: 14px; padding: 10px 12px; font-weight: 850; line-height: 1.35; }
.outOfServiceBox { max-width: 430px; justify-self: start; }
.outOfServiceDetailCard { border: 1px solid rgba(245, 158, 11, .35); background: rgba(255, 247, 237, .72); border-radius: 16px; padding: 12px; display: grid; gap: 7px; }

/* E024_EXTERNAL_CAMERA_UX_WORKSPACE */
.e024EvidenceLaunchPanel {
  display: grid;
  gap: 12px;
}
.e024StoreHero {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px;
  border-radius: 18px;
  background: linear-gradient(135deg, rgba(34, 197, 94, .12), rgba(14, 165, 233, .10));
  border: 1px solid rgba(15, 23, 42, .08);
}
.e024StoreLabel {
  font-size: 11px;
  font-weight: 950;
  text-transform: uppercase;
  color: #64748b;
  letter-spacing: .06em;
}
.e024StoreName {
  font-size: 18px;
  font-weight: 1000;
  color: #0f172a;
}
.e024OpenCameraBtn {
  justify-content: center !important;
  text-align: center !important;
  min-height: 62px;
  border-radius: 22px;
}
.e024EvidenceSummary {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 8px;
}
.e024EvidenceSummary > div {
  border-radius: 16px;
  background: #f8fafc;
  border: 1px solid rgba(15,23,42,.08);
  padding: 10px;
  text-align: center;
}
.e024EvidenceSummary strong {
  display: block;
  font-size: 22px;
  font-weight: 1000;
  color: #0f172a;
}
.e024EvidenceSummary span {
  display: block;
  font-size: 11px;
  font-weight: 850;
  color: #64748b;
}
.e024ResultGalleryPanel {
  margin-top: 12px;
}
.e024VisitGalleryRail {
  display: flex;
  gap: 10px;
  overflow-x: auto;
  padding: 4px 2px 8px;
  scroll-snap-type: x proximity;
}
.e024VisitGalleryCard {
  min-width: 122px;
  max-width: 122px;
  border-radius: 18px;
  overflow: hidden;
  background: #f8fafc;
  border: 1px solid rgba(15,23,42,.08);
  box-shadow: 0 10px 24px rgba(15,23,42,.06);
  cursor: pointer;
  scroll-snap-align: start;
}
.e024VisitGalleryCard img {
  width: 100%;
  aspect-ratio: 1;
  object-fit: cover;
  display: block;
}
.e024VisitGalleryMeta {
  padding: 8px;
  line-height: 1.15;
}
.e024VisitGalleryMeta strong {
  display: block;
  font-size: 11px;
  color: #334155;
}
.e024VisitGalleryMeta span {
  display: block;
  margin-top: 3px;
  font-size: 10px;
  color: #64748b;
  font-weight: 800;
}


/* E024I_MIS_FOTOS_LAYOUT_ROWS */
.e024iEvidenceListBtn {
  padding: 11px 12px;
}
.e024iDataRows {
  display: grid;
  gap: 5px;
  min-width: 0;
}
.e024iDataRows > div {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr);
  align-items: start;
  gap: 8px;
  line-height: 1.25;
  min-width: 0;
}
.e024iDataRows span {
  color: #78909c;
  font-size: 11px;
  font-weight: 900;
  text-transform: uppercase;
  letter-spacing: .02em;
}
.e024iDataRows strong {
  color: #263238;
  font-size: 12.5px;
  font-weight: 900;
  overflow-wrap: anywhere;
}
.e024iCommentLine {
  margin-top: 8px;
  color: #607d8b;
  font-size: 12px;
  line-height: 1.35;
}
.e024iDetailEvidenceCard {
  display: grid;
  grid-template-columns: 108px minmax(0, 1fr);
  gap: 12px;
  align-items: stretch;
  border-radius: 18px;
  border: 1px solid rgba(38,50,56,0.10);
  background: rgba(255,255,255,0.96);
  padding: 10px;
  margin-bottom: 12px;
  cursor: pointer;
}
.e024iDetailImageFrame {
  width: 108px;
  height: 108px;
  aspect-ratio: auto;
  border-radius: 16px;
}
.e024iDetailBody {
  min-width: 0;
  display: flex;
  align-items: center;
}
.e024iGalleryEvidenceCard {
  min-width: 328px;
  max-width: 328px;
  grid-template-columns: 76px minmax(0, 1fr);
}
.e024iGalleryImageFrame {
  width: 76px;
  height: 76px;
}
.e024iGalleryRows > div {
  grid-template-columns: 50px minmax(0, 1fr);
  gap: 6px;
}
.e024iGalleryRows span {
  font-size: 10px;
}
.e024iGalleryRows strong {
  font-size: 11.5px;
}
@media (max-width: 760px) {
  .e024iDetailEvidenceCard {
    grid-template-columns: 92px minmax(0, 1fr);
    gap: 10px;
  }
  .e024iDetailImageFrame {
    width: 92px;
    height: 92px;
  }
  .e024iDataRows > div {
    grid-template-columns: 54px minmax(0, 1fr);
  }
  .e024iGalleryEvidenceCard {
    min-width: 306px;
    max-width: 306px;
  }
}

`}</style>
      <div className="shell">
        <div className="stickyTop">
          <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="hero heroSplit">
            <div className="heroLogoBlock heroLogoBlockE014E">
              <img className="rezgoLogoE014E" src={REZGO_HORIZONTAL_LOGO_E014E} alt="REZGO" />
              <div className="rezgoTaglineE014E" data-tagline="Pasion por la movilidad" aria-label="Pasión por la movilidad">Pasión por la movilidad</div>
            </div>
            <div className="heroTitleBlock heroTitleBlockWide">
              <div className="heroTitle heroTitleTight">{role === "supervisor" ? <>Operación<br />supervisor</> : role === "cliente" ? <>Consulta<br />cliente</> : role === "promotor" ? <>Operación<br />del promotor</> : <>Acceso<br />no configurado</>}</div>
              <div className="heroMetaSingle heroMetaSingleWide">{actorLabel}</div>
            </div>
          </motion.div>

          {role === "supervisor" ? (
            <div className="tabsBar tabsInline">
              {supervisorTabs.map((tab) => (
                <button key={tab.key} className={`tabBtn ${supervisorModule === tab.key ? "tabBtnActive" : ""}`} onClick={() => setSupervisorModule(tab.key)}>
                  {tab.label}
                </button>
              ))}
            </div>
          ) : role === "cliente" ? (
            <div className="tabsBar tabsInline">
              {clientTabs.map((tab) => (
                <button key={tab.key} className={`tabBtn ${clientModule === tab.key ? "tabBtnActive" : ""}`} onClick={() => setClientModule(tab.key)}>
                  {tab.label}
                </button>
              ))}
            </div>
          ) : role === "promotor" ? (
            <div className="tabsBar tabsInline">
              {promotorTabs.map((tab) => (
                <button key={tab.key} className={`tabBtn ${promotorModule === tab.key ? "tabBtnActive" : ""}`} onClick={() => setPromotorModule(tab.key)}>
                  {tab.label}
                </button>
              ))}
            </div>
          ) : null}
        </div>

        {error ? (
          <div className="card warning">
            <div className="warningRow">
              <AlertTriangle size={18} />
              <span style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{error}</span>
            </div>
          </div>
        ) : null}

        {!role ? (
          <div className="card">
            <div className="sectionTitle">Acceso pendiente de configuración</div>
            <div className="helperText">
              Esta cuenta no fue reconocida con un rol válido en la plataforma. Si este acceso debe entrar como cliente, promotor o supervisor, valida el external_id correspondiente y vuelve a abrir la mini app desde Telegram.
            </div>
            {detectedExternalId ? (
              <div className="miniKpi" style={{ marginTop: 12 }}>
                <div className="miniKpiLabel">external_id detectado</div>
                <div className="miniKpiValue" style={{ fontSize: 14, wordBreak: "break-all" }}>{detectedExternalId}</div>
              </div>
            ) : null}
          </div>
        ) : null}

        {role === "cliente" ? (
          <div className="card">
            <div className="sectionTitle">Filtros del cliente</div>
            <div className="filtersRow twoColsFilters">
              <input className="inputLike" type="date" value={clientFilters.fecha_inicio} onChange={(e) => setClientFilters((prev) => ({ ...prev, fecha_inicio: e.target.value }))} />
              <input className="inputLike" type="date" value={clientFilters.fecha_fin} onChange={(e) => setClientFilters((prev) => ({ ...prev, fecha_fin: e.target.value }))} />
              <select className="inputLike" value={clientFilters.cadena} onChange={(e) => setClientFilters((prev) => ({ ...prev, cadena: e.target.value, tienda_id: "" }))}>
                <option value="">Todas las cadenas</option>
                {clientFilterOptions.cadenas.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.region} onChange={(e) => setClientFilters((prev) => ({ ...prev, region: e.target.value, tienda_id: "" }))}>
                <option value="">Todas las regiones</option>
                {clientFilterOptions.regiones.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.tienda_id} onChange={(e) => setClientFilters((prev) => ({ ...prev, tienda_id: e.target.value }))}>
                <option value="">Todas las tiendas</option>
                {clientFilterOptions.tiendas.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.marca_id} onChange={(e) => setClientFilters((prev) => ({ ...prev, marca_id: e.target.value }))}>
                <option value="">Todas las marcas</option>
                {clientFilterOptions.marcas.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
            </div>
          </div>
        ) : null}

        {role === "cliente" && clientModule === "resumen" ? (
          <div className="card">
            <div className="sectionTitle">Resumen cliente</div>
            <div className="summaryLine">Periodo: <strong>{clientDashboard.period?.label || `${clientFilters.fecha_inicio} a ${clientFilters.fecha_fin}`}</strong></div>
            <div className="summaryGrid">
              <div className="summaryBlock kpiBlock"><Store size={16} /><div className="kpiValue">{clientDashboard.kpis?.tiendas_visibles || 0}</div><div className="kpiLabel">Tiendas visibles</div></div>
              <div className="summaryBlock kpiBlock"><ClipboardList size={16} /><div className="kpiValue">{clientDashboard.kpis?.visitas || 0}</div><div className="kpiLabel">Visitas</div></div>
              <div className="summaryBlock kpiBlock"><CheckCircle2 size={16} /><div className="kpiValue">{clientDashboard.kpis?.cumplimiento_pct || 0}%</div><div className="kpiLabel">Cumplimiento</div></div>
              <div className="summaryBlock kpiBlock"><ImageIcon size={16} /><div className="kpiValue">{clientDashboard.kpis?.evidencias || 0}</div><div className="kpiLabel">Evidencias</div></div>
              <div className="summaryBlock kpiBlock"><Check size={16} /><div className="kpiValue">{clientDashboard.kpis?.aprobadas || 0}</div><div className="kpiLabel">Aprobadas</div></div>
              <div className="summaryBlock kpiBlock"><AlertTriangle size={16} /><div className="kpiValue">{clientDashboard.kpis?.alertas || 0}</div><div className="kpiLabel">Alertas</div></div>
            </div>
            <div className="twoCol">
              <div className="panel">
                <div className="miniTitle">Marcas / cuenta</div>
                <div className="summaryLine"><strong>{clientBranding.cliente_nombre || actorLabel}</strong></div>
                <div className="summaryLine">Geocerca OK: <strong>{clientDashboard.kpis?.geocerca_ok_pct || 0}%</strong></div>
                <div className="summaryLine">Observadas: <strong>{clientDashboard.kpis?.observadas || 0}</strong></div>
                <div className="summaryLine">Rechazadas: <strong>{clientDashboard.kpis?.rechazadas || 0}</strong></div>
              </div>
              <div className="panel">
                <div className="miniTitle">Top incidencias</div>
                <div className="stack compactStack">
                  {(clientDashboard.top_alerts || []).map((item) => (
                    <div className="listBtn" key={item.tipo_alerta}>
                      <div className="listTitle">{item.tipo_alerta}</div>
                      <div className="listSub">{item.total} registro(s)</div>
                    </div>
                  ))}
                  {!(clientDashboard.top_alerts || []).length ? <div className="emptyBox">Sin incidencias relevantes en el periodo.</div> : null}
                </div>
              </div>
            </div>
          </div>
        ) : null}

        {role === "cliente" && clientModule === "tiendas" ? (
          <div className="card">
            <div className="sectionTitle">Tiendas</div>
            <div className="twoCol">
              <div className="panel">
                <div className="miniTitle">Listado</div>
                <div className="stack compactStack">
                  <div ref={promotorListTopRef} className="e018ScrollAnchor" />
                  {clientStores.map((item) => (
                    <button key={item.tienda_id} className={`listBtn ${selectedClientStoreId === item.tienda_id ? "listBtnGreen" : ""}`} onClick={() => setSelectedClientStoreId(item.tienda_id)}>
                      <div className="listTitle">{item.tienda_nombre}</div>
                      <div className="listSub">{item.cadena || "Sin cadena"} · {item.region || "Sin región"}</div>
                      <div className="geoRow">
                        <span className={`riskBadge ${statusClass(item.estatus)}`}>{item.estatus}</span>
                        <span className="riskBadge riskGreen">Visitas {item.visitas}</span>
                      </div>
                    </button>
                  ))}
                  {!clientStores.length ? <div className="emptyBox">No hay tiendas con actividad para los filtros seleccionados.</div> : null}
                </div>
              </div>
              <div className="panel">
                <div className="miniTitle">Detalle</div>
                {clientStoreDetail?.store ? (
                  <>
                    <div className="summaryLine"><strong>{clientStoreDetail.store.nombre_tienda || clientStoreDetail.store.tienda_id}</strong></div>
                    <div className="summaryLine">Cadena: {clientStoreDetail.store.cadena || "-"}</div>
                    <div className="summaryLine">Región: {clientStoreDetail.store.region || "-"}</div>
                    <div className="summaryLine">Ciudad: {clientStoreDetail.store.ciudad || "-"}</div>
                    <div className="summaryLine">Visitas: <strong>{clientStoreDetail.summary?.visitas || 0}</strong></div>
                    <div className="summaryLine">Evidencias: <strong>{clientStoreDetail.summary?.evidencias || 0}</strong></div>
                    <div className="summaryLine">Aprobadas: <strong>{clientStoreDetail.summary?.aprobadas || 0}</strong></div>
                    <div className="summaryLine">Observadas: <strong>{clientStoreDetail.summary?.observadas || 0}</strong></div>
                    <div className="summaryLine">Alertas: <strong>{clientStoreDetail.summary?.alertas || 0}</strong></div>
                  </>
                ) : <div className="emptyBox">Selecciona una tienda.</div>}
              </div>
            </div>
          </div>
        ) : null}

        {role === "cliente" && clientModule === "evidencias" ? (
          <div className="card e011GroupedEvidenceHub">
            <div className="sectionTitle">Evidencias presentables</div>
            <div className="contextHint">Primero se muestran grupos de análisis; las fotos se revisan en detalle solo al abrir el grupo o el visor ampliado.</div>
            <div className="filtersRow twoColsFilters">
              <select className="inputLike" value={clientFilters.tipo_evidencia} onChange={(e) => setClientFilters((prev) => ({ ...prev, tipo_evidencia: e.target.value }))}>
                <option value="">Todos los tipos</option>
                {clientFilterOptions.tipos_evidencia.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.decision_supervisor} onChange={(e) => setClientFilters((prev) => ({ ...prev, decision_supervisor: e.target.value }))}>
                <option value="">Aprobadas + observadas</option>
                {clientFilterOptions.decisiones.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.riesgo} onChange={(e) => setClientFilters((prev) => ({ ...prev, riesgo: e.target.value }))}>
                <option value="">Todos los riesgos</option>
                {clientFilterOptions.riesgos.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
            </div>
            <div className="e011ModeRow">
              <span className="e011ModeLabel">Agrupar por</span>
              {(["marca", "tienda", "estatus"] as EvidenceGroupMode[]).map((mode) => (
                <button key={mode} className={`e011ModeBtn ${clientEvidenceGroupMode === mode ? "e011ModeBtnActive" : ""}`} onClick={() => setClientEvidenceGroupMode(mode)}>{getEvidenceGroupModeLabel(mode)}</button>
              ))}
            </div>

            {!clientEvidences.length ? <div className="emptyBox">No hay evidencias para mostrar.</div> : null}

            {clientEvidences.length ? (
              <>
                <div className="e011EvidenceBoardHeader">
                  <div>
                    <div className="miniTitle">Grupos visibles</div>
                    <div className="contextHint">{clientEvidenceGroups.length} grupo(s), {clientEvidences.length} evidencia(s). Página {clientEvidenceGroupSafePage} de {clientEvidenceGroupPageCount}.</div>
                  </div>
                  <div className="e011Pager">
                    <button className="actionButton compactBtn" disabled={clientEvidenceGroupSafePage <= 1} onClick={() => setClientEvidenceGroupPage((prev) => Math.max(1, prev - 1))}>‹ Anterior</button>
                    <button className="actionButton compactBtn" disabled={clientEvidenceGroupSafePage >= clientEvidenceGroupPageCount} onClick={() => setClientEvidenceGroupPage((prev) => Math.min(clientEvidenceGroupPageCount, prev + 1))}>Siguiente ›</button>
                  </div>
                </div>
                <div className="e011GroupBoard">
                  {pagedClientEvidenceGroups.map((group) => (
                    <button key={group.brandKey} type="button" className={`e011GroupTile ${activeClientEvidenceGroup?.brandKey === group.brandKey ? "e011GroupTileActive" : ""}`} onClick={() => setActiveClientEvidenceGroupKey(group.brandKey)}>
                      <div className="e011GroupTileTop">
                        <div>
                          <div className="e011GroupKind">{getEvidenceGroupModeLabel(clientEvidenceGroupMode)}</div>
                          <div className="e011GroupTitle">{group.brandLabel}</div>
                        </div>
                        <span className="riskBadge riskNeutral">{group.total}</span>
                      </div>
                      <div className="e011MiniCollage">
                        {group.previewItems.map((item) => <img key={item.evidencia_id} src={item.url_foto} alt={item.tipo_evidencia || "Evidencia"} />)}
                        {!group.previewItems.length ? <div className="e011EmptyThumb"><ImageIcon size={18} /></div> : null}
                      </div>
                      <div className="e011GroupStats">
                        <span className="riskBadge riskGreen">{group.aprobadas} aprobadas</span>
                        <span className="riskBadge riskAmber">{group.observadas} observadas</span>
                        <span className="riskBadge riskRed">{group.rechazadas} rechazadas</span>
                      </div>
                      <div className="e011GroupMeta">{group.tiendas.slice(0, 2).join(" · ") || group.brandSubtitle}</div>
                    </button>
                  ))}
                </div>

                {activeClientEvidenceGroup ? (
                  <div className="e011ReviewWorkspace clientWorkspace">
                    <div className="e011WorkspaceHeader">
                      <div>
                        <div className="miniTitle">{activeClientEvidenceGroup.brandLabel}</div>
                        <div className="contextHint">{activeClientEvidenceGroup.total} evidencia(s). Se muestran miniaturas compactas; doble clic abre la foto grande.</div>
                      </div>
                    </div>
                    <div className="e011ThumbStrip">
                      {activeClientEvidenceGroup.items.slice(0, E011_THUMBS_PER_GROUP).map((item) => (
                        <button key={item.evidencia_id} type="button" className="e011ThumbCard" onClick={() => handleImageTap(item.url_foto)} onDoubleClick={() => openImageViewer(item.url_foto)}>
                          <img src={item.url_foto} alt={item.tipo_evidencia || item.tipo_evento} />
                          <div className="e011ThumbInfo">
                            <strong>{item.tipo_evidencia || item.tipo_evento}</strong>
                            <span>{getStoreDisplayFromItem(item) || item.tienda_nombre || "Tienda"}</span>
                            <span>{item.fecha_hora_fmt}</span>
                          </div>
                        </button>
                      ))}
                    </div>
                    {activeClientEvidenceGroup.items.length > E011_THUMBS_PER_GROUP ? <div className="contextHint">Hay {activeClientEvidenceGroup.items.length - E011_THUMBS_PER_GROUP} evidencia(s) más en este grupo; filtra por tienda, tipo o riesgo para acotar el análisis.</div> : null}
                  </div>
                ) : null}
              </>
            ) : null}
          </div>
        ) : null}

        {role === "cliente" && clientModule === "incidencias" ? (
          <div className="card">
            <div className="sectionTitle">Incidencias</div>
            <div className="filtersRow twoColsFilters">
              <select className="inputLike" value={clientFilters.severidad} onChange={(e) => setClientFilters((prev) => ({ ...prev, severidad: e.target.value }))}>
                <option value="">Todas las severidades</option>
                {clientFilterOptions.severidades.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
              <select className="inputLike" value={clientFilters.status} onChange={(e) => setClientFilters((prev) => ({ ...prev, status: e.target.value }))}>
                <option value="">Todos los estatus</option>
                {clientFilterOptions.estatus_alerta.map((opt) => <option key={opt.id} value={opt.id}>{opt.label}</option>)}
              </select>
            </div>
            <div className="stack compactStack" style={{ marginTop: 12 }}>
              {clientIncidents.map((item) => (
                <div className="listBtn" key={item.alerta_id}>
                  <div className="listTitle">{item.tienda_nombre || item.tienda_id || "Tienda"}</div>
                  <div className="listSub">{item.tipo_alerta} · {item.fecha_hora_fmt}</div>
                  <div className="summaryLine">{item.descripcion}</div>
                  <div className="geoRow">
                    <span className={`riskBadge ${severityClass(item.severidad)}`}>{item.severidad}</span>
                    <span className={`riskBadge ${statusClass(item.status)}`}>{item.status}</span>
                  </div>
                </div>
              ))}
              {!clientIncidents.length ? <div className="emptyBox">Sin incidencias para los filtros seleccionados.</div> : null}
            </div>
          </div>
        ) : null}

        {role === "cliente" && clientModule === "entregables" ? (
          <div className="card">
            <div className="sectionTitle">Entregables</div>
            <div className="panel">
              <div className="summaryLine">{clientDeliverablesMessage || "Los entregables automáticos estarán disponibles en la siguiente fase."}</div>
            </div>
          </div>
        ) : null}

        {role === "promotor" && promotorModule === "asistencia" ? (
          <div className="card">
            <div className="sectionTitle">Asistencia</div>
            <div className="twoCol">
              <div className="panel">
                <label className="fieldLabel">Tienda</label>
                <select className="inputLike" value={selectedStoreId} onChange={(e) => setSelectedStoreId(e.target.value)}>
                  <option value="">Selecciona una tienda</option>
                  {stores.map((store) => (
                    <option key={store.tienda_id} value={store.tienda_id}>{formatStoreDisplay(store.tienda_id, store.nombre_tienda)}</option>
                  ))}
                </select>
                {!selectedStoreId ? <div className="contextHint e020CaptureGuard e020cCaptureGuardStrong">⚠️ Primero selecciona una tienda. Después podrás capturar ubicación y tomar selfie.</div> : null}
                {captureGuardMsg && promotorModule === "asistencia" ? <div className="e020cGuardToast">{captureGuardMsg}</div> : null}

                <div className="captureBlock">
                  <div className="captureTitle">Entrada</div>
                  <div className="captureStack">
                    <button className="secondaryBtn compactBtn assistQuickBtn" onClick={() => void captureLocation("entrada")} disabled={capturingLocation === "entrada"}>
                      <MapPin size={16} />
                      {capturingLocation === "entrada" ? "Ubicando..." : entryLocation ? "Ubicación lista" : "Capturar ubicación"}
                    </button>
                    <button className="secondaryBtn compactBtn assistQuickBtn" onClick={() => void openCamera("entrada", "user")} disabled={false}>
                      <Camera size={16} />
                      {entryPhoto ? "Selfie lista" : "Tomar selfie"}
                    </button>
                    {attendanceGalleryAuth.allowed ? (
                      <button className="secondaryBtn compactBtn assistQuickBtn" onClick={() => entryGalleryInputRef.current?.click()} disabled={!selectedStoreId}>
                        <ImageIcon size={16} />
                        Galería autorizada
                      </button>
                    ) : null}
                  </div>
                  <input ref={entryNativeCameraInputRef} type="file" accept="image/*" capture="user" style={{ display: "none" }} onChange={(e) => void handleNativeCameraSelection("entrada", e.target.files)} />
                  <input ref={entryGalleryInputRef} type="file" accept="image/*" style={{ display: "none" }} onChange={(e) => void handleGallerySelection("attendance-entry", e.target.files)} />
                  <div className="authTraceBox">Galería asistencia: <strong>{galleryReasonLabel(attendanceGalleryAuth)}</strong></div>
                  {entryLocation ? <div className="captureMeta">Lat {entryLocation.lat.toFixed(5)} · Lon {entryLocation.lon.toFixed(5)}</div> : null}
                  {entryPhoto ? <div className="thumbRow"><img src={entryPhoto.dataUrl} className="thumb" alt="Entrada" /></div> : null}
                </div>

                <button className="primaryBtn mainActionBtn entryActionBtn" onClick={() => void createEntry()} disabled={syncing}>
                  <span className="mainActionTop"><MapPin size={16} /><span>{syncing ? "Procesando..." : "Registrar entrada"}</span></span>
                </button>

                {hasOpenVisit && exitVisit ? (
                  <button className="secondaryBtn dangerBtn mainActionBtn exitActionBtn" onClick={() => void closeVisit()} disabled={syncing || !hasOpenVisit}>
                    <span className="mainActionTop"><CheckCircle2 size={16} /><span>{syncing ? "Procesando..." : "Registrar salida"}</span></span>
                    {!syncing && exitVisit ? <span className="mainActionSub">{getVisitDisplayName(exitVisit, stores)}</span> : null}
                  </button>
                ) : null}
              </div>

              <div className="panel">
                <div className="miniTitle">Visitas de hoy</div>
                <div className="stack compactStack">
                  {pendingVisits.map((visit) => {
                    const isOpen = !visit.hora_fin;
                    return (
                      <button key={visit.visita_id} onClick={() => { if (isOpen) setSelectedVisitId(visit.visita_id); focusAttendanceForVisit(visit.visita_id); }} className={`listBtn ${isOpen && selectedVisitId === visit.visita_id ? "listBtnGreen" : ""}`}>
                        <div className="listTitle">{getVisitDisplayName(visit, stores)}</div>
                        <div className="listSub">Entrada: {formatHourFromIso(visit.hora_inicio)} · {isOpen ? "Salida pendiente" : `Salida: ${formatHourFromIso(visit.hora_fin)}`}</div>
                        <div className="geoRow">
                          <span className={`geoBadge ${geofenceClass(visit.resultado_geocerca_entrada)}`}>E: {geofenceShortLabel(visit.resultado_geocerca_entrada)}</span>
                          {!isOpen ? <span className={`geoBadge ${geofenceClass(visit.resultado_geocerca_salida)}`}>S: {geofenceShortLabel(visit.resultado_geocerca_salida)}</span> : null}
                        </div>
                      </button>
                    );
                  })}
                  {!pendingVisits.length ? <div className="emptyBox">No hay visitas registradas hoy.</div> : null}
                </div>

                {attendanceGallery.length ? (
                  <div className="attendanceGalleryBlock">
                    <div className="miniTitle" style={{ marginTop: 12 }}>Fotos de asistencia</div>
                    <div className="galleryScroll compactGalleryScroll">
                      <div className="galleryGrid attendanceGalleryGrid">
                        {attendanceGallery.map((item) => (
                          <div className="galleryCard" key={item.evidencia_id} ref={(el) => { attendancePhotoRefs.current[item.evidencia_id] = el; }}>
                            <div className="imageFrame"><img src={item.url_foto} alt={item.tipo_evento} className="img" onDoubleClick={() => openImageViewer(item.url_foto)} onClick={(e) => { e.stopPropagation(); handleImageTap(item.url_foto); }} /></div>
                            <div className="galleryTop">
                              <div className="galleryTitle">{item.tipo_evento === "ASISTENCIA_ENTRADA" ? "Entrada" : "Salida"}</div>
                              <span className={`riskBadge ${severityClass(item.riesgo)}`}>{item.riesgo || "BAJO"}</span>
                            </div>
                            {getStoreDisplayFromItem(item) ? <div className="gallerySub compactMeta">{getStoreDisplayFromItem(item)}</div> : null}
                            <div className="galleryDate">{item.fecha_hora_fmt}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        ) : null}

        {role === "promotor" && promotorModule === "evidencias" ? (
          <div className="card">
            <div className="sectionTitle">Evidencias</div>
            <div className="panel e024EvidenceLaunchPanel">
              <label className="fieldLabel">Visita / tienda</label>
              <select className="inputLike" value={selectedVisitId} onChange={(e) => setSelectedVisitId(e.target.value)}>
                <option value="">Selecciona una visita</option>
                {openVisits.map((visit) => (
                  <option key={visit.visita_id} value={visit.visita_id}>{getVisitDisplayName(visit, stores)}</option>
                ))}
              </select>
              {selectedVisitStoreName ? <div className="e024StoreHero">
                <Store size={18} />
                <div>
                  <div className="e024StoreLabel">Tienda activa</div>
                  <div className="e024StoreName">{selectedVisitStoreName}</div>
                </div>
              </div> : null}
              {!selectedVisitId ? <div className="contextHint e020CaptureGuard e020cCaptureGuardStrong">⚠️ Primero selecciona una visita/tienda activa. Después podrás abrir la cámara de evidencias.</div> : null}
              {selectedVisitHasNoBrands ? (
                <div className="emptyBox e014NoBrandBox">Esta tienda está en rutero, pero no tiene marcas activas para capturar. Puedes registrar asistencia y cerrar visita sin evidencias obligatorias.</div>
              ) : null}
              {captureGuardMsg && promotorModule === "evidencias" ? <div className="e020cGuardToast">{captureGuardMsg}</div> : null}
              <button className="primaryBtn mainActionBtn e024OpenCameraBtn" disabled={syncing || !selectedVisitId || selectedVisitHasNoBrands} onClick={() => void startExternalCameraForEvidence()}>
                <span className="mainActionTop e014fEvidenceActionTop"><Camera size={18} /><span>{syncing ? "Abriendo..." : "Abrir cámara de evidencias"}</span></span>
              </button>
              <div className="e024EvidenceSummary">
                <div><strong>{currentVisitEvidenceGallery.length}</strong><span>fotos activas</span></div>
                <div><strong>{Array.from(new Set(currentVisitEvidenceGallery.map((item) => normalizeBrandLabel(item.marca_nombre || "", item.marca_id || "Marca")).filter(Boolean))).length}</strong><span>marcas</span></div>
                <div><strong>{Array.from(new Set(currentVisitEvidenceGallery.map((item) => item.tipo_evidencia || "").filter(Boolean))).length}</strong><span>tipos</span></div>
              </div>
            </div>

            <div className="panel e024ResultGalleryPanel">
              <div className="e018SectionHeader">
                <div>
                  <div className="sectionTitle" style={{ margin: 0 }}>Galería de la visita</div>
                  <div className="contextHint">Las fotos capturadas desde el módulo de cámara aparecen aquí al regresar.</div>
                </div>
                <button type="button" className="secondaryBtn compactBtn e018TopRefreshBtn" onClick={() => void refreshCurrentRoleData()} disabled={syncing || !!error}>
                  <RefreshCw size={15} /><span>{syncing ? "Sincronizando..." : "Actualizar"}</span>
                </button>
              </div>
              {currentVisitEvidenceGallery.length ? (
                <div className="e024VisitGalleryRail">
                  {currentVisitEvidenceGallery.slice(0, 18).map((item) => (
                    <div className="e024VisitGalleryCard" key={item.evidencia_id} onClick={() => { setPromotorModule("mis_evidencias"); setPromotorEvidenceViewFilter("fotos"); setSelectedEvidenceId(item.evidencia_id); }}>
                      <img src={item.url_foto} alt={item.tipo_evidencia || "Evidencia"} />
                      <div className="e024VisitGalleryMeta">
                        <strong>{normalizeBrandLabel(item.marca_nombre || "", item.marca_id || "Marca")}</strong>
                        <span>{item.tipo_evidencia || "Evidencia"}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="emptyBox">Aún no hay fotos registradas para esta visita.</div>
              )}
            </div>
          </div>
        ) : null}

        {role === "promotor" && promotorModule === "mis_evidencias" ? (
          <div className="card">
            <div className="e018SectionHeader">
              <div className="sectionTitle">Mis evidencias</div>
              <button type="button" className="secondaryBtn compactBtn e018TopRefreshBtn" onClick={() => void refreshCurrentRoleData()} disabled={syncing || !!error}><RefreshCw size={15} /><span>{syncing ? "Sincronizando..." : "Recargar"}</span></button>
            </div>
            <div className="e018ReviewModeBar e019PromotorModeBar">
              <button type="button" className={`e016DateChip ${promotorEvidenceViewFilter === "fotos" ? "e016DateChipActive" : ""}`} onClick={() => { setPromotorEvidenceViewFilter("fotos"); setSelectedPromotorOutOfServiceId(""); }}>Con foto</button>
              <button type="button" className={`e016DateChip ${promotorEvidenceViewFilter === "fuera" ? "e016DateChipActive" : ""}`} onClick={() => { setPromotorEvidenceViewFilter("fuera"); setSelectedEvidenceId(""); }}>Sin servicio</button>
              <button type="button" className={`e016DateChip ${promotorEvidenceViewFilter === "todo" ? "e016DateChipActive" : ""}`} onClick={() => { setPromotorEvidenceViewFilter("todo"); setSelectedEvidenceId(""); setSelectedPromotorOutOfServiceId(""); }}>Todo</button>
            </div>
            <div className="filtersRow">
              <select className="inputLike" value={evidenceFilterStore} onChange={(e) => { setEvidenceFilterStore(e.target.value); setEvidenceFilterBrand(""); setEvidenceFilterType(""); setEvidenceFilterPhase(""); }}>
                <option value="">Todas las tiendas</option>
                {evidenceFilterOptions.stores.map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
              <select className="inputLike" value={evidenceFilterBrand} onChange={(e) => { setEvidenceFilterBrand(e.target.value); setEvidenceFilterType(""); setEvidenceFilterPhase(""); }}>
                <option value="">Todas las marcas</option>
                {evidenceFilterOptions.brands.map((value) => <option key={value} value={value}>{normalizeBrandLabel(value, "Marca")}</option>)}
              </select>
              <select className="inputLike" value={evidenceFilterType} onChange={(e) => { setEvidenceFilterType(e.target.value); setEvidenceFilterPhase(""); }}>
                <option value="">Todos los tipos</option>
                {evidenceFilterOptions.types.map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
              <select className="inputLike" value={evidenceFilterPhase} onChange={(e) => setEvidenceFilterPhase(e.target.value)}>
                <option value="">Todas las fases</option>
                {evidenceFilterOptions.phases.map((value) => <option key={value} value={value}>{formatPhaseLabel(value)}</option>)}
              </select>
            </div>
            <div className="twoCol">
              <div className="panel">
                <div className="miniTitle">Listado</div>
                <div className="stack compactStack">
                  <div ref={promotorListTopRef} className="e018ScrollAnchor" />
                  {promotorEvidenceViewFilter !== "fuera" ? filteredOperationalGallery.map((item) => (
                    <button key={item.evidencia_id} onClick={() => focusPromotorEvidence(item)} className={`listBtn e024iEvidenceListBtn ${selectedEvidenceId === item.evidencia_id ? "listBtnGreen" : ""}`}>
                      <div className="e024iDataRows">
                        <div><span>Tienda</span><strong>{getStoreDisplayFromItem(item) || "Visita activa"}</strong></div>
                        <div><span>Marca</span><strong>{normalizeBrandLabel(item.marca_nombre, "Marca")}</strong></div>
                        <div><span>Tipo</span><strong>{item.tipo_evidencia || item.tipo_evento || "Evidencia"}</strong></div>
                        <div><span>Fase</span><strong>{formatPhaseLabel(item.fase || "ESTADO_ACTUAL")}</strong></div>
                        <div><span>Fecha</span><strong>{item.fecha_hora_fmt || formatDateTimeMaybe(item.fecha_hora || "")}</strong></div>
                      </div>
                    </button>
                  )) : null}
                  {promotorEvidenceViewFilter !== "fotos" ? promotorOutOfServiceVisibleRows.map((item) => (
                    <button key={item.registro_id} type="button" onClick={() => focusPromotorOutOfService(item)} className={`listBtn e019OutServiceListItem e024iEvidenceListBtn ${selectedPromotorOutOfServiceId === item.registro_id ? "listBtnGreen" : ""}`}>
                      <div className="e024iDataRows">
                        <div><span>Tienda</span><strong>{item.tienda_display || item.tienda_nombre || item.tienda_id || "Tienda"}</strong></div>
                        <div><span>Marca</span><strong>{item.marca_nombre || item.marca_id || "Marca"}</strong></div>
                        <div><span>Registro</span><strong>Sin servicio</strong></div>
                        <div><span>Motivo</span><strong>{item.motivo || "Sin motivo"}</strong></div>
                        <div><span>Fecha</span><strong>{item.fecha_hora_fmt || item.fecha_hora || "Sin fecha"}</strong></div>
                      </div>
                      {item.comentario ? <div className="e024iCommentLine">Comentario: {item.comentario}</div> : null}
                    </button>
                  )) : null}
                  {promotorEvidenceViewFilter !== "fuera" && !filteredOperationalGallery.length ? <div className="emptyBox">No hay evidencias con esos filtros.</div> : null}
                  {promotorEvidenceViewFilter !== "fotos" && !promotorOutOfServiceVisibleRows.length ? <div className="emptyBox">No hay marcas sin servicio registradas con esos filtros.</div> : null}
                  <div ref={promotorListBottomRef} className="e018ScrollAnchor" />
                </div>
                {(filteredOperationalGallery.length + promotorOutOfServiceVisibleRows.length) > 12 ? (
                  <div className="e018MiniNavRow">
                    <button type="button" onClick={() => scrollElementIntoView(promotorListTopRef, "start")}>↑ Inicio</button>
                    <button type="button" onClick={() => scrollElementIntoView(promotorListBottomRef, "end")}>↓ Final</button>
                  </div>
                ) : null}
              </div>
              <div className="panel" ref={promotorDetailRef}>
                <div className="miniTitle">Detalle útil</div>
                {selectedEvidence ? (
                  <>
                    <div className="e024iDetailEvidenceCard" onDoubleClick={() => openImageViewer(selectedEvidence.url_foto)} onClick={() => handleImageTap(selectedEvidence.url_foto)}>
                      <div className="imageFrame e024iDetailImageFrame"><img src={selectedEvidence.url_foto} alt={selectedEvidence.tipo_evidencia} className="img" /></div>
                      <div className="e024iDetailBody">
                        <div className="e024iDataRows">
                          <div><span>Tienda</span><strong>{getStoreDisplayFromItem(selectedEvidence) || "Visita activa"}</strong></div>
                          <div><span>Marca</span><strong>{normalizeBrandLabel(selectedEvidence.marca_nombre, "Marca")}</strong></div>
                          <div><span>Tipo</span><strong>{selectedEvidence.tipo_evidencia || selectedEvidence.tipo_evento || "Evidencia"}</strong></div>
                          <div><span>Fase</span><strong>{formatPhaseLabel(selectedEvidence.fase || "ESTADO_ACTUAL")}</strong></div>
                          <div><span>Fecha</span><strong>{selectedEvidence.fecha_hora_fmt || formatDateTimeMaybe(selectedEvidence.fecha_hora || "")}</strong></div>
                          <div><span>Riesgo</span><strong>{selectedEvidence.riesgo || "BAJO"}</strong></div>
                        </div>
                      </div>
                    </div>
                    <div className="actionGrid actionGridButtons">
                      <button className="actionButton" onClick={() => openImageViewer(selectedEvidence.url_foto)}><Eye size={16} /><span>Ver foto</span></button>
                      <button className="actionButton" onClick={() => void markEvidenceAsCancelled()}><Trash2 size={16} /><span>Anular</span></button>
                      <button className="actionButton" onClick={() => void openCamera("reemplazo", "environment")}><Camera size={16} /><span>Reemplazar cámara</span></button>
                      {replaceGalleryAuth.allowed ? <button className="actionButton" onClick={() => replaceGalleryInputRef.current?.click()}><ImageIcon size={16} /><span>Galería autorizada</span></button> : null}
                      <button className="actionButton" onClick={() => void saveNote()}><Pencil size={16} /><span>Guardar nota</span></button>
                    </div>
                    <input ref={replaceNativeCameraInputRef} type="file" accept="image/*" capture="environment" style={{ display: "none" }} onChange={(e) => void handleNativeCameraSelection("reemplazo", e.target.files)} />
                    <input ref={replaceGalleryInputRef} type="file" accept="image/*" style={{ display: "none" }} onChange={(e) => void handleGallerySelection("replace", e.target.files)} />
                    <div className="authTraceBox">Galería reemplazo: <strong>{galleryReasonLabel(replaceGalleryAuth)}</strong></div>
                    <label className="fieldLabel" style={{ marginTop: 10 }}>Nota</label>
                    <input className="inputLike" value={noteDraft} onChange={(e) => setNoteDraft(e.target.value)} placeholder="Escribe un comentario" />
                  </>
                ) : selectedPromotorOutOfService ? (
                  <div className="outOfServiceDetailCard">
                    <div className="outOfServiceTitle">¿Marca fuera de servicio?</div>
                    <div className="summaryLine">Tienda: <strong>{selectedPromotorOutOfService.tienda_display || selectedPromotorOutOfService.tienda_nombre || selectedPromotorOutOfService.tienda_id || "Tienda"}</strong></div>
                    <div className="summaryLine">Marca: <strong>{selectedPromotorOutOfService.marca_nombre || selectedPromotorOutOfService.marca_id || "Marca"}</strong></div>
                    <div className="summaryLine">Motivo: <strong>{selectedPromotorOutOfService.motivo || "Sin motivo"}</strong></div>
                    {selectedPromotorOutOfService.comentario ? <div className="summaryLine">Comentario: {selectedPromotorOutOfService.comentario}</div> : null}
                    <div className="summaryLine">Fecha: {selectedPromotorOutOfService.fecha_hora_fmt || selectedPromotorOutOfService.fecha_hora || "Sin fecha"}</div>
                    <div className="authTraceBox">Este registro no es una foto; es una justificación operativa de marca sin servicio.</div>
                  </div>
                ) : (
                  <div className="emptyBox">Selecciona una evidencia o un registro sin servicio.</div>
                )}
              </div>
            </div>
          </div>
        ) : null}

        {role === "promotor" && promotorModule === "resumen" ? (
          <div className="card e014cSummaryCard">
            <div className="e014cSummaryHeader">
              <div>
                <div className="sectionTitle e014cSummaryTitle">Resumen del día</div>
                <div className="e014cSummarySub">Lectura rápida de avance, evidencias y pendientes.</div>
              </div>
              <span className={`e014cStatusPill ${openVisits.length ? "e014cPillActive" : "e014cPillNeutral"}`}>{openVisits.length ? "En operación" : "Sin visita abierta"}</span>
            </div>

            <div className="e014cHeroMetric">
              <div className="e014cHeroCopy">
                <span className="e014cEyebrow">Rutero</span>
                <strong>{openVisits.length ? `${openVisits.length} visita${openVisits.length === 1 ? "" : "s"} abierta${openVisits.length === 1 ? "" : "s"}` : "Listo para iniciar"}</strong>
                <small>{stores.length ? `${stores.length} tienda${stores.length === 1 ? "" : "s"} asignada${stores.length === 1 ? "" : "s"}` : "Sin tiendas asignadas"}</small>
              </div>
              <div className="e014cHeroNumbers">
                <span>{operationalGallery.length}</span>
                <small>evidencias hoy</small>
              </div>
            </div>

            <div className="e014cMetricGrid">
              <div className="e014cMetricCard">
                <div className="e014cMetricIcon"><Store size={16} /></div>
                <div className="e014cMetricBody"><span>Tiendas asignadas</span><strong>{stores.length}</strong></div>
              </div>
              <div className="e014cMetricCard">
                <div className="e014cMetricIcon"><ClipboardList size={16} /></div>
                <div className="e014cMetricBody"><span>Visitas abiertas</span><strong>{openVisits.length}</strong></div>
              </div>
              <div className="e014cMetricCard">
                <div className="e014cMetricIcon"><ImageIcon size={16} /></div>
                <div className="e014cMetricBody"><span>Fotos hoy</span><strong>{promotorUsage.today?.fotos || 0}</strong></div>
              </div>
              <div className="e014cMetricCard">
                <div className="e014cMetricIcon"><AlertTriangle size={16} /></div>
                <div className="e014cMetricBody"><span>Alertas</span><strong>{operationalGallery.filter((g) => g.riesgo === "ALTO" || g.riesgo === "MEDIO").length}</strong></div>
              </div>
            </div>

            <div className="e014cSummaryGrid">
              <div className="e014cPanel e014cPanelWide">
                <div className="e014cPanelHead">
                  <div>
                    <span className="e014cEyebrow">Actividad</span>
                    <strong>Registros de visitas</strong>
                  </div>
                  <span className="e014cCountBadge">{pendingVisits.length}</span>
                </div>
                <div className="e014cTimeline">
                  {pendingVisits.length ? pendingVisits.map((visit) => (
                    <div className="e014cTimelineItem" key={visit.visita_id}>
                      <div className="e014cDot"><CheckCircle2 size={13} /></div>
                      <div>
                        <strong>{getVisitDisplayName(visit, stores)}</strong>
                        <span>Entrada {formatHourFromIso(visit.hora_inicio)}{visit.hora_fin ? ` · Salida ${formatHourFromIso(visit.hora_fin)}` : " · Sin salida"}</span>
                        <small>E: {geofenceShortLabel(visit.resultado_geocerca_entrada)}{visit.hora_fin ? ` · S: ${geofenceShortLabel(visit.resultado_geocerca_salida)}` : ""}</small>
                      </div>
                    </div>
                  )) : <div className="e014cEmptyLine">No hay registros del día.</div>}
                </div>
              </div>

              <div className="e014cPanel">
                <div className="e014cPanelHead">
                  <div>
                    <span className="e014cEyebrow">Envíos</span>
                    <strong>Pendientes</strong>
                  </div>
                  <span className={`e014cCountBadge ${pendingQueue.length ? "e014cBadgeWarn" : ""}`}>{pendingQueue.length}</span>
                </div>
                <div className="e014cKeyRows">
                  <div><span>Errores</span><strong>{pendingQueue.filter((item) => item.status === "ERROR_ENVIO").length}</strong></div>
                  <div><span>Conexión</span><strong>Auto-reintento</strong></div>
                </div>
                <button className="secondaryBtn e014cRetryBtn" onClick={() => void syncPendingQueue(true)} disabled={syncingPendingQueue || !pendingQueue.length}>
                  <RefreshCw size={16} className={syncingPendingQueue ? "spin" : ""} />
                  {syncingPendingQueue ? "Reintentando..." : "Reintentar envíos"}
                </button>
                {pendingQueue.length ? (
                  <div className="e014cCompactList">
                    {pendingQueue.map((item) => (
                      <div className="e014cListItem" key={item.id}>
                        <strong>{formatPendingQueueLabel(item)}</strong>
                        <span>{formatDateTimeMaybe(item.createdAt)} · {item.status === "ERROR_ENVIO" ? "Error" : "Pendiente"}</span>
                        {item.lastError ? <small>{item.lastError}</small> : null}
                      </div>
                    ))}
                  </div>
                ) : <div className="e014cEmptyLine">Sin pendientes por enviar.</div>}
              </div>

              <div className="e014cPanel">
                <div className="e014cPanelHead">
                  <div>
                    <span className="e014cEyebrow">Uso</span>
                    <strong>Consumo estimado</strong>
                  </div>
                  <span className="e014cCountBadge">{promotorUsage.reference?.reference_pct || 0}%</span>
                </div>
                <div className="e014cKeyRows">
                  <div><span>MB hoy</span><strong>{promotorUsage.today?.mb?.toFixed ? promotorUsage.today.mb.toFixed(2) : (promotorUsage.today?.mb || 0)}</strong></div>
                  <div><span>MB mes</span><strong>{promotorUsage.month?.mb?.toFixed ? promotorUsage.month.mb.toFixed(2) : (promotorUsage.month?.mb || 0)}</strong></div>
                  <div><span>GB mes</span><strong>{promotorUsage.month?.gb?.toFixed ? promotorUsage.month.gb.toFixed(3) : (promotorUsage.month?.gb || 0)}</strong></div>
                  <div><span>Ref. bolsa $200</span><strong>~$ {promotorUsage.reference?.estimated_mxn || 0}</strong></div>
                </div>
                <div className="e014cNote">{promotorUsage.reference?.note || "Estimado de uso de la mini app. No es saldo real del operador."}</div>
              </div>

              <div className="e014cPanel">
                <div className="e014cPanelHead">
                  <div>
                    <span className="e014cEyebrow">Riesgo</span>
                    <strong>Alertas recientes</strong>
                  </div>
                  <span className={`e014cCountBadge ${promotorRecentAlerts.length ? "e014cBadgeWarn" : ""}`}>{promotorRecentAlerts.length}</span>
                </div>
                <div className="e014cCompactList">
                  {promotorRecentAlerts.length ? promotorRecentAlerts.map((item) => (
                    <div className="e014cListItem" key={item.alerta_id}>
                      <strong>{item.tipo_alerta}</strong>
                      <span>{item.tienda_nombre || item.tienda_id || "Tienda"}</span>
                      <small>{item.fecha_hora_fmt || ""} · <span className={`riskBadge ${statusClass(item.status)}`}>{item.status}</span>{item.resolved_classification ? ` · ${item.resolved_classification}` : ""}</small>
                    </div>
                  )) : <div className="e014cEmptyLine">Sin alertas recientes.</div>}
                </div>
              </div>
            </div>
          </div>
        ) : null}

        {role === "supervisor" && supervisorModule === "resumen" ? (
          <div className="card e016SupervisorSummaryCard">
            <div className="e014cSummaryHeader">
              <div>
                <div className="sectionTitle e014cSummaryTitle">Resumen supervisor</div>
                <div className="e014cSummarySub">Lectura ejecutiva de operación, evidencias y pendientes.</div>
              </div>
              <span className={`e014cStatusPill ${supervisorPendingClose.open_visits ? "e014cPillActive" : "e014cPillNeutral"}`}>{supervisorPendingClose.open_visits ? "Con visitas abiertas" : "Cierre al día"}</span>
            </div>
            <div className="e014cHeroMetric e016SupervisorHero">
              <div className="e014cHeroCopy">
                <span className="e014cEyebrow">Operación del día</span>
                <strong>{supervisorSummary.visitasHoy} visita{supervisorSummary.visitasHoy === 1 ? "" : "s"}</strong>
                <small>{supervisorSummary.promotores} promotor{supervisorSummary.promotores === 1 ? "" : "es"} · {supervisorSummary.evidenciasHoy} evidencias</small>
              </div>
              <div className="e014cHeroNumbers">
                <span>{supervisorPendingClose.pending_reviews || 0}</span>
                <small>por revisar</small>
              </div>
            </div>
            <div className="e014cMetricGrid e016SupervisorMetricGrid">
              <div className="e014cMetricCard"><div className="e014cMetricIcon"><Users size={16} /></div><div className="e014cMetricBody"><span>Promotores</span><strong>{supervisorSummary.promotores}</strong></div></div>
              <div className="e014cMetricCard"><div className="e014cMetricIcon"><ClipboardList size={16} /></div><div className="e014cMetricBody"><span>Visitas hoy</span><strong>{supervisorSummary.visitasHoy}</strong></div></div>
              <div className="e014cMetricCard"><div className="e014cMetricIcon"><Store size={16} /></div><div className="e014cMetricBody"><span>Abiertas</span><strong>{supervisorSummary.abiertas}</strong></div></div>
              <div className="e014cMetricCard"><div className="e014cMetricIcon"><ShieldAlert size={16} /></div><div className="e014cMetricBody"><span>Alertas</span><strong>{supervisorSummary.alertas}</strong></div></div>
            </div>
            <div className="e016InfoGrid">
              <div className="e016InfoPanel">
                <div className="e016PanelTitle">Consumo aproximado</div>
                <div className="e016InfoLine"><span>Fotos hoy</span><strong>{supervisorUsage.today?.fotos || 0}</strong></div>
                <div className="e016InfoLine"><span>MB hoy</span><strong>{supervisorUsage.today?.mb?.toFixed ? supervisorUsage.today.mb.toFixed(2) : (supervisorUsage.today?.mb || 0)}</strong></div>
                <div className="e016InfoLine"><span>MB mes</span><strong>{supervisorUsage.month?.mb?.toFixed ? supervisorUsage.month.mb.toFixed(2) : (supervisorUsage.month?.mb || 0)}</strong></div>
              </div>
              <div className="e016InfoPanel">
                <div className="e016PanelTitle">Pendientes de cierre</div>
                <div className="e016InfoLine"><span>Visitas abiertas</span><strong>{supervisorPendingClose.open_visits || 0}</strong></div>
                <div className="e016InfoLine"><span>Alertas abiertas</span><strong>{supervisorPendingClose.open_alerts || 0}</strong></div>
                <div className="e016InfoLine"><span>Revisiones pendientes</span><strong>{supervisorPendingClose.pending_reviews || 0}</strong></div>
              </div>
            </div>
          </div>
        ) : null}

        {role === "supervisor" && supervisorModule === "equipo" ? (
          <div className="card">
            <div className="sectionTitle">Equipo</div>
            <div className="twoCol">
              <div className="panel">
                <div className="miniTitle">Promotores</div>
                <div className="stack compactStack">
                  {supervisorTeam.map((item) => (
                    <button
                      key={item.promotor_id}
                      onClick={() => {
                        setSelectedTeamPromotorId(item.promotor_id);
                        setSelectedRouteVisitId("");
                        setExpedient(null);
                      }}
                      className={`listBtn ${selectedTeamPromotorId === item.promotor_id ? "listBtnGreen" : ""}`}
                    >
                      <div className="listTitle">{item.nombre}</div>
                      <div className="listSub">Visitas: {item.visitas_hoy} · Abiertas: {item.visitas_abiertas} · Alertas: {item.alertas_abiertas}</div>
                      <div className="geoRow"><span className={`riskBadge ${statusClass(item.status_general)}`}>{item.status_general}</span></div>
                    </button>
                  ))}
                  {!supervisorTeam.length ? <div className="emptyBox">No hay promotores ligados a este supervisor.</div> : null}
                </div>
              </div>
              <div className="panel">
                <div className="miniTitle">Detalle</div>
                {selectedTeamMember ? (
                  <>
                    <div className="e016DetailHero">
                      <div>
                        <div className="e016DetailEyebrow">Promotor</div>
                        <div className="e016DetailTitle">{selectedTeamMember.nombre}</div>
                        <div className="e016DetailSub">Región: {selectedTeamMember.region || "-"}</div>
                      </div>
                      <span className={`riskBadge ${statusClass(selectedTeamMember.status_general)}`}>{selectedTeamMember.status_general}</span>
                    </div>
                    <div className="e016MiniMetricGrid">
                      <div><span>Visitas</span><strong>{selectedTeamMember.visitas_hoy}</strong></div>
                      <div><span>Abiertas</span><strong>{selectedTeamMember.visitas_abiertas}</strong></div>
                      <div><span>Evidencias</span><strong>{selectedTeamMember.evidencias_hoy}</strong></div>
                      <div><span>Alertas</span><strong>{selectedTeamMember.alertas_abiertas}</strong></div>
                    </div>
                    <div className="e016InfoPanel e016ActivityPanel">
                      <div className="e016PanelTitle">Última actividad</div>
                      <div className="e016InfoLine"><span>Tienda</span><strong>{selectedTeamMember.ultima_tienda_display || selectedTeamMember.ultima_tienda || "-"}</strong></div>
                      <div className="e016InfoLine"><span>Entrada</span><strong>{formatHourFromIso(selectedTeamMember.ultima_entrada)}</strong></div>
                      <div className="e016InfoLine"><span>Salida</span><strong>{selectedTeamMember.ultima_salida ? formatHourFromIso(selectedTeamMember.ultima_salida) : "Pendiente"}</strong></div>
                    </div>
                    <div className="actionGrid actionGridButtons e016ActionGrid">
                      <button className="actionButton" onClick={() => { setSupEvidencePromotorFilter(selectedTeamMember.promotor_id); setSupervisorModule("evidencias"); }}><ImageIcon size={16} /><span>Ver evidencias</span></button>
                      <button className="actionButton" onClick={() => { setAlertPromotorFilter(selectedTeamMember.promotor_id); setSupervisorModule("alertas"); }}><ShieldAlert size={16} /><span>Ver alertas</span></button>
                      <button className="actionButton" onClick={() => setStatusMsg(supervisorDayRoute.length ? "Selecciona una visita del día abajo." : "Este promotor no tiene visitas del día.")}><Eye size={16} /><span>Ver visitas</span></button>
                    </div>
                    <div className="miniTitle" style={{ marginTop: 14 }}>Visitas del día</div>
                    {dayRouteLoading ? <div className="emptyBox">Cargando visitas del día...</div> : null}
                    {!dayRouteLoading ? (
                      <div className="stack compactStack" style={{ marginTop: 8 }}>
                        {supervisorDayRoute.map((row) => (
                          <button
                            key={row.visita_id}
                            className={`listBtn ${selectedRouteVisitId === row.visita_id ? "listBtnGreen" : ""}`}
                            onClick={() => {
                              setSelectedRouteVisitId(row.visita_id);
                              void openVisitExpedient(row.visita_id);
                            }}
                          >
                            <div className="listTitle">{row.tienda_nombre || row.tienda_id || "Tienda"}</div>
                            <div className="listSub">Entrada: {row.entry_fmt || formatHourFromIso(row.hora_inicio)} · {row.exit_fmt ? `Salida: ${row.exit_fmt}` : "Salida pendiente"}</div>
                            <div className="listSub">Estancia: {row.stay_minutes ? `${row.stay_minutes} min` : (row.hora_fin ? "0 min" : "En curso")} · Evidencias: {row.total_evidencias} · Alertas: {row.total_alertas}</div>
                            <div className="geoRow">
                              <span className={`geoBadge ${geofenceClass(row.geofence_entry)}`}>E: {geofenceShortLabel(row.geofence_entry)}</span>
                              <span className={`geoBadge ${geofenceClass(row.geofence_exit)}`}>S: {geofenceShortLabel(row.geofence_exit)}</span>
                            </div>
                            <div className="geoRow" style={{ marginTop: 8 }}>
                              <span className="riskBadge riskGreen">Abrir expediente</span>
                            </div>
                          </button>
                        ))}
                        {!supervisorDayRoute.length ? <div className="emptyBox">{selectedTeamMember?.visitas_hoy ? "No se pudieron cargar las visitas registradas del día." : "Este promotor no tiene visitas registradas hoy."}</div> : null}
                      </div>
                    ) : null}
                  </>
                ) : (
                  <div className="emptyBox">Selecciona un promotor.</div>
                )}
              </div>
            </div>
          </div>
        ) : null}

        {role === "supervisor" && supervisorModule === "alertas" ? (
          <div className="card">
            <div className="sectionTitle">Alertas</div>
            <div className="filtersRow">
              <select className="inputLike" value={alertPromotorFilter} onChange={(e) => setAlertPromotorFilter(e.target.value)}>
                <option value="">Todos los promotores</option>
                {supervisorPromotorOptions.map((opt) => <option key={opt.id} value={opt.id}>{opt.nombre}</option>)}
              </select>
              <select className="inputLike" value={alertStatusFilter} onChange={(e) => setAlertStatusFilter(e.target.value)}>
                <option value="">Todos los estatus</option>
                <option value="ABIERTA">ABIERTA</option>
                <option value="RESUELTA">RESUELTA</option>
                <option value="DESCARTADA">DESCARTADA</option>
              </select>
              <select className="inputLike" value={alertSeverityFilter} onChange={(e) => setAlertSeverityFilter(e.target.value)}>
                <option value="">Todas las severidades</option>
                <option value="ALTA">ALTA</option>
                <option value="MEDIA">MEDIA</option>
                <option value="BAJA">BAJA</option>
              </select>
            </div>
            <div className="twoCol">
              <div className="panel">
                <div className="miniTitle">Listado</div>
                <div className="stack compactStack">
                  {supervisorAlerts.map((item) => (
                    <button key={item.alerta_id} onClick={() => setSelectedAlertId(item.alerta_id)} className={`listBtn ${selectedAlertId === item.alerta_id ? "listBtnGreen" : ""}`}>
                      <div className="listTitle">{item.promotor_nombre || item.promotor_id}</div>
                      <div className="listSub">{item.tienda_display || item.tienda_nombre || item.tienda_id || "Tienda"} · {item.tipo_alerta}</div>
                      <div className="geoRow">
                        <span className={`riskBadge ${severityClass(item.severidad)}`}>{item.severidad}</span>
                        <span className={`riskBadge ${statusClass(item.status)}`}>{item.status}</span>
                      </div>
                    </button>
                  ))}
                  {!supervisorAlerts.length ? <div className="emptyBox">No hay alertas con esos filtros.</div> : null}
                </div>
              </div>
              <div className="panel">
                <div className="miniTitle">Detalle</div>
                {selectedAlert ? (
                  <>
                    <div className="e016DetailHero">
                      <div>
                        <div className="e016DetailEyebrow">Alerta</div>
                        <div className="e016DetailTitle">{selectedAlert.promotor_nombre || selectedAlert.promotor_id}</div>
                        <div className="e016DetailSub">{selectedAlert.tienda_display || selectedAlert.tienda_nombre || selectedAlert.tienda_id || "Tienda"}</div>
                      </div>
                      <div className="e016BadgeStack"><span className={`riskBadge ${severityClass(selectedAlert.severidad)}`}>{selectedAlert.severidad}</span><span className={`riskBadge ${statusClass(selectedAlert.status)}`}>{selectedAlert.status}</span></div>
                    </div>
                    <div className="e016InfoPanel e016ActivityPanel">
                      <div className="e016InfoLine"><span>Tipo</span><strong>{selectedAlert.tipo_alerta}</strong></div>
                      <div className="e016InfoLine"><span>Fecha</span><strong>{selectedAlert.fecha_hora_fmt}</strong></div>
                      <div className="e016InfoLine"><span>Canal</span><strong>{selectedAlert.canal_notificacion || "-"}</strong></div>
                    </div>
                    <div className="e016DescriptionBox">{selectedAlert.descripcion || "Sin descripción"}</div>
                    {(selectedAlert.photo_url || selectedAlert.url_foto) ? <div className="previewFrame" onDoubleClick={() => openImageViewer(selectedAlert.photo_url || selectedAlert.url_foto || "")} onClick={() => handleImageTap(selectedAlert.photo_url || selectedAlert.url_foto || "")}><img src={selectedAlert.photo_url || selectedAlert.url_foto} alt={selectedAlert.tipo_alerta} className="img" /></div> : null}
                    {selectedAlert.hallazgos_ai ? <div className="summaryLine">Causa detectada: {selectedAlert.hallazgos_ai}</div> : null}
                    <div className="geoRow">
                      <span className={`riskBadge ${severityClass(selectedAlert.severidad)}`}>{selectedAlert.severidad}</span>
                      <span className={`riskBadge ${statusClass(selectedAlert.status)}`}>{selectedAlert.status}</span>
                    </div>
                    {(selectedAlert.atendida_por || selectedAlert.fecha_atencion || selectedAlert.comentario_cierre || selectedAlert.origen_cierre) ? (
                      <div className="traceBox">
                        <div className="traceTitle">Trazabilidad</div>
                        {selectedAlert.atendida_por ? <div className="summaryLine">Atendida por: <strong>{selectedAlert.atendida_por}</strong></div> : null}
                        {selectedAlert.fecha_atencion ? <div className="summaryLine">Fecha atención: <strong>{formatDateTimeMaybe(selectedAlert.fecha_atencion)}</strong></div> : null}
                        {selectedAlert.origen_cierre ? <div className="summaryLine">Origen cierre: <strong>{selectedAlert.origen_cierre}</strong></div> : null}
                        {selectedAlert.comentario_cierre ? <div className="summaryLine">Comentario: {selectedAlert.comentario_cierre}</div> : null}
                      </div>
                    ) : null}
                    <div className="traceBox" style={{ marginBottom: 10 }}><div className="traceTitle">¿Qué significa?</div><div className="summaryLine"><strong>RESUELTA</strong>: la alerta sí aplicaba y ya fue atendida.</div><div className="summaryLine"><strong>DESCARTADA</strong>: la alerta no aplicaba o fue falso positivo.</div></div><label className="fieldLabel" style={{ marginTop: 10 }}>Estatus final</label>
                    <select className="inputLike" value={alertFinalStatus} onChange={(e) => setAlertFinalStatus(e.target.value as AlertFinalStatus)}>
                      <option value="RESUELTA">RESUELTA</option>
                      <option value="DESCARTADA">DESCARTADA</option>
                    </select>
                    <label className="fieldLabel" style={{ marginTop: 10 }}>Comentario de cierre</label>
                    <input className="inputLike" value={alertCloseNote} onChange={(e) => setAlertCloseNote(e.target.value)} placeholder="Validado con promotor / visita revisada" />
                    <div className="actionGrid actionGridButtons">
                      <button className="actionButton" onClick={() => void closeSelectedAlert()}><Check size={16} /><span>Cerrar alerta</span></button>
                      <button className="actionButton" onClick={() => { if (selectedAlert.visita_id) void openVisitExpedient(selectedAlert.visita_id); }}><Eye size={16} /><span>Ver visita</span></button>
                    </div>
                  </>
                ) : (
                  <div className="emptyBox">Selecciona una alerta.</div>
                )}
              </div>
            </div>
          </div>
        ) : null}

        {role === "supervisor" && supervisorModule === "evidencias" ? (
          <div className="card e013SupervisorQueue">
            <div className="e013TopBar">
              <div>
                <div className="sectionTitle e010PageTitle">Revisar evidencias</div>
                <div className="contextHint e013Sub">Revisión continua: decide y avanza de inmediato; Promobolsillo guarda las revisiones en segundo plano.</div>
              </div>
              <div className="e013CounterStrip">
                <span><strong>{filteredSupervisorEvidences.length}</strong><small>Evidencias</small></span>
                <span><strong>{filteredSupervisorOutOfServiceRows.length}</strong><small>Sin servicio</small></span>
                <span><strong>{supervisorEvidenceSummary.pendientes}</strong><small>Pendientes</small></span>
                <span><strong>{supervisorEvidenceSummary.observadas + supervisorEvidenceSummary.rechazadas}</strong><small>Con acción</small></span>
              </div>
            </div>

            <div className={`e028Fix2SyncBar ${supervisorReviewLastError ? "e028Fix2SyncError" : ""}`}>
              <div className="e028Fix2SyncState">
                {supervisorReviewSyncing ? (
                  <><RefreshCw className="spin" size={15} /><strong>Guardando {supervisorReviewQueue.length} revisión(es)...</strong></>
                ) : supervisorReviewQueue.length ? (
                  <><RefreshCw size={15} /><strong>{supervisorReviewQueue.length} revisión(es) pendientes de sincronizar</strong></>
                ) : (
                  <><CheckCircle2 size={15} /><strong>Todo sincronizado</strong></>
                )}
                {supervisorReviewLastError ? <span>{supervisorReviewLastError}</span> : <span>Puedes continuar revisando sin esperar.</span>}
              </div>
              <div className="e028Fix2SyncActions">
                {lastSupervisorReviewUndo && lastSupervisorReviewUndo.expiresAt > Date.now() ? (
                  <button type="button" className="actionButton compactBtn" onClick={() => undoLastSupervisorReview()}>
                    <RotateCcw size={14} />
                    <span>Deshacer última acción</span>
                  </button>
                ) : null}
                {supervisorReviewQueue.length && !supervisorReviewSyncing ? (
                  <button type="button" className="actionButton compactBtn" onClick={() => void syncSupervisorReviewQueue(true)}>
                    <RefreshCw size={14} />
                    <span>Sincronizar ahora</span>
                  </button>
                ) : null}
              </div>
            </div>

            <div className="e020FilterPanel">
              <div className="e020FilterGrid">
                <label className="e020FilterField">
                  <span>Periodo</span>
                  <select className="inputLike" value={draftSupEvidenceDatePreset} onChange={(e) => setDraftSupEvidenceDatePreset(e.target.value as "hoy" | "semana" | "rango")}>
                    <option value="hoy">Hoy</option>
                    <option value="semana">Semana actual</option>
                    <option value="rango">Rango personalizado</option>
                  </select>
                </label>
                <label className="e020FilterField">
                  <span>Tipo</span>
                  <select className="inputLike" value={draftSupReviewContentFilter} onChange={(e) => setDraftSupReviewContentFilter(e.target.value as "evidencias" | "fuera" | "todo")}>
                    <option value="evidencias">Evidencias</option>
                    <option value="fuera">Sin servicio</option>
                    <option value="todo">Todo</option>
                  </select>
                </label>
                <label className="e020FilterField">
                  <span>Estado</span>
                  <select className="inputLike" value={draftSupEvidenceStatusFilter} onChange={(e) => setDraftSupEvidenceStatusFilter(e.target.value)}>
                    <option value="PENDIENTE">Pendientes</option>
                    <option value="APROBADA">Aprobadas</option>
                    <option value="OBSERVADA">Comentadas</option>
                    <option value="RECHAZADA">Rechazadas</option>
                    <option value="">Todo</option>
                  </select>
                </label>
              </div>
              {draftSupEvidenceDatePreset === "rango" ? (
                <div className="e020RangeGrid">
                  <label className="e020FilterField"><span>Desde</span><input className="inputLike" type="date" value={draftSupEvidenceDateStart} onChange={(e) => setDraftSupEvidenceDateStart(e.target.value)} /></label>
                  <label className="e020FilterField"><span>Hasta</span><input className="inputLike" type="date" value={draftSupEvidenceDateEnd} onChange={(e) => setDraftSupEvidenceDateEnd(e.target.value)} /></label>
                </div>
              ) : null}
              <div className="e020FilterActions">
                <button type="button" className="primaryBtn compactBtn e020UpdateBtn" onClick={() => applySupervisorMainFilters()} disabled={syncing}>
                  <RefreshCw size={15} />
                  <span>{syncing ? "Sincronizando..." : "Actualizar resultados"}</span>
                </button>
                <span className="e020ActiveFilterText">Activo: {supervisorDateBounds.label} · {supReviewContentFilter === "fuera" ? "Sin servicio" : supReviewContentFilter === "todo" ? "Todo" : "Evidencias"} · {supEvidenceStatusFilter ? getSupervisorReviewLabel(supEvidenceStatusFilter) : "Todo"}</span>
              </div>
            </div>

            <details className="e013FiltersDrawer">
              <summary>Filtros opcionales</summary>
              <div className="filtersRow filtersRowSupervisorTop">
                <select className="inputLike" value={supEvidencePromotorFilter} onChange={(e) => { setSupEvidencePromotorFilter(e.target.value); setSupEvidenceStoreFilter(""); setSupEvidenceBrandFilter(""); setSupEvidenceTypeFilter(""); setSupEvidencePhaseFilter(""); }}>
                  <option value="">Todos los promotores</option>
                  {supervisorPromotorOptions.map((opt) => <option key={opt.id} value={opt.id}>{opt.nombre}</option>)}
                </select>
                <select className="inputLike" value={supEvidenceStoreFilter} onChange={(e) => { setSupEvidenceStoreFilter(e.target.value); setSupEvidenceBrandFilter(""); setSupEvidenceTypeFilter(""); setSupEvidencePhaseFilter(""); }}>
                  <option value="">Todas las tiendas</option>
                  {supervisorEvidenceFilterOptions.stores.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <select className="inputLike" value={supEvidenceBrandFilter} onChange={(e) => { setSupEvidenceBrandFilter(e.target.value); setSupEvidenceTypeFilter(""); setSupEvidencePhaseFilter(""); }}>
                  <option value="">Todas las marcas</option>
                  {supervisorEvidenceFilterOptions.brands.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <button className="actionButton compactBtn" onClick={() => clearSupervisorEvidenceFilters()}><Trash2 size={14} /><span>Limpiar</span></button>
              </div>
            </details>

            {supReviewContentFilter !== "fuera" && filteredSupervisorEvidences.length ? (
              <div className="e019BatchBar">
                <div className="e019BatchInfo"><strong>{selectedSupEvidenceIds.length}</strong> seleccionada(s)</div>
                <button type="button" className="actionButton compactBtn" onClick={() => selectedSupEvidenceIds.length === visibleSupervisorQueueItems.length ? setSelectedSupEvidenceIds([]) : selectAllVisibleSupervisorEvidences()}>{selectedSupEvidenceIds.length === visibleSupervisorQueueItems.length ? "Limpiar selección" : "Seleccionar cargadas"}</button>
                <button type="button" className="actionButton compactBtn e013Approve" disabled={!selectedSupEvidenceIds.length || !!reviewActionInProgress} onClick={() => void runBatchEvidenceReview("APROBADA")}><Check size={14} /><span>"Aprobar selección"</span></button>
                <button type="button" className="actionButton compactBtn e013Comment" disabled={!selectedSupEvidenceIds.length || !!reviewActionInProgress} onClick={() => void runBatchEvidenceReview("OBSERVADA")}><Pencil size={14} /><span>"Comentar selección"</span></button>
                <button type="button" className="actionButton compactBtn e013Reject" disabled={!selectedSupEvidenceIds.length || !!reviewActionInProgress} onClick={() => void runBatchEvidenceReview("RECHAZADA")}><Trash2 size={14} /><span>"Rechazar selección"</span></button>
              </div>
            ) : null}
            {reviewActionInProgress ? <div className="e019ActionNotice"><CheckCircle2 size={15} /> Decisión aplicada. Avanzando a la siguiente foto...</div> : null}

            {!supervisorReviewVisibleCount ? (
              <div className="emptyBox">No hay registros pendientes con los filtros actuales.</div>
            ) : (
              <div className="e013QueueLayout">
                <aside className="e013QueueList" aria-label="Cola de evidencias por revisar">
                  <div ref={supervisorQueueTopRef} className="e018ScrollAnchor" />
                  {supReviewContentFilter !== "fuera" ? visibleSupervisorQueueItems.map((item) => (
                    <button key={item.evidencia_id} type="button" className={`e013QueueItem ${selectedSupEvidenceId === item.evidencia_id ? "e013QueueItemActive" : ""}`} onClick={() => focusSupervisorEvidence(item)}>
                      <div className="e013MiniPhoto">
                        {getSupervisorThumbSrc(item) ? (
                          <img
                            src={getSupervisorThumbSrc(item)}
                            alt={item.tipo_evidencia || item.tipo_evento || "Evidencia"}
                            loading="lazy"
                            decoding="async"
                          />
                        ) : (
                          <span className="e028DeferredThumb">{isSupervisorPhotoLoading(item.evidencia_id) ? "Cargando..." : "Foto histórica"}</span>
                        )}
                      </div>
                      <div className="e013QueueText">
                        <div className="e013QueueTitle">{item.tienda_display || item.tienda_nombre || item.tienda_id || "Tienda"} · {normalizeBrandLabel(String(item.marca_nombre || item.marca_id || ""), "Marca")}</div>
                        <div className="e013QueueMeta e018QueueMetaName">{item.promotor_nombre || item.promotor_id || "Promotor"}</div>
                        <div className="e013QueueMeta e018QueueMetaDate">{item.fecha_hora_fmt || "Sin hora"}</div>
                        <div className="e013QueueMeta">{item.tipo_evidencia || item.tipo_evento || "Evidencia"}{item.fase ? ` · ${item.fase}` : ""}</div>
                        <div className="e013QueueBadges">
                          <span className={`riskBadge ${getSupervisorReviewClass(item)}`}>{getSupervisorReviewLabel(item)}</span>
                          {item.local_review_pending ? <span className="riskBadge e028Fix2PendingBadge">Guardando</span> : null}
                          <span className={`riskBadge ${severityClass(item.riesgo || "BAJO")}`}>{item.riesgo || "Sin riesgo"}</span>
                        </div>
                      </div>
                      <label className="e019SelectPill" onClick={(e) => e.stopPropagation()}>
                        <input type="checkbox" checked={selectedSupEvidenceIds.includes(item.evidencia_id)} onChange={() => toggleSupervisorEvidenceSelection(item.evidencia_id)} />
                        Seleccionar
                      </label>
                      <span className="e013ReviewCta">Revisar</span>
                    </button>
                  )) : null}
                  {supReviewContentFilter !== "evidencias" ? filteredSupervisorOutOfServiceRows.map((item) => (
                    <div key={item.registro_id} className="e018OutServiceCard">
                      <div className="e018OutServiceTitle">{item.tienda_display || item.tienda_nombre || item.tienda_id || "Tienda"} · {item.marca_nombre || item.marca_id || "Marca"}</div>
                      <div className="e018OutServiceMeta">{item.promotor_nombre || item.promotor_id || "Promotor"}</div>
                      <div className="e018OutServiceMeta">{item.fecha_hora_fmt || item.fecha_hora || "Sin fecha"}</div>
                      <div className="e018OutServiceReason">Fuera de servicio · {item.motivo || "Sin motivo"}</div>
                      {item.comentario ? <div className="e018OutServiceComment">Comentario: {item.comentario}</div> : null}
                      {item.visita_id ? <button type="button" className="actionButton compactBtn" onClick={() => void openVisitExpedient(item.visita_id || "")}><Eye size={14} /><span>Ver visita</span></button> : null}
                    </div>
                  )) : null}
                  {supReviewContentFilter !== "fuera" && supervisorQueueVisibleCount < filteredSupervisorEvidences.length ? (
                    <button
                      type="button"
                      className="actionButton e028LoadMoreBtn"
                      onClick={() => setSupervisorQueueVisibleCount((current) => Math.min(filteredSupervisorEvidences.length, current + 24))}
                    >
                      Mostrar 24 más · {filteredSupervisorEvidences.length - supervisorQueueVisibleCount} pendientes por cargar
                    </button>
                  ) : null}
                  <div ref={supervisorQueueBottomRef} className="e018ScrollAnchor" />
                </aside>

                <section ref={supervisorReviewDetailRef} className="e013ReviewFocus" aria-label="Revisión de evidencia seleccionada">
                  {selectedSupervisorEvidence ? (
                    <>
                      <div className="e013ReviewHeader">
                        <button className="actionButton compactBtn" onClick={() => {
                          const i = filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === selectedSupervisorEvidence.evidencia_id);
                          const prev = filteredSupervisorEvidences[Math.max(0, i - 1)];
                          if (prev) setSelectedSupEvidenceId(prev.evidencia_id);
                        }} disabled={filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === selectedSupervisorEvidence.evidencia_id) <= 0}>‹ Anterior</button>
                        <div className="e013ReviewPosition">Foto {Math.max(1, filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === selectedSupervisorEvidence.evidencia_id) + 1)} de {filteredSupervisorEvidences.length}</div>
                        <button className="actionButton compactBtn" onClick={() => {
                          const i = filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === selectedSupervisorEvidence.evidencia_id);
                          const next = filteredSupervisorEvidences[Math.min(filteredSupervisorEvidences.length - 1, i + 1)];
                          if (next) setSelectedSupEvidenceId(next.evidencia_id);
                        }} disabled={filteredSupervisorEvidences.findIndex((item) => item.evidencia_id === selectedSupervisorEvidence.evidencia_id) >= filteredSupervisorEvidences.length - 1}>Siguiente ›</button>
                      </div>

                      <div className="e013PhotoStage" onDoubleClick={() => void openSupervisorEvidenceViewer(selectedSupervisorEvidence)}>
                        {getSupervisorFullSrc(selectedSupervisorEvidence) ? (
                          <img
                            src={getSupervisorFullSrc(selectedSupervisorEvidence)}
                            alt={selectedSupervisorEvidence.tipo_evidencia || "Evidencia"}
                            decoding="async"
                            onClick={() => handleImageTap(getSupervisorFullSrc(selectedSupervisorEvidence))}
                          />
                        ) : (
                          <div className="e028PhotoLoading">
                            <RefreshCw className={isSupervisorPhotoLoading(selectedSupervisorEvidence.evidencia_id) ? "spin" : ""} size={22} />
                            <strong>{isSupervisorPhotoLoading(selectedSupervisorEvidence.evidencia_id) ? "Cargando fotografía..." : "Fotografía disponible bajo demanda"}</strong>
                            <span>La cola ya cargó; ahora se descarga únicamente esta evidencia.</span>
                            {!isSupervisorPhotoLoading(selectedSupervisorEvidence.evidencia_id) ? (
                              <button type="button" className="actionButton compactBtn" onClick={() => void ensureSupervisorEvidencePhoto(selectedSupervisorEvidence)}>
                                Cargar fotografía
                              </button>
                            ) : null}
                          </div>
                        )}
                        <button type="button" className="e013ZoomBtn" disabled={!getSupervisorFullSrc(selectedSupervisorEvidence) && isSupervisorPhotoLoading(selectedSupervisorEvidence.evidencia_id)} onClick={(e) => { e.stopPropagation(); void openSupervisorEvidenceViewer(selectedSupervisorEvidence); }}><Eye size={15} /> Zoom</button>
                      </div>

                      <div className="e013ContextLine">
                        <strong>{selectedSupervisorEvidence.tienda_display || selectedSupervisorEvidence.tienda_nombre || selectedSupervisorEvidence.tienda_id || "Tienda"}</strong>
                        <span>{normalizeBrandLabel(String(selectedSupervisorEvidence.marca_nombre || selectedSupervisorEvidence.marca_id || ""), "Marca")}</span>
                        <span>{selectedSupervisorEvidence.promotor_nombre || selectedSupervisorEvidence.promotor_id || "Promotor"}</span>
                      </div>
                      <div className="e013ContextSub">
                        <span>{selectedSupervisorEvidence.tipo_evidencia || selectedSupervisorEvidence.tipo_evento || "Evidencia"}</span>
                        {selectedSupervisorEvidence.fase ? <span>{selectedSupervisorEvidence.fase}</span> : null}
                        <span>{selectedSupervisorEvidence.fecha_hora_fmt}</span>
                      </div>
                      {(selectedSupervisorEvidence.hallazgos_ai || selectedSupervisorEvidence.reglas_disparadas) ? (
                        <div className="e013ObservationBox">
                          {selectedSupervisorEvidence.hallazgos_ai ? <div><strong>Hallazgo:</strong> {selectedSupervisorEvidence.hallazgos_ai}</div> : null}
                          {selectedSupervisorEvidence.reglas_disparadas ? <div><strong>Reglas:</strong> {selectedSupervisorEvidence.reglas_disparadas}</div> : null}
                        </div>
                      ) : null}

                      <input className="inputLike e013CommentInput" value={reviewNote} onChange={(e) => setReviewNote(e.target.value)} placeholder="Comentario opcional para comentar o rechazar" />

                      <div className="e013DecisionDock">
                        <button className="actionButton e013Approve" disabled={!!reviewActionInProgress} onClick={() => void quickReviewEvidence(selectedSupervisorEvidence, "APROBADA")}><Check size={16} /><span>"Aprobar"</span></button>
                        <button className="actionButton e013Comment" disabled={!!reviewActionInProgress} onClick={() => { setReviewDecision("OBSERVADA"); void quickReviewEvidence(selectedSupervisorEvidence, "OBSERVADA"); }}><Pencil size={16} /><span>"Comentar"</span></button>
                        <button className="actionButton e013Reject" disabled={!!reviewActionInProgress} onClick={() => { setReviewDecision("RECHAZADA"); void quickReviewEvidence(selectedSupervisorEvidence, "RECHAZADA"); }}><Trash2 size={16} /><span>"Rechazar"</span></button>
                      </div>
                    </>
                  ) : (
                    <div className="emptyBox">Selecciona una evidencia de la cola.</div>
                  )}
                </section>
              </div>
            )}
            {supervisorReviewVisibleCount > 12 ? (
              <div className="e018FloatingNav" aria-label="Navegación rápida de revisión">
                <button type="button" onClick={() => scrollElementIntoView(supervisorQueueTopRef, "start")}>↑ Inicio</button>
                <button type="button" onClick={() => scrollElementIntoView(supervisorQueueBottomRef, "end")}>↓ Final</button>
              </div>
            ) : null}
          </div>
        ) : null}
        {role === "promotor" && promotorEvidenceViewFilter !== "fuera" && (promotorModule === "evidencias" || promotorModule === "mis_evidencias") && filteredOperationalGallery.length > 0 ? (
          <div className="card">
            <div className="e018SectionHeader">
              <div className="sectionTitle">Galería de evidencias</div>
              <div className="e018MiniNavRow e018MiniNavRowInline">
                <button type="button" onClick={() => scrollHorizontalRefToStart(promotorGalleryScrollRef)}>← Inicio</button>
                <button type="button" onClick={() => scrollHorizontalRefToEnd(promotorGalleryScrollRef)}>Final →</button>
              </div>
            </div>
            <div className="galleryScroll" ref={promotorGalleryScrollRef}>
              <div className="galleryGrid">
                {filteredOperationalGallery.slice(0, 30).map((item) => (
                  <button type="button" className="galleryCard galleryCardCompact e018GalleryCardBtn e024iGalleryEvidenceCard" key={item.evidencia_id} onClick={() => focusPromotorEvidence(item)}>
                    <div className="imageFrame imageFrameCompact e024iGalleryImageFrame"><img src={item.url_foto} alt={item.tipo_evidencia} className="img" onDoubleClick={(e) => { e.stopPropagation(); openImageViewer(item.url_foto); }} /></div>
                    <div className="galleryBodyCompact e024iGalleryBody">
                      <div className="e024iDataRows e024iGalleryRows">
                        <div><span>Tienda</span><strong>{getStoreDisplayFromItem(item) || "Visita activa"}</strong></div>
                        <div><span>Marca</span><strong>{normalizeBrandLabel(item.marca_nombre, "Marca")}</strong></div>
                        <div><span>Tipo</span><strong>{item.tipo_evidencia || item.tipo_evento || "Evidencia"}</strong></div>
                        <div><span>Fase</span><strong>{formatPhaseLabel(item.fase || "ESTADO_ACTUAL")}</strong></div>
                        <div><span>Fecha</span><strong>{item.fecha_hora_fmt || formatDateTimeMaybe(item.fecha_hora || "")}</strong></div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          </div>
        ) : null}

        {role === "supervisor" && expedient ? (
          <div className="card">
            <div className="sectionTitle">Expediente de visita</div>
            {expedientLoading ? (
              <div className="emptyBox">Cargando expediente...</div>
            ) : (
              <div className="twoCol">
                <div className="panel">
                  <div className="miniTitle">Visita</div>
                  <div className="summaryLine"><strong>{expedient.visita?.promotor_nombre || "Promotor"}</strong></div>
                  <div className="summaryLine">Tienda: {expedient.visita?.tienda_display || expedient.visita?.tienda_nombre || expedient.visita?.tienda_id || "-"}</div>
                  <div className="summaryLine">Entrada: {expedient.visita?.entry_fmt || formatHourFromIso(expedient.visita?.hora_inicio || "")}</div>
                  <div className="summaryLine">Salida: {expedient.visita?.exit_fmt || (expedient.visita?.hora_fin ? formatHourFromIso(expedient.visita.hora_fin) : "Pendiente")}</div>
                  <div className="summaryLine">Tiempo de estancia: {typeof expedient.visita?.stay_minutes === "number" ? `${expedient.visita.stay_minutes} min` : "-"}</div>
                  <div className="geoRow">
                    <span className={`geoBadge ${geofenceClass(expedient.visita?.resultado_geocerca_entrada)}`}>E: {geofenceShortLabel(expedient.visita?.resultado_geocerca_entrada)}</span>
                    <span className={`geoBadge ${geofenceClass(expedient.visita?.resultado_geocerca_salida)}`}>S: {geofenceShortLabel(expedient.visita?.resultado_geocerca_salida)}</span>
                  </div>
                </div>
                <div className="panel">
                  <div className="miniTitle">Resumen de la visita</div>
                  <div className="summaryLine">Evidencias operativas: <strong>{expedient.summary?.total_evidencias || 0}</strong></div>
                  <div className="summaryLine">Alertas: <strong>{expedient.summary?.total_alertas || 0}</strong></div>
                  {expedient.summary_by_brand?.length ? expedient.summary_by_brand.map((brand) => (
                    <div key={`${brand.marca_id || brand.marca_nombre}`} className="traceBox">
                      <div className="traceTitle">Marca · {brand.marca_nombre || brand.marca_id || "Marca"} <span style={{ fontWeight: 400 }}>({brand.total || 0})</span></div>
                      {(brand.types || []).map((tipo) => (
                        <div key={`${brand.marca_id}-${tipo.tipo_evidencia}`} style={{ marginTop: 6, paddingLeft: 10, borderLeft: "2px solid rgba(15,118,110,0.15)" }}>
                          <div className="summaryLine"><strong>Tipo:</strong> {tipo.tipo_evidencia} <span style={{ opacity: 0.75 }}>({tipo.total || 0})</span></div>
                          <div className="summaryLine"><strong>Fases:</strong> {(tipo.phases || []).map((phase) => `${phase.fase || "NA"} ${phase.total || 0}`).join(" · ")}</div>
                        </div>
                      ))}
                    </div>
                  )) : null}
                </div>
                <div className="panel">
                  <div className="miniTitle">Alertas ligadas</div>
                  <div className="stack compactStack">
                    {(expedient.alertas || []).map((item) => (
                      <div className="listBtn" key={item.alerta_id}>
                        <div className="listTitle">{item.tipo_alerta}</div>
                        <div className="listSub">{item.descripcion}</div>
                        <div className="geoRow">
                          <span className={`riskBadge ${severityClass(item.severidad)}`}>{item.severidad}</span>
                          <span className={`riskBadge ${statusClass(item.status)}`}>{item.status}</span>
                        </div>
                      </div>
                    ))}
                    {!(expedient.alertas || []).length ? <div className="emptyBox">Sin alertas ligadas.</div> : null}
                  </div>
                </div>
                <div className="panel fullSpan">
                  <div className="miniTitle">Asistencia de la visita</div>
                  <div className="galleryScroll compactGalleryScroll">
                  <div className="galleryGrid">
                    {expedientAttendance.map((item) => (
                      <div className="galleryCard galleryCardCompact" key={item.evidencia_id}>
                        <div className="imageFrame imageFrameCompact"><img src={item.url_foto} alt={item.tipo_evento} className="img" onDoubleClick={() => openImageViewer(item.url_foto)} onClick={(e) => { e.stopPropagation(); handleImageTap(item.url_foto); }} /></div>
                        <div className="galleryBodyCompact">
                          <div className="galleryTop compactTop">
                            <div className="galleryTitle">{item.tipo_evento === "ASISTENCIA_ENTRADA" ? "Entrada" : "Salida"}</div>
                            <span className={`riskBadge ${severityClass(item.riesgo)}`}>{item.riesgo}</span>
                          </div>
                          <div className="galleryDate">{item.fecha_hora_fmt}</div>
                          <div className="galleryDesc compactDesc">{cleanEvidenceDescription(item.descripcion)}</div>
                        </div>
                      </div>
                    ))}
                    {!expedientAttendance.length ? <div className="emptyBox">Sin fotos de asistencia ligadas.</div> : null}
                  </div>
                  </div>
                </div>
                <div className="panel fullSpan">
                  <div className="miniTitle">Evidencias operativas de la visita</div>
                  <div className="galleryScroll compactGalleryScroll">
                  <div className="galleryGrid">
                    {expedientOperational.map((item) => (
                      <div className="galleryCard galleryCardCompact" key={item.evidencia_id}>
                        <div className="imageFrame imageFrameCompact"><img src={item.url_foto} alt={item.tipo_evidencia} className="img" onDoubleClick={() => openImageViewer(item.url_foto)} onClick={(e) => { e.stopPropagation(); handleImageTap(item.url_foto); }} /></div>
                        <div className="galleryBodyCompact">
                          <div className="galleryTop compactTop">
                            <div className="galleryTitle">{item.tipo_evidencia || item.tipo_evento}</div>
                            <span className={`riskBadge ${severityClass(item.riesgo)}`}>{item.riesgo}</span>
                          </div>
                          <div className="gallerySub compactMeta">{compactMetaLine(item)}</div>
                          <div className="galleryDate">{item.fecha_hora_fmt}</div>
                          <div className="galleryDesc compactDesc">{cleanEvidenceDescription(item.descripcion)}</div>
                        </div>
                      </div>
                    ))}
                    {!expedientOperational.length ? <div className="emptyBox">Sin evidencias operativas ligadas.</div> : null}
                  </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        ) : null}

        {cameraModal.open ? (
          <div className="overlayBackdrop cameraBackdrop" onClick={() => void closeCameraModal()}>
            <div className={`cameraModal cameraModalTight ${cameraModal.facing === "environment" ? "cameraModalEvidence" : ""}`} onClick={(e) => e.stopPropagation()}>
              <div className="miniTitle cameraTitle">{cameraModal.target === "entrada" ? "Selfie de entrada" : cameraModal.target === "salida" ? "Foto de salida" : cameraModal.target === "reemplazo" ? "Reemplazar evidencia" : "Foto de evidencia"}</div>
              <div className={`cameraViewport cameraViewportTight ${cameraModal.facing === "environment" ? "cameraViewportEvidence" : ""}`}>
                <video ref={cameraVideoRef} className={`cameraVideo cameraVideoTight ${cameraModal.facing === "environment" ? "cameraVideoEvidence" : ""}`} playsInline muted autoPlay />
              </div>
              {cameraModal.facing === "environment" ? (
                <div className="cameraHint e020cCameraHint">Usa el celular en horizontal si necesitas una toma más panorámica. Si tu equipo lo permite, ajusta el zoom antes de capturar.</div>
              ) : (
                <div className="cameraHint e020cCameraHint">Cámara frontal para selfie de asistencia.</div>
              )}
              {cameraZoom.supported ? (
                <div className="e020cZoomBox">
                  <div className="e020cZoomLabel">Zoom</div>
                  <input className="e020cZoomRange" type="range" min={cameraZoom.min} max={cameraZoom.max} step={cameraZoom.step} value={cameraZoom.value} onChange={(e) => void changeCameraZoom(Number(e.target.value))} />
                  <div className="e020cZoomValue">{cameraZoom.value.toFixed(1)}x</div>
                </div>
              ) : cameraModal.facing === "environment" ? (
                <div className="cameraHint e020cZoomUnavailable">Zoom no disponible en este equipo o dentro de Telegram.</div>
              ) : null}
              <div className="cameraActionRow cameraActionRowTight">
                <button className="cameraCaptureBtn cameraCaptureBtnTight" onClick={() => void captureFromCameraModal()}><Camera size={18} />Capturar</button>
                <button className="cameraCancelBtn cameraCancelBtnTight" onClick={() => void closeCameraModal()}><Trash2 size={16} />Cancelar</button>
              </div>
            </div>
          </div>
        ) : null}

        {imageViewerSrc ? (
          <div
            className="overlayBackdrop overlayBackdropRich"
            style={{ paddingTop: 78, paddingBottom: 82, alignItems: "center", justifyItems: "center" }}
            onClick={(e) => { if (e.target === e.currentTarget) closeImageViewer(); }}
            onMouseMove={handleImageViewerMouseMove as any}
            onMouseUp={handleImageViewerMouseUp}
            onMouseLeave={handleImageViewerMouseUp}
            onTouchStart={handleImageViewerTouchStart as any}
            onTouchMove={handleImageViewerTouchMove as any}
            onTouchEnd={handleImageViewerTouchEnd}
          >
            <div
              className="e014ViewerTopbar"
              style={{ position: "fixed", top: 10, left: 8, right: 8, zIndex: 200, display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, pointerEvents: "auto" }}
              onClick={(e) => e.stopPropagation()}
            >
              <button
                type="button"
                className="e014ViewerClose"
                style={{ border: "1px solid rgba(255,255,255,0.24)", background: "rgba(15,23,42,0.88)", color: "#fff", borderRadius: 999, padding: "11px 13px", fontWeight: 900, cursor: "pointer", boxShadow: "0 10px 24px rgba(0,0,0,0.28)", backdropFilter: "blur(10px)", WebkitBackdropFilter: "blur(10px)" }}
                onClick={closeImageViewer}
              >
                × Cerrar
              </button>
              <div
                className="e014ViewerControls"
                style={{ display: "flex", alignItems: "center", gap: 6, borderRadius: 999, padding: 5, background: "rgba(15,23,42,0.76)", border: "1px solid rgba(255,255,255,0.16)", color: "#fff", fontWeight: 850, overflowX: "auto", maxWidth: "66vw", boxShadow: "0 10px 24px rgba(0,0,0,0.24)", backdropFilter: "blur(10px)", WebkitBackdropFilter: "blur(10px)" }}
              >
                <button type="button" style={{ minWidth: 38, minHeight: 36, borderRadius: 999, border: "1px solid rgba(255,255,255,0.20)", background: "rgba(255,255,255,0.10)", color: "#fff", fontWeight: 900 }} onClick={() => zoomImageViewer(imageViewerScale - 0.25)}>−</button>
                <span style={{ minWidth: 44, textAlign: "center" }}>{Math.round(imageViewerScale * 100)}%</span>
                <button type="button" style={{ minWidth: 38, minHeight: 36, borderRadius: 999, border: "1px solid rgba(255,255,255,0.20)", background: "rgba(255,255,255,0.10)", color: "#fff", fontWeight: 900 }} onClick={() => zoomImageViewer(imageViewerScale + 0.25)}>+</button>
                <button type="button" style={{ minHeight: 36, borderRadius: 999, border: "1px solid rgba(255,255,255,0.20)", background: "rgba(255,255,255,0.10)", color: "#fff", fontWeight: 900, padding: "0 10px" }} onClick={() => { setImageViewerOffset({ x: 0, y: 0 }); zoomImageViewer(1); }}>Ajustar</button>
              </div>
            </div>
            {activeViewerSupervisorEvidence ? (
              <>
                <div
                  onClick={(e) => e.stopPropagation()}
                  style={{ position: "fixed", top: 12, left: 12, zIndex: 92, maxWidth: "min(420px, calc(100vw - 96px))", borderRadius: 14, background: "rgba(15,23,42,0.56)", border: "1px solid rgba(255,255,255,0.12)", backdropFilter: "blur(8px)", color: "#fff", padding: "10px 12px", display: "grid", gap: 6 }}
                >
                  <div style={{ fontWeight: 700, fontSize: 14, lineHeight: 1.2 }}>{normalizeBrandLabel(activeViewerSupervisorEvidence.marca_nombre || "", activeViewerSupervisorEvidence.marca_id || "Marca")} · {activeViewerSupervisorEvidence.tipo_evidencia || activeViewerSupervisorEvidence.tipo_evento}</div>
                  <div style={{ fontSize: 12, opacity: 0.84 }}>{activeViewerSupervisorEvidence.promotor_nombre || activeViewerSupervisorEvidence.promotor_id || "Promotor"} · {getStoreDisplayFromItem(activeViewerSupervisorEvidence) || activeViewerSupervisorEvidence.tienda_nombre || "Tienda"}</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    {activeViewerSupervisorEvidence.fase ? <span className="riskBadge riskNeutral">{activeViewerSupervisorEvidence.fase}</span> : null}
                    <span className={`riskBadge ${severityClass(activeViewerSupervisorEvidence.riesgo || "BAJO")}`}>{activeViewerSupervisorEvidence.riesgo || "Sin riesgo"}</span>
                    <span className={`riskBadge ${getSupervisorReviewClass(activeViewerSupervisorEvidence)}`}>{getSupervisorReviewLabel(activeViewerSupervisorEvidence)}</span>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); closeImageViewer(); }}
                  style={{ position: "fixed", top: 12, right: 12, zIndex: 92, borderRadius: 999, border: "1px solid rgba(255,255,255,0.16)", background: "rgba(15,23,42,0.62)", color: "#fff", padding: "10px 14px", cursor: "pointer", backdropFilter: "blur(8px)", fontWeight: 700 }}
                >
                  Cerrar / volver
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); void moveSupervisorEvidenceViewer(-1); }}
                  disabled={activeViewerSupervisorEvidenceIndex <= 0}
                  style={{ position: "fixed", left: 12, top: "50%", transform: "translateY(-50%)", zIndex: 92, borderRadius: 999, border: "1px solid rgba(255,255,255,0.16)", background: activeViewerSupervisorEvidenceIndex <= 0 ? "rgba(15,23,42,0.28)" : "rgba(15,23,42,0.62)", color: "#fff", padding: "12px 14px", cursor: activeViewerSupervisorEvidenceIndex <= 0 ? "not-allowed" : "pointer", backdropFilter: "blur(8px)", fontWeight: 700 }}
                >
                  ‹
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); void moveSupervisorEvidenceViewer(1); }}
                  disabled={activeViewerSupervisorEvidenceIndex < 0 || activeViewerSupervisorEvidenceIndex >= activeViewerSupervisorEvidenceSequence.length - 1}
                  style={{ position: "fixed", right: 12, top: "50%", transform: "translateY(-50%)", zIndex: 92, borderRadius: 999, border: "1px solid rgba(255,255,255,0.16)", background: activeViewerSupervisorEvidenceIndex < 0 || activeViewerSupervisorEvidenceIndex >= activeViewerSupervisorEvidenceSequence.length - 1 ? "rgba(15,23,42,0.28)" : "rgba(15,23,42,0.62)", color: "#fff", padding: "12px 14px", cursor: activeViewerSupervisorEvidenceIndex < 0 || activeViewerSupervisorEvidenceIndex >= activeViewerSupervisorEvidenceSequence.length - 1 ? "not-allowed" : "pointer", backdropFilter: "blur(8px)", fontWeight: 700 }}
                >
                  ›
                </button>
              </>
            ) : null}

            <img
              src={imageViewerSrc}
              alt="Vista ampliada"
              className="overlayImage"
              draggable={false}
              style={{ maxWidth: "calc(100vw - 20px)", maxHeight: "calc(100vh - 168px)", objectFit: "contain", transform: `translate(${imageViewerOffset.x}px, ${imageViewerOffset.y}px) scale(${imageViewerScale})`, cursor: imageViewerScale > 1 ? (imageViewerDragging ? "grabbing" : "grab") : "zoom-in", transition: imageViewerDragging ? "none" : "transform .12s ease", userSelect: "none" }}
              onClick={(e) => e.stopPropagation()}
              onWheel={handleImageViewerWheel}
              onMouseDown={handleImageViewerMouseDown}
              onMouseMove={handleImageViewerMouseMove}
              onMouseUp={handleImageViewerMouseUp}
              onMouseLeave={handleImageViewerMouseUp}
              onTouchStart={handleImageViewerTouchStart}
              onTouchMove={handleImageViewerTouchMove}
              onTouchEnd={handleImageViewerTouchEnd}
              onDoubleClick={(e) => { e.stopPropagation(); if (imageViewerScale > 1) { setImageViewerOffset({ x: 0, y: 0 }); zoomImageViewer(1); } else { zoomImageViewer(2); } }}
            />

            <div
              style={{ position: "fixed", left: 8, right: 8, bottom: 10, zIndex: 200, display: "flex", justifyContent: "center", pointerEvents: "auto" }}
              onClick={(e) => e.stopPropagation()}
            >
              <button
                type="button"
                onClick={closeImageViewer}
                style={{ width: "min(360px, calc(100vw - 24px))", border: "1px solid rgba(255,255,255,0.22)", background: "rgba(15,23,42,0.90)", color: "#fff", borderRadius: 999, padding: "13px 16px", fontWeight: 900, boxShadow: "0 14px 30px rgba(0,0,0,0.32)", backdropFilter: "blur(10px)", WebkitBackdropFilter: "blur(10px)" }}
              >
                Cerrar imagen y volver
              </button>
            </div>

            {activeViewerSupervisorEvidence ? (
              <div
                onClick={(e) => e.stopPropagation()}
                style={{ position: "fixed", left: "50%", bottom: 14, transform: "translateX(-50%)", zIndex: 92, width: "min(920px, calc(100vw - 24px))", borderRadius: 16, background: "rgba(15,23,42,0.50)", border: "1px solid rgba(255,255,255,0.10)", backdropFilter: "blur(8px)", padding: 10, display: "grid", gap: 8 }}
              >
                <div style={{ display: "flex", gap: 8, alignItems: "center", justifyContent: "center", flexWrap: "wrap" }}>
                  <button className="actionButton compactBtn" onClick={() => void quickReviewEvidence(activeViewerSupervisorEvidence, "APROBADA")}><Check size={14} /><span>Aprobar</span></button>
                  <button className="actionButton compactBtn" onClick={() => { setReviewDecision("OBSERVADA"); void quickReviewEvidence(activeViewerSupervisorEvidence, "OBSERVADA"); }}><Pencil size={14} /><span>Comentar</span></button>
                  <button className="actionButton compactBtn" onClick={() => { setReviewDecision("RECHAZADA"); void quickReviewEvidence(activeViewerSupervisorEvidence, "RECHAZADA"); }}><Trash2 size={14} /><span>Rechazar</span></button>
                  <input className="inputLike" style={{ minWidth: 220, flex: "1 1 260px", maxWidth: 360, margin: 0 }} value={reviewNote} onChange={(e) => setReviewNote(e.target.value)} placeholder="Comentario" />
                </div>
                <div style={{ textAlign: "center", fontSize: 12, color: "rgba(255,255,255,0.74)" }}>Doble clic para zoom. Arrastra la imagen cuando esté ampliada.</div>
              </div>
            ) : null}
          </div>
        ) : null}

{statusMsg ? <div className="statusBar">{statusMsg}</div> : null}

        <div className="footerActions">
          <button className="secondaryBtn footerBtn" onClick={() => void refreshCurrentRoleData()} disabled={syncing || !!error}>
            <RefreshCw size={16} />
            {syncing ? "Sincronizando..." : "Recargar"}
          </button>
        </div>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  page: {
    minHeight: "100vh",
    background: "linear-gradient(180deg, #eef1f4 0%, #e7ebef 100%)",
    color: "#263238",
    padding: "12px 12px 28px",
    overflowX: "hidden",
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
  },
};

const globalCss = `
* { box-sizing: border-box; }
html, body, #root { max-width: 100%; overflow-x: hidden; }
body { margin: 0; background: #eef1f4; }
button, input, select { font: inherit; }
input[type=file] { display: none; }
.shell { width: 100%; max-width: 1180px; margin: 0 auto; overflow-x: hidden; }
.stickyTop { position: sticky; top: 0; z-index: 20; background: linear-gradient(180deg, rgba(238,241,244,0.97) 0%, rgba(238,241,244,0.92) 100%); backdrop-filter: blur(6px); padding-bottom: 8px; }
.hero { display: flex; background: linear-gradient(135deg, #f8f9fb 0%, #edf1f3 100%); border: 1px solid rgba(38,50,56,0.08); border-radius: 16px; padding: 8px 12px; box-shadow: 0 6px 16px rgba(38,50,56,0.06); }
.heroSplit { justify-content: space-between; align-items: center; gap: 12px; }
.heroLogoBlock { display: flex; align-items: center; min-width: 0; }
.brandWord { font-size: 22px; line-height: 1; font-weight: 900; letter-spacing: 0.02em; color: #43a047; }
/* E014E: Logo oficial REZGO en encabezado */
.heroLogoBlockE014E { flex-direction: column; align-items: flex-start; justify-content: center; gap: 2px; max-width: min(56%, 360px); }
.rezgoLogoE014E { display: block; width: min(210px, 44vw); max-height: 46px; object-fit: contain; object-position: left center; border: 0; background: transparent; }
.rezgoTaglineE014E { font-size: 10px; line-height: 1.1; color: #78909c; font-weight: 700; letter-spacing: 0.01em; padding-left: 2px; margin-top: 1px; }
@media (max-width: 420px) {
  .heroLogoBlockE014E { max-width: 52%; }
  .rezgoLogoE014E { width: min(174px, 48vw); max-height: 38px; }
  .rezgoTaglineE014E { font-size: 9px; }
  .heroTitleBlockWide { min-width: 150px; width: 45%; }
}

.heroTitleBlock { display: flex; flex-direction: column; align-items: flex-end; justify-content: center; margin-left: auto; overflow: hidden; }
.heroTitleBlockWide { width: min(240px, 48%); min-width: 190px; }
.heroTitle { font-size: 14px; line-height: 1.05; font-weight: 800; color: #263238; }
.heroTitleTight { text-align: right; max-width: 132px; }
.heroMetaSingle { color: #78909c; font-size: 10px; text-align: right; margin-top: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.heroMetaSingleWide { width: 100%; max-width: 220px; }
.card { margin-top: 12px; background: rgba(255,255,255,0.92); border: 1px solid rgba(38,50,56,0.08); border-radius: 18px; padding: 14px; box-shadow: 0 10px 22px rgba(38,50,56,0.07); }
.loadingCard { background: rgba(255,255,255,0.95); }
.warning { background: rgba(255,244,229,0.96); border-color: rgba(245,158,11,0.25); }
.warningRow, .loadingRow { display: flex; align-items: center; gap: 10px; color: #263238; }
.spin { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
.sectionTitle { font-size: 18px; font-weight: 800; color: #263238; }
.tabsBar { margin-top: 8px; display: flex; gap: 4px; overflow-x: auto; white-space: nowrap; background: rgba(255,255,255,0.92); border: 1px solid rgba(38,50,56,0.08); border-radius: 14px; padding: 4px; scrollbar-width: thin; }
.tabsInline::-webkit-scrollbar { height: 6px; }
.tabsInline::-webkit-scrollbar-thumb { background: rgba(96,125,139,0.24); border-radius: 999px; }
.tabBtn { border: 0; border-radius: 8px; background: transparent; color: #546e7a; padding: 8px 12px; cursor: pointer; font-weight: 700; flex: 0 0 auto; }
.tabBtnActive { background: rgba(76,175,80,.14); color: #2e7d32; }
/* E008_TAB_HORIZONTAL_SCROLL: en celular conserva una sola linea horizontal y permite scroll lateral. */
@media (max-width: 430px) {
  .tabsBar { display: flex; flex-wrap: nowrap; overflow-x: auto; overflow-y: hidden; white-space: nowrap; -webkit-overflow-scrolling: touch; scroll-snap-type: x proximity; gap: 4px; }
  .tabBtn { flex: 0 0 auto; min-width: 118px; width: auto; padding: 8px 12px; font-size: 12px; text-align: center; justify-content: center; scroll-snap-align: start; }
}
.twoCol { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 14px; }
.miniTitle { font-size: 15px; font-weight: 800; margin-bottom: 10px; color: #263238; }
.stack { display: flex; flex-direction: column; gap: 8px; }
.compactStack { max-height: 320px; overflow-y: auto; overflow-x: hidden; scrollbar-width: auto; scrollbar-color: rgba(76,175,80,.58) rgba(76,175,80,.12); border: 1px solid rgba(76,175,80,.16); border-radius: 14px; padding: 8px; background: rgba(255,255,255,0.72); }
.compactStack::-webkit-scrollbar { width: 8px; }
.compactStack::-webkit-scrollbar-thumb { background: rgba(76,175,80,.52); border-radius: 999px; }
.compactStack::-webkit-scrollbar-track { background: rgba(76,175,80,.10); border-radius: 999px; }
.listBtn { width: 100%; text-align: left; border-radius: 16px; border: 1px solid rgba(38,50,56,0.08); background: rgba(255,255,255,0.96); padding: 12px; color: #263238; cursor: pointer; }
.listBtnGreen { border-color: rgba(76,175,80,.45); background: rgba(232,245,233,0.95); }
.listTitle { font-weight: 800; }
.listSub { margin-top: 4px; color: #607d8b; font-size: 12px; }
.geoRow { margin-top: 6px; display: flex; gap: 6px; flex-wrap: wrap; }
.geoBadge { font-size: 11px; font-weight: 700; border-radius: 999px; padding: 4px 8px; }
.geoGreen { background: rgba(76,175,80,.14); color: #2e7d32; }
.geoAmber { background: rgba(245,158,11,.14); color: #ed6c02; }
.geoRed { background: rgba(239,68,68,.14); color: #d32f2f; }
.geoNeutral { background: rgba(96,125,139,.12); color: #546e7a; }
.panel { border-radius: 16px; border: 1px solid rgba(38,50,56,0.08); background: rgba(248,249,251,0.95); padding: 14px; width: 100%; max-width: 100%; overflow-x: hidden; }
/* E008_TAB_VISIBILITY_FIX: tabsBar no debe ocultar el boton Resumen; scroll horizontal permitido. */
.card, .captureBlock, .hero { width: 100%; max-width: 100%; overflow-x: hidden; }
.fieldLabel { margin-bottom: 6px; display: block; font-size: 13px; color: #546e7a; }
.inputLike { width: 100%; border-radius: 12px; border: 1px solid rgba(38,50,56,0.10); background: rgba(255,255,255,0.96); color: #263238; padding: 11px 12px; }
.contextHint { margin-top: 8px; font-size: 12px; color: #607d8b; }
.primaryBtn, .secondaryBtn, .fileBtn { margin-top: 10px; width: 100%; max-width: 100%; border: 0; border-radius: 14px; padding: 13px 14px; display: flex; justify-content: flex-start; align-items: center; gap: 8px; font-weight: 800; cursor: pointer; text-decoration: none; flex-wrap: wrap; min-width: 0; text-align: left; line-height: 1.15; overflow-wrap: anywhere; word-break: break-word; }
.primaryBtn svg, .secondaryBtn svg, .fileBtn svg { flex: 0 0 auto; }
.primaryBtn { background: #4caf50; color: white; }
.secondaryBtn, .fileBtn { background: #eceff1; color: #37474f; }
.primaryBtn:disabled, .secondaryBtn:disabled, .inputLike:disabled { opacity: 0.7; cursor: not-allowed; }
.compactBtn { margin-top: 0; padding: 11px 12px; min-height: 48px; }
.assistQuickBtn { justify-content: flex-start; text-align: left; padding: 12px 14px; }
.wideFileBtn { margin-top: 12px; }
.emptyBox { padding: 12px; border-radius: 12px; background: rgba(96,125,139,0.08); color: #607d8b; font-size: 13px; }
.captureBlock { margin-top: 12px; border-radius: 14px; background: rgba(255,255,255,0.86); border: 1px solid rgba(38,50,56,0.08); padding: 12px; }
.captureTitle { font-size: 13px; font-weight: 800; color: #37474f; margin-bottom: 8px; }
.captureGrid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; width: 100%; min-width: 0; }
.captureGrid.threeCols { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.captureGrid > * { min-width: 0; }
.captureStack { display: flex; flex-direction: column; gap: 8px; width: 100%; min-width: 0; }
.captureStack > * { min-width: 0; }
.captureMeta { margin-top: 8px; font-size: 12px; color: #607d8b; }
.thumbRow, .thumbGrid { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.thumb { width: 66px; height: 66px; object-fit: cover; border-radius: 10px; border: 1px solid rgba(38,50,56,0.12); }
.actionGrid, .summaryGrid { margin-top: 14px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.actionGridButtons { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.summaryBlock { border-radius: 16px; padding: 14px; background: rgba(248,249,251,0.95); border: 1px solid rgba(38,50,56,0.08); }
.kpiBlock { display: flex; flex-direction: column; gap: 6px; align-items: flex-start; }
.kpiValue { font-size: 28px; font-weight: 900; color: #263238; }
.kpiLabel { font-size: 12px; color: #607d8b; font-weight: 700; }
.summaryLine { color: #455a64; font-size: 13px; margin-top: 8px; }
.summaryGeo { margin-top: 4px; color: #607d8b; font-size: 12px; }
.previewFrame { aspect-ratio: 4 / 3; overflow: hidden; border-radius: 14px; background: #dfe5e8; margin-bottom: 10px; }
.actionButton { border: 0; border-radius: 12px; background: rgba(96,125,139,0.12); color: #37474f; font-weight: 700; padding: 10px 12px; display: inline-flex; align-items: center; justify-content: center; gap: 8px; cursor: pointer; }
.galleryScroll { max-width: 100%; overflow-x: auto; overflow-y: hidden; padding: 8px 8px 10px 8px; scrollbar-width: auto; scrollbar-color: rgba(76,175,80,.58) rgba(76,175,80,.12); border: 1px solid rgba(76,175,80,.16); border-radius: 14px; background: rgba(255,255,255,0.72); }
.galleryScroll::-webkit-scrollbar { height: 8px; }
.galleryScroll::-webkit-scrollbar-thumb { background: rgba(76,175,80,.52); border-radius: 999px; }
.galleryScroll::-webkit-scrollbar-track { background: rgba(76,175,80,.10); border-radius: 999px; }
.compactGalleryScroll { max-width: 100%; }
.galleryGrid { margin-top: 6px; display: flex; flex-wrap: nowrap; gap: 12px; width: max-content; min-width: 100%; align-items: stretch; }
.attendanceGalleryGrid { display: flex; flex-wrap: nowrap; }
.attendanceGalleryBlock { margin-top: 8px; }
.galleryCard { flex: 0 0 240px; border-radius: 18px; border: 1px solid rgba(38,50,56,0.08); background: rgba(255,255,255,0.96); padding: 12px; }
.galleryCardCompact { display: grid; grid-template-columns: 72px 1fr; gap: 10px; align-items: start; min-width: 280px; }
.galleryBodyCompact { min-width: 0; }
.imageFrame { aspect-ratio: 4 / 3; overflow: hidden; border-radius: 14px; background: #dfe5e8; }
.imageFrameCompact { width: 72px; height: 72px; aspect-ratio: auto; }
.img { width: 100%; height: 100%; object-fit: cover; display: block; }
.galleryTop { margin-top: 10px; display: flex; justify-content: space-between; gap: 8px; align-items: center; }
.compactTop { margin-top: 0; }
.galleryTitle { font-weight: 800; color: #263238; font-size: 13px; }
.gallerySub { margin-top: 4px; color: #607d8b; font-size: 13px; }
.compactMeta { line-height: 1.2; }
.galleryDate { margin-top: 4px; color: #78909c; font-size: 12px; }
.galleryDesc { margin-top: 8px; color: #455a64; font-size: 13px; line-height: 1.45; }
.compactDesc { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
.riskBadge { border-radius: 999px; padding: 6px 10px; font-size: 11px; font-weight: 800; }
.riskRed { background: rgba(239,68,68,.14); color: #d32f2f; }
.riskAmber { background: rgba(245,158,11,.14); color: #ed6c02; }
.riskGreen { background: rgba(76,175,80,.14); color: #2e7d32; }
.riskNeutral { background: rgba(96,125,139,.14); color: #546e7a; }
.filtersRow { margin-top: 12px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
.filtersStickyCard { margin-top: 10px; padding: 12px; border-radius: 16px; background: rgba(248,249,251,0.95); border: 1px solid rgba(38,50,56,0.08); }
.filtersRowSupervisorBottom { grid-template-columns: repeat(4, minmax(0, 1fr)); }
.toggleCard { min-height: 48px; border-radius: 12px; border: 1px solid rgba(38,50,56,0.10); background: rgba(255,255,255,0.96); padding: 10px 12px; display: inline-flex; align-items: center; gap: 10px; color: #37474f; font-weight: 700; }
.supervisorSummaryGrid { grid-template-columns: repeat(5, minmax(0, 1fr)); }
.brandGroupList { margin-top: 14px; display: flex; flex-direction: column; gap: 14px; }
.brandGroupCard { border-radius: 18px; border: 1px solid rgba(38,50,56,0.08); background: rgba(255,255,255,0.96); padding: 14px; }
.brandGroupHeader { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; flex-wrap: wrap; }
.brandGroupTitle { font-size: 16px; font-weight: 900; color: #263238; }
.brandGroupCounters { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 8px; }
.brandGroupActions { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
.reviewRailCardWide { flex-basis: 220px; }
.reviewRailBodyWide { display: flex; flex-direction: column; gap: 6px; }
.reviewRailBadges { margin-top: 6px; display: flex; flex-wrap: wrap; gap: 6px; }
.reviewRailDesc { min-height: 32px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
.quickActionRow { margin-top: 8px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
.overlayBackdropRich { padding: 12px; }
.viewerChrome { position: fixed; left: 12px; right: 12px; z-index: 92; max-width: 1040px; margin: 0 auto; border-radius: 16px; background: rgba(15,23,42,0.86); border: 1px solid rgba(255,255,255,0.10); backdrop-filter: blur(12px); padding: 12px; color: white; display: flex; gap: 12px; justify-content: space-between; align-items: center; }
.viewerChromeTop { top: 10px; }
.viewerChromeBottom { bottom: 10px; flex-direction: column; align-items: stretch; }
.viewerTitle { font-size: 14px; font-weight: 900; }
.viewerMeta, .viewerFooterMeta { font-size: 12px; color: rgba(255,255,255,0.72); }
.viewerTopBadges { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; justify-content: flex-end; }
.viewerActionRow { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 8px; }
.viewerInput { margin-top: 8px; }
.twoColsFilters { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.statusBar { position: fixed; left: 50%; transform: translateX(-50%); bottom: 12px; z-index: 60; width: calc(100% - 24px); max-width: 760px; border-radius: 16px; padding: 12px 14px; background: rgba(232,245,233,0.98); color: #2e7d32; border: 1px solid rgba(76,175,80,0.20); font-weight: 700; box-shadow: 0 12px 28px rgba(38,50,56,0.16); }
.footerActions { margin-top: 12px; margin-bottom: 74px; display: flex; justify-content: flex-end; }
.footerBtn { width: auto; min-width: 160px; }
.fullSpan { grid-column: 1 / -1; }
.traceBox { margin-top: 10px; border-radius: 12px; padding: 10px 12px; background: rgba(96,125,139,0.08); border: 1px solid rgba(38,50,56,0.08); }
.selectionToolbar { margin-top: 12px; display: flex; justify-content: space-between; gap: 10px; align-items: center; flex-wrap: wrap; }
.selectionToolbarLeft { display: inline-flex; gap: 6px; align-items: center; color: #455a64; }
.selectionToolbarActions { display: inline-flex; gap: 8px; flex-wrap: wrap; }
.railScrollFrame { margin-top: 14px; border: 1px solid rgba(76,175,80,.16); border-radius: 14px; background: rgba(255,255,255,0.72); padding: 8px; }
.reviewRail { display: flex; gap: 10px; overflow-x: auto; padding-bottom: 6px; scrollbar-width: auto; scrollbar-color: rgba(76,175,80,.58) rgba(76,175,80,.12); }
.reviewRail::-webkit-scrollbar { height: 8px; }
.reviewRail::-webkit-scrollbar-thumb { background: rgba(76,175,80,.52); border-radius: 999px; }
.reviewRail::-webkit-scrollbar-track { background: rgba(76,175,80,.10); border-radius: 999px; }
.reviewRailCard { flex: 0 0 152px; border-radius: 16px; border: 2px solid rgba(38,50,56,0.08); background: rgba(255,255,255,0.96); overflow: hidden; cursor: pointer; transition: transform .12s ease, box-shadow .12s ease, border-color .12s ease; padding: 0; text-align: left; }
.reviewRailCard:hover { transform: translateY(-1px); box-shadow: 0 10px 18px rgba(38,50,56,0.10); }
.reviewRailCardSelected { border-color: rgba(76,175,80,.65); box-shadow: 0 12px 20px rgba(76,175,80,.12); }
.reviewRailMedia { position: relative; aspect-ratio: 4 / 3; overflow: hidden; background: #dfe5e8; }
.reviewRailBody { padding: 8px 10px 10px; }
.reviewRailTitle { font-weight: 800; color: #263238; font-size: 12px; line-height: 1.2; }
.reviewRailMeta { margin-top: 4px; color: #607d8b; font-size: 11px; line-height: 1.2; }
.selectionPill { position: absolute; right: 10px; top: 10px; width: 28px; height: 28px; border-radius: 999px; background: rgba(255,255,255,0.92); color: #546e7a; display: grid; place-items: center; font-weight: 900; border: 1px solid rgba(38,50,56,0.14); }
.selectionPillActive { background: #4caf50; color: white; border-color: rgba(76,175,80,.65); }
.detailSubcard { margin-top: 16px; }
.traceTitle { font-size: 12px; font-weight: 800; color: #455a64; margin-bottom: 4px; }
.removeThumbBtn { position: absolute; right: -4px; top: -4px; width: 22px; height: 22px; border-radius: 999px; border: 0; background: rgba(211,47,47,0.95); color: white; font-weight: 900; cursor: pointer; }
.authTraceBox { margin-top: 8px; padding: 9px 11px; border-radius: 12px; background: rgba(76,175,80,0.08); border: 1px solid rgba(76,175,80,0.18); color: #2f4f37; font-size: 11px; line-height: 1.3; white-space: normal; overflow-wrap: anywhere; word-break: break-word; max-width: 100%; }
.mainActionBtn { width: 100%; max-width: 100%; box-sizing: border-box; padding: 12px 14px; white-space: normal; line-height: 1.15; min-height: 56px; display: flex; flex-direction: column; align-items: stretch; justify-content: center; text-align: left; gap: 4px; overflow: hidden; }
.outOfServiceBox { margin-top: 10px; padding: 12px; border: 1px solid rgba(245, 158, 11, .35); border-radius: 16px; background: rgba(255, 247, 237, .78); display: grid; gap: 8px; }
.outOfServiceTitle { font-weight: 900; color: #7c2d12; font-size: .92rem; }
.outOfServiceText { color: #7c2d12; font-size: .84rem; line-height: 1.35; }
.outOfServiceBtn { width: 100%; justify-content: flex-start; margin-top: 2px; border-color: rgba(245, 158, 11, .5); background: rgba(255, 255, 255, .75); }
.mainActionTop { display: inline-flex; align-items: center; justify-content: flex-start; gap: 8px; flex-wrap: wrap; width: 100%; max-width: 100%; min-width: 0; text-align: left; }
.mainActionTop > span:last-child { min-width: 0; max-width: 100%; overflow-wrap: anywhere; word-break: break-word; }
/* E014F: Registrar evidencia icono + texto en una sola linea y alineado a la izquierda */
.e014dEvidenceActionBtn { flex-direction: row !important; align-items: center !important; justify-content: flex-start !important; text-align: left !important; gap: 8px !important; }
.e014dEvidenceActionBtn .e014fEvidenceActionTop { width: 100% !important; justify-content: flex-start !important; flex-wrap: nowrap !important; gap: 8px !important; }
.e014dEvidenceActionBtn .e014fEvidenceActionTop svg { flex: 0 0 auto !important; }
.e014dEvidenceActionBtn .e014fEvidenceActionTop > span:last-child { white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; word-break: normal !important; overflow-wrap: normal !important; }
.mainActionSub { display: block; width: 100%; max-width: 100%; font-size: 11px; font-weight: 700; opacity: 0.96; overflow-wrap: anywhere; word-break: break-word; padding: 0; text-align: left; }
.entryActionBtn { background: #4caf50; color: white; }
.dangerBtn { background: #d32f2f !important; color: white !important; }
.overlayBackdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.86); z-index: 90; display: grid; place-items: center; padding: 10px; touch-action: none; overflow: hidden; }
.cameraBackdrop { align-content: center; }
.overlayImage { max-width: 100%; max-height: 100%; object-fit: contain; border-radius: 10px; transition: transform .12s ease; touch-action: none; }
.cameraModal { width: min(calc(100vw - 20px), 320px); max-width: calc(100vw - 20px); max-height: calc(100vh - 90px); background: #111; border-radius: 16px; padding: 10px; display: flex; flex-direction: column; gap: 8px; overflow: hidden; box-sizing: border-box; margin: 0 auto; }
.cameraModalTight { width: min(calc(100vw - 20px), 320px); max-width: calc(100vw - 20px); }
.cameraViewport { width: 100%; border-radius: 12px; overflow: hidden; background: #000; aspect-ratio: 1 / 1; max-height: min(38vh, 300px); }
.cameraViewportTight { aspect-ratio: 3 / 4; max-height: min(46vh, 420px); }
.cameraVideo { width: 100%; height: 100%; min-height: 0; max-height: min(38vh, 300px); border-radius: 12px; background: #000; object-fit: cover; display: block; }
.cameraVideoTight { max-height: min(46vh, 420px); }
.cameraHint { color: rgba(255,255,255,0.74); font-size: 11px; text-align: center; }
.cameraActionRow { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.cameraActionRowTight { margin-top: 2px; }
.cameraCaptureBtn, .cameraCancelBtn { border: 0; border-radius: 14px; min-height: 50px; font-weight: 800; display: inline-flex; align-items: center; justify-content: center; gap: 8px; cursor: pointer; flex-wrap: wrap; }
.cameraCaptureBtnTight, .cameraCancelBtnTight { min-height: 52px; }
.cameraCaptureBtn { background: #4caf50; color: white; }
.cameraCancelBtn { background: #eceff1; color: #37474f; }

/* E020C_CAMERA_MEJORADA_PROMOTOR */
.e020cCaptureGuardStrong {
  border-width: 2px;
  background: #fff7ed;
  box-shadow: 0 10px 24px rgba(245, 158, 11, 0.18);
}
.e020cGuardToast {
  margin: 10px 0;
  border-radius: 16px;
  padding: 12px 14px;
  border: 2px solid rgba(245, 158, 11, .55);
  background: #fff7ed;
  color: #7c2d12;
  font-weight: 900;
  line-height: 1.35;
}
.cameraModalEvidence {
  width: min(calc(100vw - 12px), 760px);
  max-height: calc(100vh - 28px);
}
.cameraViewportEvidence {
  aspect-ratio: 16 / 9;
  max-height: min(62vh, 520px);
}
.cameraVideoEvidence {
  object-fit: contain;
  background: #000;
  max-height: min(62vh, 520px);
}
.e020cCameraHint {
  font-size: 12px;
  line-height: 1.35;
}
.e020cZoomBox {
  display: grid;
  grid-template-columns: auto 1fr auto;
  gap: 10px;
  align-items: center;
  padding: 8px 10px;
  border-radius: 14px;
  background: rgba(255,255,255,0.08);
  color: #fff;
}
.e020cZoomLabel, .e020cZoomValue { font-weight: 900; font-size: 12px; }
.e020cZoomRange { width: 100%; }
.e020cZoomUnavailable { color: rgba(255,255,255,0.64); }
@media (max-width: 760px) {
  .cameraModalEvidence { width: calc(100vw - 8px); padding: 8px; }
  .cameraViewportEvidence { max-height: min(58vh, 460px); }
  .cameraVideoEvidence { max-height: min(58vh, 460px); }
}


	/* E016_SUPERVISOR_FILTROS_COMENTAR_REDESIGN */
	.e016SupervisorSummaryCard { padding: 16px; }
	.e016SupervisorHero { margin-top: 14px; }
	.e016SupervisorMetricGrid { margin-top: 12px; }
	.e016InfoGrid { margin-top: 12px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
	.e016InfoPanel { border-radius: 16px; padding: 12px; background: linear-gradient(180deg, rgba(255,255,255,.96), rgba(248,250,252,.92)); border: 1px solid rgba(38,50,56,.08); box-shadow: inset 0 1px 0 rgba(255,255,255,.72); }
	.e016PanelTitle { font-size: 12px; font-weight: 900; color: #263238; margin-bottom: 8px; letter-spacing: .01em; }
	.e016InfoLine { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; padding: 7px 0; border-top: 1px solid rgba(38,50,56,.06); color: #607d8b; font-size: 12px; text-align: left; }
	.e016InfoLine:first-of-type { border-top: 0; }
	.e016InfoLine strong { color: #263238; text-align: right; overflow-wrap: anywhere; }
	.e016DetailHero { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; padding: 12px; border-radius: 16px; background: linear-gradient(135deg, rgba(232,245,233,.92), rgba(255,255,255,.96)); border: 1px solid rgba(76,175,80,.18); text-align: left; }
	.e016DetailEyebrow { font-size: 10px; font-weight: 900; color: #2e7d32; text-transform: uppercase; letter-spacing: .08em; }
	.e016DetailTitle { margin-top: 3px; font-size: 16px; line-height: 1.2; font-weight: 900; color: #263238; }
	.e016DetailSub { margin-top: 5px; color: #607d8b; font-size: 12px; font-weight: 700; }
	.e016MiniMetricGrid { margin-top: 10px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
	.e016MiniMetricGrid > div { border-radius: 14px; padding: 10px; background: rgba(255,255,255,.96); border: 1px solid rgba(38,50,56,.08); text-align: left; }
	.e016MiniMetricGrid span { display: block; color: #607d8b; font-size: 11px; font-weight: 800; }
	.e016MiniMetricGrid strong { display: block; margin-top: 4px; color: #263238; font-size: 22px; font-weight: 900; }
	.e016ActivityPanel { margin-top: 10px; }
	.e016ActionGrid { margin-top: 10px; }
	.e016BadgeStack { display: flex; flex-direction: column; gap: 6px; align-items: flex-end; }
	.e016DescriptionBox { margin: 10px 0; padding: 11px 12px; border-radius: 14px; background: rgba(239,246,255,.76); border: 1px solid rgba(96,125,139,.12); color: #455a64; font-size: 13px; line-height: 1.35; text-align: left; }
	.e016DateFilterBar { margin-top: 12px; display: flex; gap: 8px; align-items: center; flex-wrap: nowrap; overflow-x: auto; padding-bottom: 2px; }
	.e016DateChip { flex: 0 0 auto; border: 1px solid rgba(38,50,56,.08); background: rgba(255,255,255,.96); color: #455a64; border-radius: 999px; padding: 9px 13px; font-weight: 900; cursor: pointer; }
	.e016DateChipActive { background: #4caf50; color: #fff; border-color: rgba(76,175,80,.58); box-shadow: 0 8px 18px rgba(76,175,80,.18); }
	.e016DateLabel { flex: 0 0 auto; color: #607d8b; font-size: 12px; font-weight: 800; padding: 0 4px; }
	.e016RangeRow { margin-top: 8px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }

@media (max-width: 900px) { .twoCol, .actionGrid, .summaryGrid, .actionGridButtons, .captureGrid, .captureGrid.threeCols, .filtersRow, .twoColsFilters, .quickActionRow, .viewerActionRow, .supervisorSummaryGrid, .e016InfoGrid, .e016RangeRow { grid-template-columns: 1fr; } .reviewRailCard { flex-basis: 136px; } .reviewRailCardWide { flex-basis: 180px; } .galleryCard { flex-basis: 220px; } .galleryCardCompact { min-width: 240px; } .viewerChrome { left: 8px; right: 8px; } }
@media (max-width: 760px) { .heroTitleBlockWide { width: min(220px, 58%); min-width: 168px; } .heroMetaSingleWide { max-width: 190px; } .cameraModal, .cameraModalTight { width: calc(100vw - 16px); max-height: calc(100vh - 64px); padding: 10px; } .cameraViewport { max-height: min(42vh, 320px); } .cameraViewportTight { max-height: min(48vh, 440px); } .cameraVideo { min-height: 0; max-height: min(42vh, 320px); } .cameraVideoTight { max-height: min(48vh, 440px); } .cameraActionRow, .cameraActionRowTight { grid-template-columns: 1fr 1fr; } .mainActionBtn { min-height: 54px; padding: 12px 12px; } .compactBtn, .assistQuickBtn { padding: 12px 14px; } }

/* E014C_PROMOTOR_SUMMARY_REDESIGN --------------------------------------------
   Rediseño del Resumen del promotor: menos texto centrado, más jerarquía,
   lectura operativa móvil y tarjetas alineadas a la izquierda.
*/
.e014cSummaryCard { overflow: hidden; }
.e014cSummaryHeader { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
.e014cSummaryTitle { margin: 0; }
.e014cSummarySub { margin-top: 4px; color: var(--e010-muted, #64748b); font-size: 13px; line-height: 1.3; }
.e014cStatusPill { flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 7px 10px; font-size: 11px; font-weight: 900; white-space: nowrap; }
.e014cPillActive { background: rgba(22, 163, 74, 0.12); color: #047857; border: 1px solid rgba(22, 163, 74, 0.18); }
.e014cPillNeutral { background: rgba(100, 116, 139, 0.10); color: #475569; border: 1px solid rgba(100, 116, 139, 0.14); }
.e014cHeroMetric { display: flex; justify-content: space-between; align-items: stretch; gap: 14px; border-radius: 26px; padding: 16px; background: linear-gradient(135deg, rgba(15,23,42,0.96), rgba(22,101,52,0.92)); color: #fff; box-shadow: 0 20px 48px rgba(15, 23, 42, 0.16); }
.e014cHeroCopy { display: flex; flex-direction: column; gap: 4px; min-width: 0; text-align: left; }
.e014cHeroCopy strong { font-size: 23px; line-height: 1.05; letter-spacing: -0.045em; }
.e014cHeroCopy small, .e014cHeroNumbers small { color: rgba(255,255,255,0.74); font-size: 12px; }
.e014cHeroNumbers { min-width: 96px; border-radius: 22px; background: rgba(255,255,255,0.10); border: 1px solid rgba(255,255,255,0.14); display: flex; flex-direction: column; align-items: flex-start; justify-content: center; padding: 12px; text-align: left; }
.e014cHeroNumbers span { font-size: 34px; line-height: 1; font-weight: 950; letter-spacing: -0.06em; }
.e014cEyebrow { display: block; color: var(--e010-muted, #64748b); font-size: 10px; font-weight: 950; letter-spacing: .08em; text-transform: uppercase; margin-bottom: 3px; }
.e014cHeroMetric .e014cEyebrow { color: rgba(255,255,255,0.58); }
.e014cMetricGrid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-top: 12px; }
.e014cMetricCard { display: flex; align-items: center; gap: 10px; min-width: 0; border-radius: 20px; padding: 12px; background: rgba(255,255,255,0.86); border: 1px solid rgba(15,23,42,0.07); box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05); text-align: left; }
.e014cMetricIcon { width: 34px; height: 34px; border-radius: 14px; display: grid; place-items: center; flex: 0 0 auto; background: rgba(22,163,74,0.10); color: #047857; }
.e014cMetricBody { min-width: 0; display: flex; flex-direction: column; align-items: flex-start; gap: 2px; }
.e014cMetricBody span { font-size: 11px; color: var(--e010-muted, #64748b); font-weight: 800; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }
.e014cMetricBody strong { color: var(--e010-ink, #0f172a); font-size: 22px; line-height: 1; letter-spacing: -0.05em; }
.e014cSummaryGrid { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, .85fr); gap: 12px; margin-top: 14px; align-items: start; }
.e014cPanel { border-radius: 24px; padding: 14px; background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%); border: 1px solid rgba(15,23,42,0.07); box-shadow: 0 12px 30px rgba(15, 23, 42, 0.05); text-align: left; min-width: 0; }
.e014cPanelWide { grid-row: span 2; }
.e014cPanelHead { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; margin-bottom: 10px; }
.e014cPanelHead strong { display: block; color: var(--e010-ink, #0f172a); font-size: 15px; line-height: 1.15; }
.e014cCountBadge { display: inline-flex; min-width: 30px; height: 30px; padding: 0 9px; border-radius: 999px; align-items: center; justify-content: center; color: #047857; background: rgba(22,163,74,0.10); border: 1px solid rgba(22,163,74,0.16); font-size: 12px; font-weight: 950; }
.e014cBadgeWarn { color: #b45309; background: rgba(245,158,11,0.12); border-color: rgba(245,158,11,0.20); }
.e014cTimeline { display: flex; flex-direction: column; gap: 10px; }
.e014cTimelineItem { display: grid; grid-template-columns: 32px minmax(0, 1fr); gap: 10px; padding: 11px; border-radius: 18px; background: rgba(248,250,252,0.90); border: 1px solid rgba(15,23,42,0.06); }
.e014cDot { width: 32px; height: 32px; border-radius: 999px; display: grid; place-items: center; background: rgba(22,163,74,0.10); color: #047857; }
.e014cTimelineItem strong, .e014cListItem strong { display: block; color: var(--e010-ink, #0f172a); font-size: 13px; line-height: 1.25; overflow-wrap: anywhere; }
.e014cTimelineItem span, .e014cListItem span { display: block; color: #475569; font-size: 12px; margin-top: 3px; line-height: 1.25; }
.e014cTimelineItem small, .e014cListItem small { display: block; color: var(--e010-muted, #64748b); font-size: 11px; margin-top: 3px; line-height: 1.25; overflow-wrap: anywhere; }
.e014cKeyRows { display: flex; flex-direction: column; gap: 7px; }
.e014cKeyRows > div { display: flex; justify-content: space-between; gap: 10px; align-items: center; padding: 8px 0; border-bottom: 1px solid rgba(15,23,42,0.06); }
.e014cKeyRows > div:last-child { border-bottom: 0; }
.e014cKeyRows span { color: var(--e010-muted, #64748b); font-size: 12px; font-weight: 750; }
.e014cKeyRows strong { color: var(--e010-ink, #0f172a); font-size: 13px; text-align: right; }
.e014cRetryBtn { margin-top: 12px !important; }
.e014cCompactList { display: flex; flex-direction: column; gap: 8px; margin-top: 10px; }
.e014cListItem { padding: 10px; border-radius: 16px; background: rgba(248,250,252,0.92); border: 1px solid rgba(15,23,42,0.06); }
.e014cNote, .e014cEmptyLine { margin-top: 10px; padding: 10px; border-radius: 16px; background: rgba(100,116,139,0.08); color: #64748b; font-size: 12px; line-height: 1.35; text-align: left; }
@media (max-width: 900px) {
  .e014cMetricGrid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .e014cSummaryGrid { grid-template-columns: 1fr; }
  .e014cPanelWide { grid-row: auto; }
}
@media (max-width: 430px) {
  .e014cSummaryHeader { flex-direction: column; align-items: flex-start; }
  .e014cHeroMetric { padding: 14px; gap: 10px; }
  .e014cHeroCopy strong { font-size: 20px; }
  .e014cHeroNumbers { min-width: 82px; padding: 10px; }
  .e014cHeroNumbers span { font-size: 30px; }
  .e014cMetricGrid { grid-template-columns: 1fr; }
}


/* E010_UX_ACTION_FIRST_SUPERVISOR -------------------------------------------------
   Refresh visual para Promobolsillo: más actual, móvil y orientado a acción.
   Mantiene la funcionalidad E009B; cambia jerarquía visual, tabs, tarjetas y bandeja supervisor.
*/
:root {
  --e010-bg: #f7f8fb;
  --e010-ink: #0f172a;
  --e010-muted: #64748b;
  --e010-line: rgba(15, 23, 42, 0.08);
  --e010-green: #16a34a;
  --e010-green-dark: #047857;
  --e010-purple: #6d28d9;
  --e010-orange: #f97316;
  --e010-red: #ef4444;
  --e010-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
}
body {
  background:
    radial-gradient(circle at 16% 4%, rgba(34, 197, 94, 0.12), transparent 30%),
    radial-gradient(circle at 92% 0%, rgba(109, 40, 217, 0.10), transparent 28%),
    var(--e010-bg);
}
.shell { max-width: 1240px; }
.stickyTop {
  background: rgba(247, 248, 251, 0.84);
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
  border-bottom: 1px solid rgba(15, 23, 42, 0.06);
  padding-top: 6px;
}
.hero {
  background: rgba(255, 255, 255, 0.86);
  border: 1px solid rgba(15, 23, 42, 0.06);
  border-radius: 28px;
  box-shadow: var(--e010-shadow);
}
.heroTitle, .heroTitleTight {
  letter-spacing: -0.04em;
  color: var(--e010-ink);
  font-weight: 900;
}
.brandWord { color: var(--e010-green); }
.tabsBar {
  display: flex;
  gap: 8px;
  align-items: center;
  background: rgba(255, 255, 255, 0.76);
  border: 1px solid rgba(15, 23, 42, 0.07);
  border-radius: 999px;
  padding: 6px;
  box-shadow: 0 10px 26px rgba(15, 23, 42, 0.06);
  overflow-x: auto;
  scrollbar-width: none;
}
.tabsBar::-webkit-scrollbar { display: none; }
.tabBtn {
  border: 0;
  border-radius: 999px;
  background: transparent;
  color: var(--e010-muted);
  font-weight: 800;
  padding: 11px 16px;
  white-space: nowrap;
  transition: all 160ms ease;
}
.tabBtnActive {
  background: linear-gradient(135deg, var(--e010-green) 0%, var(--e010-green-dark) 100%);
  color: #fff;
  box-shadow: 0 10px 22px rgba(22, 163, 74, 0.24);
}
.card, .panel, .detailSubcard, .filtersStickyCard, .traceBox, .emptyBox {
  border-radius: 28px;
  border: 1px solid rgba(15, 23, 42, 0.07);
  background: rgba(255, 255, 255, 0.88);
  box-shadow: var(--e010-shadow);
}
.panel { box-shadow: 0 10px 28px rgba(15, 23, 42, 0.05); }
.sectionTitle {
  color: var(--e010-ink);
  font-size: 20px;
  font-weight: 900;
  letter-spacing: -0.03em;
}
.contextHint, .helperText, .summaryLine, .listSub, .reviewRailMeta, .kpiLabel {
  color: var(--e010-muted);
}
.e010PageTitle { font-size: 24px; margin-bottom: 6px; }
.e010PageSub { max-width: 820px; margin-bottom: 12px; }
.e010FlowSteps {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 10px;
  margin: 12px 0 14px;
}
.e010FlowSteps > div {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  border-radius: 18px;
  background: linear-gradient(135deg, rgba(109,40,217,0.09), rgba(34,197,94,0.07));
  border: 1px solid rgba(109, 40, 217, 0.10);
  color: var(--e010-ink);
  font-weight: 800;
  font-size: 12px;
}
.e010FlowSteps strong {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border-radius: 999px;
  color: #fff;
  background: var(--e010-purple);
  flex: 0 0 auto;
}
.inputLike, select.inputLike, input.inputLike {
  border-radius: 16px;
  border: 1px solid rgba(15, 23, 42, 0.10);
  background: rgba(255, 255, 255, 0.92);
  min-height: 42px;
  color: var(--e010-ink);
}
.summaryGrid { gap: 12px; }
.summaryBlock, .kpiBlock {
  border-radius: 24px;
  background: linear-gradient(180deg, #ffffff 0%, #f9fafb 100%);
  border: 1px solid rgba(15, 23, 42, 0.07);
  box-shadow: 0 12px 30px rgba(15, 23, 42, 0.05);
}
.kpiValue {
  color: var(--e010-ink);
  font-size: 26px;
  letter-spacing: -0.04em;
}
.filtersStickyCard {
  position: sticky;
  top: 112px;
  z-index: 9;
  padding: 12px;
  margin-bottom: 14px;
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
}
.selectionToolbar {
  border-radius: 20px;
  background: linear-gradient(135deg, rgba(109, 40, 217, 0.08), rgba(22, 163, 74, 0.06));
  border: 1px solid rgba(109, 40, 217, 0.12);
}
.actionButton, .primaryBtn, .secondaryBtn, .mainActionBtn, .compactBtn {
  border-radius: 18px;
  border: 1px solid rgba(15, 23, 42, 0.08);
  font-weight: 850;
  transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
}
.actionButton:active, .primaryBtn:active, .secondaryBtn:active, .mainActionBtn:active { transform: translateY(1px) scale(0.99); }
.mainActionBtn, .primaryBtn {
  background: linear-gradient(135deg, var(--e010-green) 0%, var(--e010-green-dark) 100%);
  color: #fff;
  box-shadow: 0 14px 30px rgba(22, 163, 74, 0.25);
}
.brandGroupCard {
  border-radius: 30px;
  background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(248,250,252,0.96) 100%);
  border: 1px solid rgba(109, 40, 217, 0.10);
  box-shadow: var(--e010-shadow);
  overflow: hidden;
}
.brandGroupHeader {
  padding: 16px;
  background: linear-gradient(135deg, rgba(109, 40, 217, 0.08), rgba(22, 163, 74, 0.04));
  border-bottom: 1px solid rgba(15, 23, 42, 0.06);
}
.brandGroupTitle {
  font-size: 18px;
  font-weight: 900;
  color: var(--e010-ink);
  letter-spacing: -0.03em;
}
.railScrollFrame { padding: 14px 16px 18px; }
.reviewRail { gap: 14px; }
.reviewRailCard, .reviewRailCardWide {
  min-width: 260px;
  max-width: 280px;
  border-radius: 26px;
  background: #fff;
  border: 1px solid rgba(15, 23, 42, 0.08);
  box-shadow: 0 14px 30px rgba(15, 23, 42, 0.07);
  overflow: hidden;
}
.reviewRailCardSelected {
  outline: 3px solid rgba(109, 40, 217, 0.28);
  box-shadow: 0 18px 42px rgba(109, 40, 217, 0.16);
}
.reviewRailMedia {
  height: 158px;
  border-radius: 22px;
  margin: 10px 10px 0;
  overflow: hidden;
  background: #0f172a;
}
.reviewRailBody { padding: 12px; }
.reviewRailTitle {
  font-size: 15px;
  font-weight: 900;
  letter-spacing: -0.02em;
  color: var(--e010-ink);
}
.riskBadge {
  border-radius: 999px;
  padding: 5px 9px;
  font-weight: 850;
  letter-spacing: -0.01em;
}
.quickActionRow {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin-top: 10px;
}
.quickActionRow .actionButton:nth-child(1) { background: #ecfdf5; color: #166534; border-color: rgba(22, 163, 74, 0.18); }
.quickActionRow .actionButton:nth-child(2) { background: #fff7ed; color: #9a3412; border-color: rgba(249, 115, 22, 0.20); }
.quickActionRow .actionButton:nth-child(3) { background: #fef2f2; color: #991b1b; border-color: rgba(239, 68, 68, 0.20); }
.quickActionRow .actionButton:nth-child(4) { background: #f8fafc; color: #334155; border-color: rgba(15, 23, 42, 0.10); }
.detailSubcard {
  margin-top: 16px;
  background: linear-gradient(135deg, rgba(255,255,255,0.98), rgba(245,243,255,0.86));
  border-color: rgba(109, 40, 217, 0.16);
}
.previewFrame, .imageFrame, .galleryCard {
  border-radius: 24px;
  overflow: hidden;
}
.galleryCard {
  border: 1px solid rgba(15, 23, 42, 0.08);
  background: #fff;
  box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
}
.footerActions {
  background: rgba(255,255,255,0.86);
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
  border-top: 1px solid rgba(15, 23, 42, 0.06);
}
.footerBtn {
  border-radius: 18px;
  font-weight: 800;
}
@media (max-width: 820px) {
  .shell { max-width: 100%; }
  .hero { border-radius: 24px; }
  .card, .panel, .detailSubcard, .filtersStickyCard { border-radius: 24px; }
  .e010FlowSteps { grid-template-columns: 1fr; }
  .e010FlowSteps > div { justify-content: flex-start; }
  .filtersStickyCard { top: 98px; }
  .reviewRailCard, .reviewRailCardWide { min-width: 78vw; max-width: 78vw; }
  .reviewRailMedia { height: 190px; }
  .brandGroupActions { width: 100%; overflow-x: auto; padding-bottom: 3px; }
  .quickActionRow { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 480px) {
  .reviewRailCard, .reviewRailCardWide { min-width: 84vw; max-width: 84vw; }
  .reviewRailMedia { height: 172px; }
  .tabBtn { padding: 10px 14px; }
  .kpiValue { font-size: 23px; }
}

/* E011_EVIDENCIAS_AGRUPADAS_SUPERVISOR_CLIENTE -------------------------------
   La galería deja de ser una lista vertical de carretes. Primero hay tarjetas
   de grupo paginadas y luego un workspace de revisión con miniaturas compactas.
*/
.e011GroupedEvidenceHub { overflow: visible; }
.e011ModeRow { margin-top: 14px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.e011ModeLabel { color: var(--e010-muted, #64748b); font-weight: 850; font-size: 12px; margin-right: 4px; }
.e011ModeBtn { border: 1px solid rgba(15,23,42,.08); background: rgba(255,255,255,.86); color: #334155; border-radius: 999px; padding: 9px 12px; font-weight: 850; cursor: pointer; }
.e011ModeBtnActive { background: #0f172a; color: white; box-shadow: 0 10px 24px rgba(15,23,42,.16); }
.e011EvidenceBoardHeader { margin-top: 16px; display: flex; align-items: flex-end; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
.e011Pager { display: inline-flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.e011Pager .actionButton:disabled { opacity: .45; cursor: not-allowed; box-shadow: none; }
.e011GroupBoard { margin-top: 12px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
.e011GroupTile { width: 100%; border: 1px solid rgba(15,23,42,.08); background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,250,252,.96)); border-radius: 24px; padding: 12px; box-shadow: 0 14px 30px rgba(15,23,42,.06); cursor: pointer; text-align: left; transition: transform .12s ease, box-shadow .12s ease, border-color .12s ease; }
.e011GroupTile:hover { transform: translateY(-1px); box-shadow: 0 18px 38px rgba(15,23,42,.10); }
.e011GroupTileActive { border-color: rgba(109,40,217,.42); outline: 3px solid rgba(109,40,217,.14); }
.e011GroupTileTop { display: flex; justify-content: space-between; gap: 8px; align-items: flex-start; }
.e011GroupKind { font-size: 10px; text-transform: uppercase; letter-spacing: .08em; color: #64748b; font-weight: 900; }
.e011GroupTitle { margin-top: 2px; font-size: 15px; line-height: 1.15; font-weight: 950; color: #0f172a; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; min-height: 34px; }
.e011MiniCollage { margin-top: 10px; display: grid; grid-template-columns: repeat(4, 1fr); gap: 5px; height: 54px; overflow: hidden; }
.e011MiniCollage img, .e011EmptyThumb { width: 100%; height: 54px; object-fit: cover; border-radius: 12px; background: #e2e8f0; display: grid; place-items: center; color: #64748b; }
.e011GroupStats { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 6px; }
.e011GroupStats .riskBadge { padding: 4px 7px; font-size: 10px; }
.e011GroupMeta { margin-top: 8px; font-size: 11px; line-height: 1.25; color: #64748b; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; min-height: 27px; }
.e011ReviewWorkspace { margin-top: 16px; border: 1px solid rgba(109,40,217,.12); background: linear-gradient(135deg, rgba(255,255,255,.98), rgba(245,243,255,.82)); border-radius: 28px; padding: 14px; box-shadow: 0 16px 36px rgba(15,23,42,.07); }
.e011WorkspaceHeader { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
.e011WorkspaceActions { justify-content: flex-start; }
.e011ThumbStrip { margin-top: 12px; display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; }
.e011ThumbCard { border: 1px solid rgba(15,23,42,.08); background: white; border-radius: 18px; padding: 8px; box-shadow: 0 8px 20px rgba(15,23,42,.05); cursor: pointer; text-align: left; min-width: 0; }
.e011ThumbCardActive { outline: 3px solid rgba(22,163,74,.18); border-color: rgba(22,163,74,.42); }
.e011ThumbImageWrap { position: relative; height: 82px; border-radius: 14px; overflow: hidden; background: #0f172a; }
.e011ThumbImageWrap img, .e011ThumbCard > img { width: 100%; height: 82px; object-fit: cover; border-radius: 14px; display: block; background: #0f172a; }
.e011ThumbCard .selectionPill { right: 6px; top: 6px; width: 26px; height: 26px; font-size: 13px; }
.e011ThumbInfo { margin-top: 7px; display: grid; gap: 2px; min-width: 0; }
.e011ThumbInfo strong, .e011ThumbInfo span { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.e011ThumbInfo strong { font-size: 12px; color: #0f172a; font-weight: 900; }
.e011ThumbInfo span { font-size: 10.5px; color: #64748b; }
.e011ThumbBadges { margin-top: 7px; display: flex; flex-wrap: wrap; gap: 5px; }
.e011ThumbBadges .riskBadge { padding: 4px 6px; font-size: 9.5px; }
.e011DetailCard { margin-top: 14px; }
.e011DetailPreview { width: 132px; height: 132px; border-radius: 20px; overflow: hidden; background: #0f172a; margin-bottom: 10px; }
.clientWorkspace .e011ThumbStrip { grid-template-columns: repeat(6, minmax(0, 1fr)); }
@media (max-width: 1080px) {
  .e011GroupBoard { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .e011ThumbStrip, .clientWorkspace .e011ThumbStrip { grid-template-columns: repeat(4, minmax(0, 1fr)); }
}
@media (max-width: 760px) {
  .e011GroupBoard { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .e011ThumbStrip, .clientWorkspace .e011ThumbStrip { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .e011ThumbImageWrap, .e011ThumbImageWrap img, .e011ThumbCard > img { height: 74px; }
  .e011WorkspaceHeader { gap: 8px; }
}
@media (max-width: 480px) {
  .e011GroupBoard { grid-template-columns: 1fr 1fr; gap: 8px; }
  .e011GroupTile { border-radius: 20px; padding: 10px; }
  .e011GroupTitle { font-size: 13px; min-height: 31px; }
  .e011MiniCollage { height: 46px; }
  .e011MiniCollage img, .e011EmptyThumb { height: 46px; border-radius: 10px; }
  .e011ThumbStrip, .clientWorkspace .e011ThumbStrip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}


/* E012_SUPERVISOR_REVIEW_WORKSPACE -----------------------------------------
   Supervisor deja de tener tres secciones verticales. La bandeja funciona como
   mesa de revisión: grupos compactos a la izquierda, evidencia activa y acciones
   a la derecha, con zoom fullscreen para detalle.
*/
.e012SupervisorWorkspace { overflow: visible; }
.e012HeaderRow { display: flex; justify-content: space-between; gap: 14px; align-items: flex-start; flex-wrap: wrap; }
.e012KpiStrip { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.e012KpiStrip span { min-width: 92px; border-radius: 18px; padding: 10px 12px; background: rgba(15,23,42,.05); border: 1px solid rgba(15,23,42,.07); display: grid; gap: 2px; }
.e012KpiStrip strong { font-size: 20px; line-height: 1; color: #0f172a; }
.e012KpiStrip small { color: #64748b; font-size: 11px; font-weight: 800; }
.e012FiltersCard { margin-top: 14px; }
.e012FiltersBottom { margin-top: 8px; }
.e012ModeRow { margin-bottom: 12px; }
.e012ReviewGrid { display: grid; grid-template-columns: minmax(260px, 360px) minmax(0, 1fr); gap: 14px; align-items: start; margin-top: 12px; }
.e012GroupColumn { border: 1px solid rgba(15,23,42,.08); border-radius: 26px; padding: 12px; background: rgba(248,250,252,.78); box-shadow: inset 0 1px 0 rgba(255,255,255,.72); }
.e012ColumnHeader { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; margin-bottom: 10px; }
.e012GroupList { display: grid; gap: 9px; max-height: 62vh; overflow: auto; padding-right: 3px; }
.e012GroupTile { border-radius: 20px; padding: 10px; box-shadow: 0 10px 22px rgba(15,23,42,.05); }
.e012MiniCollage { height: 44px; margin-top: 8px; }
.e012MiniCollage img, .e012MiniCollage .e011EmptyThumb { height: 44px; border-radius: 10px; }
.e012GroupStats { margin-top: 8px; }
.e012GroupStats .riskBadge { font-size: 9.5px; padding: 3px 6px; }
.e012ReviewPanel { border: 1px solid rgba(109,40,217,.14); border-radius: 30px; padding: 14px; background: linear-gradient(135deg, rgba(255,255,255,.98), rgba(245,243,255,.82)); box-shadow: 0 18px 40px rgba(15,23,42,.08); position: sticky; top: 96px; }
.e012ReviewTop { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }
.e012ReviewPills { display: flex; gap: 6px; flex-wrap: wrap; justify-content: flex-end; }
.e012ActiveReviewCard { display: grid; grid-template-columns: minmax(220px, 44%) minmax(0, 1fr); gap: 14px; align-items: stretch; }
.e012ImageStage { min-height: 288px; max-height: 46vh; border-radius: 24px; overflow: hidden; position: relative; background: #0f172a; cursor: zoom-in; display: grid; place-items: center; }
.e012ImageStage img { width: 100%; height: 100%; min-height: 288px; object-fit: contain; display: block; }
.e012ZoomButton { position: absolute; right: 10px; bottom: 10px; border: 1px solid rgba(255,255,255,.22); background: rgba(15,23,42,.72); color: #fff; border-radius: 999px; padding: 9px 12px; display: inline-flex; gap: 6px; align-items: center; font-weight: 850; cursor: pointer; backdrop-filter: blur(8px); }
.e012EvidenceInfo { border: 1px solid rgba(15,23,42,.06); border-radius: 22px; background: rgba(255,255,255,.78); padding: 12px; display: grid; gap: 7px; align-content: start; }
.e012EvidenceTitle { font-size: 18px; font-weight: 950; color: #0f172a; line-height: 1.15; }
.e012ThumbRail { margin-top: 12px; display: flex; gap: 8px; overflow-x: auto; padding: 2px 2px 9px; scroll-snap-type: x proximity; }
.e012Thumb { flex: 0 0 86px; height: 100px; border: 1px solid rgba(15,23,42,.08); border-radius: 16px; background: white; padding: 5px; position: relative; cursor: pointer; scroll-snap-align: start; box-shadow: 0 8px 18px rgba(15,23,42,.05); }
.e012ThumbActive { outline: 3px solid rgba(34,197,94,.20); border-color: rgba(34,197,94,.48); }
.e012Thumb img { width: 100%; height: 64px; object-fit: cover; border-radius: 12px; display: block; background: #0f172a; }
.e012Thumb small { display: block; margin-top: 4px; font-size: 9px; color: #64748b; font-weight: 850; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.e012Thumb .selectionPill { right: 6px; top: 6px; width: 24px; height: 24px; font-size: 12px; }
.e012ActionDock { margin-top: 10px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; border-top: 1px solid rgba(15,23,42,.07); padding-top: 10px; }
.e012ActionDock .actionButton { justify-content: center; }
@media (max-width: 980px) {
  .e012ReviewGrid { grid-template-columns: 1fr; }
  .e012GroupList { max-height: none; grid-template-columns: repeat(2, minmax(0, 1fr)); overflow: visible; padding-right: 0; }
  .e012ReviewPanel { position: static; }
}
@media (max-width: 640px) {
  .e012KpiStrip { width: 100%; justify-content: stretch; }
  .e012KpiStrip span { flex: 1; min-width: 0; }
  .e012GroupList { grid-template-columns: 1fr 1fr; gap: 8px; }
  .e012ActiveReviewCard { grid-template-columns: 1fr; }
  .e012ImageStage, .e012ImageStage img { min-height: 230px; }
  .e012ActionDock { grid-template-columns: 1fr 1fr; }
}

.e012BulkBar { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; border-top: 1px solid rgba(15,23,42,.06); padding-top: 9px; }
.e012BulkBar span { color: #64748b; font-size: 12px; font-weight: 850; margin-right: auto; }
.e012BulkBar .actionButton:disabled { opacity: .45; cursor: not-allowed; box-shadow: none; }
.e012GroupQuickActions { width: 100%; display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.e012ManualReview { margin-top: 10px; display: grid; grid-template-columns: minmax(140px, 180px) 1fr minmax(150px, 190px); gap: 8px; align-items: center; border-top: 1px solid rgba(15,23,42,.07); padding-top: 10px; }
.e012ManualReview .inputLike { margin: 0; }
@media (max-width: 640px) {
  .e012GroupQuickActions { justify-content: flex-start; }
  .e012ManualReview { grid-template-columns: 1fr; }
}



/* E013 - Supervisor review queue: one task per screen */
.e013SupervisorQueue { overflow: visible; }
.e013TopBar { display: flex; align-items: flex-start; justify-content: space-between; gap: 14px; flex-wrap: wrap; }
.e013Sub { max-width: 760px; }
.e013CounterStrip { display: flex; gap: 8px; flex-wrap: wrap; }
.e013CounterStrip span { min-width: 86px; border: 1px solid rgba(15,23,42,.08); background: rgba(248,250,252,.92); border-radius: 18px; padding: 9px 11px; display: grid; gap: 2px; }
.e013CounterStrip strong { font-size: 20px; line-height: 1; color: #0f172a; }
.e013CounterStrip small { color: #64748b; font-size: 11px; font-weight: 850; }
.e013FiltersDrawer { margin-top: 12px; border: 1px solid rgba(15,23,42,.08); border-radius: 18px; background: rgba(255,255,255,.72); padding: 9px 11px; }
.e013FiltersDrawer summary { cursor: pointer; font-weight: 900; color: #334155; }
.e013FiltersDrawer .filtersRow { margin-top: 10px; }
.e013QueueLayout { margin-top: 14px; display: grid; grid-template-columns: minmax(280px, 390px) minmax(0, 1fr); gap: 14px; align-items: start; }
.e013QueueList { display: grid; gap: 8px; }
.e013QueueItem { width: 100%; border: 1px solid rgba(15,23,42,.08); background: rgba(255,255,255,.94); border-radius: 20px; padding: 9px; display: grid; grid-template-columns: 64px minmax(0, 1fr) auto; gap: 10px; align-items: center; cursor: pointer; text-align: left; box-shadow: 0 8px 20px rgba(15,23,42,.045); }
.e013QueueItemActive { border-color: rgba(16,185,129,.55); outline: 3px solid rgba(16,185,129,.14); background: #fff; }
.e013MiniPhoto { width: 64px; height: 64px; border-radius: 16px; overflow: hidden; background: #0f172a; }
.e013MiniPhoto img { width: 100%; height: 100%; object-fit: cover; display: block; }
.e028DeferredThumb { width: 100%; height: 100%; display: grid; place-items: center; padding: 6px; text-align: center; font-size: 10px; line-height: 1.15; color: #607d8b; background: linear-gradient(145deg, rgba(236,239,241,.95), rgba(250,250,250,.98)); }
.e028PhotoLoading { min-height: 320px; width: 100%; display: grid; place-items: center; align-content: center; gap: 10px; padding: 24px; text-align: center; color: #455a64; background: linear-gradient(145deg, rgba(250,250,250,.98), rgba(236,239,241,.88)); }
.e028PhotoLoading span { max-width: 420px; font-size: 12px; color: #78909c; }
.e028LoadMoreBtn { width: calc(100% - 12px); margin: 8px 6px 14px; justify-content: center; border-style: dashed; }
.e028Fix2SyncBar { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin: 10px 0 14px; padding: 10px 12px; border: 1px solid rgba(16,185,129,.22); border-radius: 14px; background: rgba(236,253,245,.72); }
.e028Fix2SyncError { border-color: rgba(245,158,11,.35); background: rgba(255,251,235,.82); }
.e028Fix2SyncState { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; min-width: 0; }
.e028Fix2SyncState strong { font-size: 13px; }
.e028Fix2SyncState span { font-size: 12px; color: #64748b; }
.e028Fix2SyncActions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.e028Fix2PendingBadge { background: rgba(59,130,246,.1); color: #1d4ed8; border-color: rgba(59,130,246,.22); }

.e013QueueText { min-width: 0; display: grid; gap: 2px; }
.e013QueueTitle { font-size: 13px; font-weight: 950; color: #0f172a; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.e013QueueMeta { font-size: 11px; color: #64748b; font-weight: 750; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.e013QueueBadges { margin-top: 3px; display: flex; gap: 5px; flex-wrap: wrap; }
.e013QueueBadges .riskBadge { font-size: 9.5px; padding: 3px 6px; }
.e013ReviewCta { font-size: 11px; font-weight: 900; color: #0f766e; background: rgba(20,184,166,.10); border-radius: 999px; padding: 7px 8px; white-space: nowrap; }
.e013ReviewFocus { border: 1px solid rgba(15,23,42,.08); border-radius: 26px; padding: 12px; background: linear-gradient(135deg, #ffffff, #f8fafc); box-shadow: 0 16px 36px rgba(15,23,42,.075); position: sticky; top: 90px; }
.e013ReviewHeader { display: grid; grid-template-columns: auto 1fr auto; gap: 8px; align-items: center; margin-bottom: 10px; }
.e013ReviewPosition { text-align: center; color: #64748b; font-size: 12px; font-weight: 900; }
.e013PhotoStage { min-height: 360px; max-height: 54vh; border-radius: 24px; overflow: hidden; background: #0f172a; position: relative; display: grid; place-items: center; cursor: zoom-in; }
.e013PhotoStage img { width: 100%; height: 100%; min-height: 360px; object-fit: contain; display: block; }
.e013ZoomBtn { position: absolute; right: 12px; bottom: 12px; border: 1px solid rgba(255,255,255,.22); background: rgba(15,23,42,.74); color: white; border-radius: 999px; padding: 9px 13px; display: inline-flex; align-items: center; gap: 7px; font-weight: 900; cursor: pointer; backdrop-filter: blur(8px); }
.e013ContextLine { margin-top: 11px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; color: #0f172a; font-size: 14px; }
.e013ContextLine span { color: #475569; font-weight: 800; }
.e013ContextSub { margin-top: 5px; display: flex; gap: 8px; flex-wrap: wrap; color: #64748b; font-size: 12px; font-weight: 800; }
.e013ObservationBox { margin-top: 9px; border: 1px solid rgba(245,158,11,.20); background: rgba(255,251,235,.72); color: #92400e; border-radius: 16px; padding: 9px 11px; font-size: 12px; display: grid; gap: 4px; }
.e013CommentInput { margin-top: 10px; }
.e013DecisionDock { margin-top: 10px; display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 9px; }
.e013DecisionDock .actionButton { justify-content: center; }
.e013Approve { background: linear-gradient(135deg, #059669, #10b981); color: white; }
.e013Comment { background: linear-gradient(135deg, #d97706, #f59e0b); color: white; }
.e013Reject { background: linear-gradient(135deg, #dc2626, #f43f5e); color: white; }
@media (max-width: 980px) {
  .e013QueueLayout { grid-template-columns: 1fr; }
  .e013ReviewFocus { position: static; }
  .e013QueueList { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 640px) {
  .e013CounterStrip { width: 100%; }
  .e013CounterStrip span { flex: 1; min-width: 0; }
  .e013QueueList { grid-template-columns: 1fr; }
  .e013QueueItem { grid-template-columns: 58px minmax(0, 1fr); }
  .e013ReviewCta { display: none; }
  .e013PhotoStage, .e013PhotoStage img { min-height: 285px; }
  .e013DecisionDock { grid-template-columns: 1fr; }
  .e013ReviewHeader { grid-template-columns: 1fr; }
  .e013ReviewPosition { order: -1; }
}


/* E018 - navegación rápida y detalle útil común promotor/supervisor */
.e018SectionHeader { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }
.e018TopRefreshBtn { width: auto; min-width: 132px; margin-top: 0; justify-content: center; }
.e018ReviewModeBar { margin-top: 8px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.e018ScrollAnchor { width: 1px; height: 1px; pointer-events: none; }
.e018QueueMetaName, .e018QueueMetaDate { white-space: normal; overflow: visible; text-overflow: clip; line-height: 1.25; }
.e018FloatingNav { position: fixed; right: 14px; bottom: 86px; z-index: 58; display: grid; gap: 8px; }
.e018FloatingNav button, .e018MiniNavRow button { border: 1px solid rgba(15,23,42,.10); background: rgba(255,255,255,.94); color: #0f172a; border-radius: 999px; padding: 9px 12px; font-weight: 950; box-shadow: 0 10px 22px rgba(15,23,42,.12); cursor: pointer; }
.e018MiniNavRow { margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.e018MiniNavRowInline { margin-top: 0; }
.e018GalleryCardBtn { text-align: left; cursor: pointer; }
.e018OutServiceCard { border: 1px solid rgba(245,158,11,.24); background: rgba(255,251,235,.86); border-radius: 20px; padding: 12px; display: grid; gap: 6px; box-shadow: 0 8px 20px rgba(15,23,42,.045); }
.e018OutServiceTitle { color: #0f172a; font-weight: 950; font-size: 13px; line-height: 1.25; }
.e018OutServiceMeta { color: #64748b; font-size: 12px; font-weight: 800; line-height: 1.25; }
.e018OutServiceReason { color: #92400e; background: rgba(245,158,11,.12); border-radius: 12px; padding: 7px 9px; font-size: 12px; font-weight: 900; }
.e018OutServiceComment { color: #7c2d12; font-size: 12px; line-height: 1.35; }
@media (max-width: 640px) {
  .e018TopRefreshBtn { width: 100%; }
  .e018FloatingNav { right: 10px; bottom: 78px; }
  .e018FloatingNav button { padding: 8px 10px; font-size: 12px; }
  .e018SectionHeader { align-items: stretch; }
  .e018MiniNavRowInline { width: 100%; justify-content: space-between; }
}

/* E014 - REZGO rules + safe image viewer exit */
.e014NoBrandBox {
  margin-top: 10px;
  border-style: dashed;
  background: rgba(245, 158, 11, 0.08);
}
.e014ViewerTopbar {
  position: fixed;
  top: 12px;
  right: 12px;
  left: 12px;
  z-index: 120;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  pointer-events: auto;
}
.e014ViewerClose, .e014ViewerControls button {
  border: 1px solid rgba(255,255,255,0.20);
  background: rgba(15,23,42,0.72);
  color: #fff;
  border-radius: 999px;
  padding: 10px 14px;
  font-weight: 900;
  cursor: pointer;
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
}
.e014ViewerControls {
  display: flex;
  align-items: center;
  gap: 7px;
  border-radius: 999px;
  padding: 6px;
  background: rgba(15,23,42,0.42);
  border: 1px solid rgba(255,255,255,0.12);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  color: #fff;
  font-weight: 850;
}
.e014ViewerControls span { min-width: 48px; text-align: center; }
@media (max-width: 560px) {
  .e014ViewerTopbar { align-items: flex-start; }
  .e014ViewerControls { max-width: 46vw; overflow-x: auto; }
  .e014ViewerClose { padding: 10px 12px; }
}

/* E019 - fluidez, sin servicio y revisión masiva visible */
.e019OutServiceActions { display: grid; grid-template-columns: 1fr auto; gap: 8px; margin-top: 10px; align-items: center; }
.e019PromotorModeBar, .e019StatusModeBar { display: flex; gap: 8px; flex-wrap: wrap; margin: 10px 0 12px; }
.e019OutServiceListItem { border: 1px solid rgba(249,115,22,0.18); background: rgba(255,247,237,0.72); text-align: left; }
.e019BatchBar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; padding: 10px; border: 1px solid rgba(15,23,42,0.08); border-radius: 18px; background: rgba(255,255,255,0.86); margin: 10px 0 14px; }
.e019BatchInfo { padding: 8px 10px; border-radius: 999px; background: rgba(15,23,42,0.06); color: #334155; font-weight: 800; }
.e019ActionNotice { display: flex; align-items: center; gap: 8px; padding: 11px 13px; margin: 8px 0 12px; border-radius: 16px; background: rgba(236,253,245,0.95); border: 1px solid rgba(34,197,94,0.22); color: #166534; font-weight: 900; }
.e019SelectPill { display: inline-flex; align-items: center; gap: 6px; align-self: flex-start; padding: 7px 9px; border-radius: 999px; background: rgba(15,23,42,0.05); color: #475569; font-size: 11px; font-weight: 900; cursor: pointer; }
.e019SelectPill input { width: 14px; height: 14px; accent-color: #4caf50; }
@media (max-width: 760px) { .e019OutServiceActions { grid-template-columns: 1fr; } .e019BatchBar .actionButton, .e019BatchBar .compactBtn { width: 100%; justify-content: center; } }

`;