// scripts/update-books.mjs
// 目的：TRC新刊TSV/TXT(zip) → ISBN抽出（列揺れ耐性）→ whitelist.json（レーベル一致）
// → openBDでタイトル/発売日/書影取得 → 書影不足はNDL/OpenLibraryで補完
// → data/books.json更新（ロケットえんぴつ方式）
// ※items=0 の日は books.json を更新しない（真っ白回避）
//
// A+（最強ログ）
// - TRCページ上のzipリンクを複数抽出して一覧表示
// - ファイル名に含まれるYYYYMMDDで「最大」を自前で選ぶ（=最新）
// - 選定結果/候補一覧/日付抽出結果をログに出す

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";

const AMAZON_TAG = "mozublo-22";

// 取りに行く候補数（多めに取って whitelist で絞る）
const TAKE_ISBNS = 1200;
// openBDは一括に限界があるので分割（安全側）
const OPENBD_CHUNK = 100;

// whitelist
const WHITELIST_PATH = "data/whitelist.json";
// output
const BOOKS_PATH = "data/books.json";

// 書影フォールバック（実在チェックして採用）
const NDL_THUMB = (isbn13) => `https://ndlsearch.ndl.go.jp/thumbnail/${isbn13}.jpg`; // 404あり
const OL_THUMB = (isbn13) => `https://covers.openlibrary.org/b/isbn/${isbn13}-L.jpg?default=false`; // 404あり

// A+：TRCページ上のzip候補を何件表示するか（ログ用）
const TRC_ZIP_LOG_LIMIT = 12;

// ---------- utils ----------
function uniq(arr){ return [...new Set(arr)]; }

function normStr(s){
  return String(s ?? "").normalize("NFKC").trim();
}

function normIsbn(s){
  return normStr(s).replace(/[^0-9Xx]/g,"").toUpperCase();
}

function isIsbn13Like(s){
  const t = normIsbn(s);
  return /^97[89]\d{10}$/.test(t);
}

function extractIsbnFromTextLine(line){
  const m = normStr(line).match(/97[89]\d{10}/);
  return m ? m[0] : "";
}

function amazonUrlFromIsbn(isbn){
  return `https://www.amazon.co.jp/s?k=${encodeURIComponent(isbn)}&tag=${encodeURIComponent(AMAZON_TAG)}`;
}

async function fetchText(url){
  const res = await fetch(url, { headers: { "user-agent": "mozublo-bot/1.0" }});
  if(!res.ok) throw new Error(`fetch failed ${res.status} ${url}`);
  return await res.text();
}

async function fetchJson(url){
  const res = await fetch(url, { headers: { "user-agent": "mozublo-bot/1.0" }});
  if(!res.ok) throw new Error(`fetch failed ${res.status} ${url}`);
  return await res.json();
}

async function fetchBinToFile(url, filepath){
  const res = await fetch(url, { headers: { "user-agent": "mozublo-bot/1.0" }});
  if(!res.ok) throw new Error(`fetch failed ${res.status} ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  await fs.writeFile(filepath, buf);
}

async function exists(path){
  return await fs.access(path).then(()=>true).catch(()=>false);
}

// timeout付きfetch（画像存在チェック用）
async function fetchWithTimeout(url, opts = {}, ms = 4000){
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try{
    const res = await fetch(url, { ...opts, signal: ac.signal, headers: { "user-agent": "mozublo-bot/1.0", ...(opts.headers||{}) } });
    return res;
  } finally {
    clearTimeout(t);
  }
}

async function probeImage(url){
  try{
    const res = await fetchWithTimeout(url, { method: "GET" }, 5000);
    if(!res.ok) return null;
    const ct = (res.headers.get("content-type") || "").toLowerCase();
    if(ct && !ct.startsWith("image/")) return null;
    return url;
  } catch {
    return null;
  }
}

// ---------- TRC (A+ 強化) ----------
function extractZipLinksFromHtml(html){
  // href="...zip" を全部拾う。相対/絶対どっちもOK
  const out = [];
  const re = /href="([^"]+?\.zip)"/gi;
  let m;
  while((m = re.exec(html)) !== null){
    out.push(m[1]);
  }
  return uniq(out);
}

function parseYyyymmddFromZipName(urlStr){
  // 例: TRCOpenBibData_20260110.zip から 20260110 を抜く
  const s = String(urlStr);
  const m = s.match(/(\d{8})(?=\.zip\b)/);
  return m ? m[1] : "";
}

function yyyymmddToNumber(ymd){
  if(!/^\d{8}$/.test(ymd)) return -1;
  return Number(ymd);
}

async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);

  const rawLinks = extractZipLinksFromHtml(html);
  if(rawLinks.length === 0) throw new Error("TRC zip link not found");

  // TRC_PAGE を基準に絶対URLへ
  const abs = rawLinks.map(href => new URL(href, TRC_PAGE).toString());

  // 日付推定（YYYYMMDD）を付与して並べる
  const withDate = abs.map(u => ({
    url: u,
    ymd: parseYyyymmddFromZipName(u),
    n: yyyymmddToNumber(parseYyyymmddFromZipName(u)),
  }));

  // ログ：候補一覧（最大TRC_ZIP_LOG_LIMIT件）
  const topForLog = withDate.slice(0, TRC_ZIP_LOG_LIMIT);
  console.log("TRC zip candidates (first " + topForLog.length + "):");
  for(const x of topForLog){
    console.log(" -", x.ymd || "--------", x.url);
  }

  // 日付が取れるやつだけで最大を選ぶ。取れないURLしかない場合は「先頭」を使う。
  const dated = withDate.filter(x => x.n !== -1).sort((a,b) => b.n - a.n);

  if(dated.length > 0){
    const picked = dated[0];
    console.log("TRC latest zip picked by max yyyymmdd:", picked.ymd, picked.url);
    return picked.url;
  }

  // fallback
  console.log("WARN: could not parse yyyymmdd from zip names -> fallback to first href");
  console.log("TRC latest zip picked by fallback:", withDate[0].url);
  return withDate[0].url;
}

async function unzipPickTextFile(zipPath){
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  const pick =
    names.find(n => n.toLowerCase().endsWith(".tsv")) ||
    names.find(n => n.toLowerCase().endsWith(".txt")) ||
    names[0];

  if(!pick) throw new Error("No file in zip");

  console.log("ZIP CONTENTS picked:", pick);

  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, pick], {
    maxBuffer: 50 * 1024 * 1024,
  });
  return stdout;
}

// ---------- whitelist ----------
async function loadWhitelistLabels(){
  try{
    const raw = await fs.readFile(WHITELIST_PATH, "utf-8");
    const json = JSON.parse(raw);
    const labels = (json.label_includes_any || []).map(x => normStr(x)).filter(Boolean);
    return uniq(labels);
  }catch{
    return [];
  }
}

function lineHasAnyLabel(line, labels){
  if(labels.length === 0) return false;
  const t = normStr(line);
  return labels.some(lb => lb && t.includes(lb));
}

// ---------- parsing / inference ----------
function splitLines(text){
  return String(text ?? "")
    .split(/\r?\n/)
    .map(s => s.replace(/\u0000/g,""))
    .filter(s => s.trim().length > 0);
}

function detectDelimiter(line){
  const tab = (line.match(/\t/g) || []).length;
  const comma = (line.match(/,/g) || []).length;
  return tab >= comma ? "\t" : ",";
}

function parseTable(lines){
  if(lines.length === 0) return { header: [], rows: [], delim: "\t" };

  const delim = detectDelimiter(lines[0]);
  const rows = lines.map(l => l.split(delim));
  const maxCols = Math.max(...rows.map(r => r.length));

  const normRows = rows.map(r => {
    if(r.length === maxCols) return r;
    return [...r, ...Array(maxCols - r.length).fill("")];
  });

  return { header: normRows[0], rows: normRows.slice(1), delim };
}

function inferIsbnColumn(header, rows){
  const h = header.map(x => normStr(x));
  let idx = h.findIndex(x => x.toLowerCase().includes("isbn"));
  if(idx !== -1) return idx;

  const sample = rows.slice(0, 200);
  if(sample.length === 0) return -1;

  const cols = header.length;
  const counts = Array(cols).fill(0);

  for(const r of sample){
    for(let c=0;c<cols;c++){
      if(isIsbn13Like(r[c])) counts[c] += 1;
    }
  }

  let best = -1;
  let bestCount = 0;
  for(let c=0;c<cols;c++){
    if(counts[c] > bestCount){
      bestCount = counts[c];
      best = c;
    }
  }

  if(bestCount < 3) return -1;
  return best;
}

function formatPubdateMin(raw){
  if(!raw) return null;
  const t = String(raw).trim();
  if(!t) return null;

  if(/^\d{8}$/.test(t)){
    return `${t.slice(0,4)}/${t.slice(4,6)}/${t.slice(6,8)}`;
  }
  const m = t.match(/^(\d{4})[\/\-\.年](\d{1,2})[\/\-\.月](\d{1,2})/);
  if(m){
    const y=m[1], mm=m[2].padStart(2,"0"), dd=m[3].padStart(2,"0");
    return `${y}/${mm}/${dd}`;
  }
  const m2 = t.match(/^(\d{4})[\/\-\.年](\d{1,2})/);
  if(m2){
    const y=m2[1], mm=m2[2].padStart(2,"0");
    return `${y}/${mm}`;
  }
  return (t.length <= 10) ? t : null;
}

function inferPubdateColumn(header, rows){
  const h = header.map(x => normStr(x));
  // ヘッダにありがちな語を優先
  const keys = ["発売日","発行日","配本日","刊行日","発売","発行","刊行"];
  let idx = h.findIndex(x => keys.some(k => x.includes(k)));
  if(idx !== -1) return idx;

  // データから日付っぽい値が多い列を採用
  const sample = rows.slice(0, 200);
  if(sample.length === 0) return -1;

  const cols = header.length;
  const counts = Array(cols).fill(0);

  for(const r of sample){
    for(let c=0;c<cols;c++){
      const v = formatPubdateMin(r[c]);
      if(v) counts[c] += 1;
    }
  }

  let best = -1;
  let bestCount = 0;
  for(let c=0;c<cols;c++){
    if(counts[c] > bestCount){
      bestCount = counts[c];
      best = c;
    }
  }

  if(bestCount < 3) return -1;
  return best;
}

function extractIsbnsAndPubdateFromTrcText(text, labels){
  const lines = splitLines(text);
  if(lines.length === 0) return { isbns: [], pubdateByIsbn: new Map(), pubCol: -1 };

  const { header, rows } = parseTable(lines);

  const isbnCol = inferIsbnColumn(header, rows);
  if(isbnCol === -1) console.log("WARN: ISBN column not inferred");

  const pubCol = inferPubdateColumn(header, rows);
  if(pubCol === -1) console.log("WARN: pubdate column not inferred");

  const isbns = [];
  const pubdateByIsbn = new Map();

  for(const r of rows){
    const joined = r.join("\t");
    if(!lineHasAnyLabel(joined, labels)) continue;

    let isbn = "";
    if(isbnCol !== -1){
      isbn = normIsbn(r[isbnCol]);
      if(!/^97[89]\d{10}$/.test(isbn)) isbn = "";
    }
    if(!isbn){
      isbn = extractIsbnFromTextLine(joined);
    }
    if(!isbn) continue;

    isbns.push(isbn);

    // 発売日（推定列が取れたらそこ、取れないなら行から拾う）
    let pd = null;
    if(pubCol !== -1){
      pd = formatPubdateMin(r[pubCol]);
    }
    if(!pd){
      // 行内に日付っぽいものがあれば拾う（保険）
      const m8 = joined.match(/\b(\d{8})\b/);
      if(m8) pd = formatPubdateMin(m8[1]);
      if(!pd){
        const mYmd = joined.match(/(\d{4})[\/\-\.年](\d{1,2})[\/\-\.月](\d{1,2})/);
        if(mYmd) pd = formatPubdateMin(mYmd[0]);
      }
    }
    if(pd && !pubdateByIsbn.has(isbn)){
      pubdateByIsbn.set(isbn, pd);
    }
  }

  return { isbns: uniq(isbns), pubdateByIsbn, pubCol };
}

// ---------- openBD ----------
function pickCover(entry){
  const s = entry?.summary || {};
  if(s.cover) return s.cover;

  if(entry?.hanmoto?.cover) return entry.hanmoto.cover;

  const link =
    entry?.onix?.CollateralDetail?.SupportResource?.[0]?.ResourceVersion?.[0]?.ResourceLink;
  if(link) return link;

  return null;
}

function pickTitle(entry){
  const s = entry?.summary || {};
  if(s.title) return s.title;

  const t =
    entry?.onix?.DescriptiveDetail?.TitleDetail?.[0]?.TitleElement?.[0]?.TitleText?.content;
  if(t) return t;

  return "";
}

function pickPubdateFromOpenbd(entry){
  const s = entry?.summary || {};
  const raw = s.pubdate ?? "";
  return formatPubdateMin(raw);
}

function pick(entry, fallbackIsbn, trcPubdate){
  if(!entry) return null;

  const s = entry.summary || {};
  const isbn = normIsbn(s.isbn || fallbackIsbn);
  const title = pickTitle(entry);
  const cover = pickCover(entry);

  // 発売日：TRC優先 → openBD補助
  const pubdate = trcPubdate ?? pickPubdateFromOpenbd(entry) ?? null;

  if(!isbn || !title) return null;

  return {
    isbn,
    title,
    cover: cover ?? null,
    pubdate,
    amazon: amazonUrlFromIsbn(isbn),
  };
}

async function openbdBatch(isbns){
  const out = [];
  for(let i=0;i<isbns.length;i+=OPENBD_CHUNK){
    const chunk = isbns.slice(i, i+OPENBD_CHUNK);
    const url = OPENBD + encodeURIComponent(chunk.join(","));
    const res = await fetchJson(url);
    out.push(...res);
  }
  return out;
}

// ---------- cover fallback ----------
async function fillMissingCovers(items){
  for(const it of items){
    if(it.cover) continue;

    const isbn13 = normIsbn(it.isbn);
    if(!/^97[89]\d{10}$/.test(isbn13)) continue;

    const ndl = await probeImage(NDL_THUMB(isbn13));
    if(ndl){
      it.cover = ndl;
      continue;
    }

    const ol = await probeImage(OL_THUMB(isbn13));
    if(ol){
      it.cover = ol;
      continue;
    }
  }
  return items;
}

// ---------- rocket pencil merge ----------
async function loadExistingItems(){
  try{
    const raw = await fs.readFile(BOOKS_PATH, "utf-8");
    const json = JSON.parse(raw);
    const items = Array.isArray(json.items) ? json.items : [];
    return items
      .map(x => ({
        isbn: normIsbn(x?.isbn),
        title: String(x?.title ?? ""),
        cover: x?.cover ?? null,
        pubdate: x?.pubdate ?? null,
        amazon: x?.amazon ?? (x?.isbn ? amazonUrlFromIsbn(normIsbn(x.isbn)) : "#"),
      }))
      .filter(x => /^97[89]\d{10}$/.test(x.isbn) && x.title);
  } catch {
    return [];
  }
}

function mergeRocketPencil(newItems, oldItems, limit = 8){
  // 既存の情報で補完（新しい方が欠けてる時だけ）
  const oldByIsbn = new Map(oldItems.map(x => [normIsbn(x.isbn), x]));
  const patchedNew = newItems.map(it => {
    const k = normIsbn(it?.isbn);
    const old = oldByIsbn.get(k);
    if(!old) return it;
    return {
      ...it,
      cover: it.cover ?? old.cover ?? null,
      pubdate: it.pubdate ?? old.pubdate ?? null,
      amazon: it.amazon || old.amazon || (k ? amazonUrlFromIsbn(k) : "#"),
    };
  });

  const seen = new Set();
  const merged = [];
  for(const it of [...patchedNew, ...oldItems]){
    const k = normIsbn(it?.isbn);
    if(!k || seen.has(k)) continue;
    seen.add(k);
    merged.push(it);
    if(merged.length >= limit) break;
  }
  return merged;
}

// ---------- main ----------
async function main(){
  await fs.mkdir("data", { recursive: true });

  const labels = await loadWhitelistLabels();

  const zipUrl = await findLatestTrcZipUrl();
  console.log("TRC ZIP URL:", zipUrl);

  const tmpZip = "data/_trc_latest.zip";

  await fetchBinToFile(zipUrl, tmpZip);
  const trcText = await unzipPickTextFile(tmpZip);

  const { isbns: isbnsAll, pubdateByIsbn, pubCol } = extractIsbnsAndPubdateFromTrcText(trcText, labels);
  const isbns = isbnsAll.slice(0, TAKE_ISBNS);

  // ---- A+ ログ（状況が一目で分かるやつ）----
  console.log("WHITELIST labels:", labels.length);
  console.log("TRC label-matched unique isbns:", isbnsAll.length);
  console.log("TRC pubdateByIsbn size:", pubdateByIsbn.size);
  console.log("TRC inferred pubCol:", pubCol);
  console.log("TRC isbn head:", isbnsAll.slice(0, 10).join(", "));
  // --------------------------------------------

  if(isbns.length === 0){
    if(!(await exists(BOOKS_PATH))){
      const out0 = { generated_at: new Date().toISOString(), source: zipUrl, items: [] };
      await fs.writeFile(BOOKS_PATH, JSON.stringify(out0, null, 2), "utf-8");
      console.log("OK: 0 items (books.json created empty because it did not exist)");
    } else {
      console.log("OK: 0 items (no label-matched isbns) -> keep existing books.json");
    }
    return;
  }

  const openbd = await openbdBatch(isbns);

  const itemsTodayAll = [];
  for(let i=0;i<openbd.length;i++){
    const isbn = isbns[i];
    const trcPd = pubdateByIsbn.get(isbn) ?? null;
    const b = pick(openbd[i], isbn, trcPd);
    if(b) itemsTodayAll.push(b);
  }

  // 今日の候補を最大8に絞ってから書影補完（通信量節約）
  const itemsToday = await fillMissingCovers(itemsTodayAll.slice(0, 8));

  if(itemsToday.length === 0){
    console.log("OK: 0 items after openBD -> keep existing books.json");
    return;
  }

  // ロケットえんぴつ：既存と結合して最大8枠維持
  const existing = await loadExistingItems();
  const merged = mergeRocketPencil(itemsToday, existing, 8);

  // ---- A+ ログ（ロケット挙動確認）----
  const existingIsbns = existing.map(x => x.isbn);
  const todayIsbns = itemsToday.map(x => x.isbn);
  const mergedIsbns = merged.map(x => x.isbn);

  const newToday = todayIsbns.filter(x => !existingIsbns.includes(x));
  console.log("EXISTING ISBNs:", existingIsbns.join(", "));
  console.log("TODAY    ISBNs:", todayIsbns.join(", "));
  console.log("MERGED   ISBNs:", mergedIsbns.join(", "));
  console.log("newTodayCount (today not in existing):", newToday.length);
  // -----------------------------------

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: merged,
  };

  await fs.writeFile(BOOKS_PATH, JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", out.items.length, "items (rocket pencil)");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
