// scripts/update-books.mjs
// TRC新刊(zip内TSV/TXT) → ISBN抽出（列揺れ耐性）→ whitelist.json（レーベル一致）
// → openBDでタイトル/書影 → cover欠損はNDL/OpenLibraryで補完
// → data/books.json をロケットえんぴつ方式で更新（最大8枠維持）
// ※items=0 の日は books.json を更新しない（真っ白回避）

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

// ---------- utils ----------
function uniq(arr){ return [...new Set(arr)]; }

function normStr(s){
  return String(s ?? "").normalize("NFKC").trim();
}

function normIsbn(s){
  const t = normStr(s).replace(/[^0-9Xx]/g,"").toUpperCase();
  return t;
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

// 画像URLが実在するか確認して、OKならURLを返す
async function probeImage(url){
  try{
    // HEADが弾かれることがあるのでGET（軽量）
    const res = await fetchWithTimeout(url, { method: "GET" }, 5000);
    if(!res.ok) return null;

    const ct = (res.headers.get("content-type") || "").toLowerCase();
    // 画像ならOK（ctが取れないケースもあるので甘め）
    if(ct && !ct.startsWith("image/")) return null;

    return url;
  } catch {
    return null;
  }
}

// ---------- TRC ----------
async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");
  return new URL(m[1], TRC_PAGE).toString();
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

function extractIsbnsFromTrcText(text, labels){
  const lines = splitLines(text);
  if(lines.length === 0) return [];

  const { header, rows } = parseTable(lines);

  const isbnCol = inferIsbnColumn(header, rows);
  if(isbnCol === -1) console.log("WARN: ISBN column not inferred");

  const isbns = [];

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
    if(isbn) isbns.push(isbn);
  }

  return uniq(isbns);
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

function pick(entry, fallbackIsbn){
  if(!entry) return null;

  const s = entry.summary || {};
  const isbn = normIsbn(s.isbn || fallbackIsbn);
  const title = pickTitle(entry);
  const cover = pickCover(entry);

  if(!isbn || !title) return null;

  return {
    isbn,
    title,
    cover: cover ?? null,
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
  // coverがnullのものだけ補完
  for(const it of items){
    if(it.cover) continue;

    const isbn13 = normIsbn(it.isbn);
    if(!/^97[89]\d{10}$/.test(isbn13)) continue;

    // 1) NDL thumbnail（あれば最優先）
    const ndl = await probeImage(NDL_THUMB(isbn13));
    if(ndl){
      it.cover = ndl;
      continue;
    }

    // 2) Open Library covers
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
    // 最低限の形だけ整える
    return items
      .map(x => ({
        isbn: normIsbn(x?.isbn),
        title: String(x?.title ?? ""),
        cover: x?.cover ?? null,
        amazon: x?.amazon ?? (x?.isbn ? amazonUrlFromIsbn(normIsbn(x.isbn)) : "#"),
      }))
      .filter(x => /^97[89]\d{10}$/.test(x.isbn) && x.title);
  } catch {
    return [];
  }
}

function mergeRocketPencil(newItems, oldItems, limit = 8){
  // 先頭にnewItems、後ろにoldItems。ISBN重複は「先に出た方」を採用
  const seen = new Set();
  const merged = [];

  for(const it of [...newItems, ...oldItems]){
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
  const tmpZip = "data/_trc_latest.zip";

  await fetchBinToFile(zipUrl, tmpZip);
  const trcText = await unzipPickTextFile(tmpZip);

  const isbnsAll = extractIsbnsFromTrcText(trcText, labels);
  const isbns = isbnsAll.slice(0, TAKE_ISBNS);

  if(isbns.length === 0){
    // items=0日は更新しない（既存が無い場合だけ空を作る）
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

  // openBD結果から itemsToday を作る（まず多めに作って、最後に8にする）
  const itemsTodayAll = [];
  for(let i=0;i<openbd.length;i++){
    const b = pick(openbd[i], isbns[i]);
    if(b) itemsTodayAll.push(b);
  }

  // 先に最大8だけに絞り、そこだけ書影補完（無駄なHTTPを減らす）
  const itemsToday = await fillMissingCovers(itemsTodayAll.slice(0, 8));

  if(itemsToday.length === 0){
    // openBD/補完後に0になった日も更新しない
    console.log("OK: 0 items after openBD -> keep existing books.json");
    return;
  }

  // ロケットえんぴつ：既存と結合して常に最大8枠維持
  const existing = await loadExistingItems();
  const merged = mergeRocketPencil(itemsToday, existing, 8);

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
