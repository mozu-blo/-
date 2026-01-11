// scripts/update-books.mjs
// 目的：TRC新刊TSV/TXT(zip) → ISBN抽出（列揺れ耐性）→ whitelist.json（レーベル一致）
// → openBDで書影/タイトル取得 → data/books.json更新（ロケットえんぴつ方式）
// ※items=0 の日は books.json を更新しない（表示が真っ白にならない）

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

// ---------- utils ----------
function uniq(arr){ return [...new Set(arr)]; }

function normStr(s){
  return String(s ?? "").normalize("NFKC").trim();
}

function normIsbn(s){
  // 978/979 + 10桁 or 13桁、ハイフン等を除去
  const t = normStr(s).replace(/[^0-9Xx]/g,"").toUpperCase();
  // ISBN10も来たら13に変換…はしない（TRCは基本ISBN13）
  return t;
}

function isIsbn13Like(s){
  const t = normIsbn(s);
  return /^97[89]\d{10}$/.test(t);
}

function extractIsbnFromTextLine(line){
  // 行全体から ISBN13 を拾う（列推定できないときの保険）
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

// ---------- TRC ----------
async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);
  // TRCページは新しいのが上の想定：最初の .zip を採用
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");
  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipPickTextFile(zipPath){
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  // TSV/TXT を優先して拾う（TRCは .txt のことがある）
  const pick =
    names.find(n => n.toLowerCase().endsWith(".tsv")) ||
    names.find(n => n.toLowerCase().endsWith(".txt")) ||
    names[0];

  if(!pick) throw new Error("No file in zip");

  console.log("ZIP CONTENTS picked:", pick);

  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, pick], {
    // 大きめに（TRCはそこそこサイズある）
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

function cellHasAnyLabel(cell, labels){
  if(labels.length === 0) return false;
  const t = normStr(cell);
  return labels.some(lb => lb && t.includes(lb));
}

// ---------- parsing / inference ----------
function splitLines(text){
  return String(text ?? "").split(/\r?\n/).map(s => s.replace(/\u0000/g,"")).filter(s => s.trim().length > 0);
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

  // 全行を同じ列数にそろえる
  const normRows = rows.map(r => {
    if(r.length === maxCols) return r;
    return [...r, ...Array(maxCols - r.length).fill("")];
  });

  return { header: normRows[0], rows: normRows.slice(1), delim };
}

function inferIsbnColumn(header, rows){
  // 1) ヘッダ名に ISBN が含まれていれば最優先
  const h = header.map(x => normStr(x));
  let idx = h.findIndex(x => x.toLowerCase().includes("isbn"));
  if(idx !== -1) return idx;

  // 2) 実データで ISBN13っぽい値が多い列を採用
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

  // 3件以上一致がないなら「列推定できない」
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
    // label_only：行のどこかにホワイトリストのレーベル文字列が含まれること
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
  // openBDは summary.cover が null のことがあるので保険を複数
  const s = entry?.summary || {};
  if(s.cover) return s.cover;

  // hanmoto
  if(entry?.hanmoto?.cover) return entry.hanmoto.cover;

  // onix（存在する場合）
  const link =
    entry?.onix?.CollateralDetail?.SupportResource?.[0]?.ResourceVersion?.[0]?.ResourceLink;
  if(link) return link;

  return null;
}

function pickTitle(entry){
  const s = entry?.summary || {};
  if(s.title) return s.title;

  // onix title fallback
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
  // chunked
  const out = [];
  for(let i=0;i<isbns.length;i+=OPENBD_CHUNK){
    const chunk = isbns.slice(i, i+OPENBD_CHUNK);
    const url = OPENBD + encodeURIComponent(chunk.join(","));
    const res = await fetchJson(url);
    out.push(...res);
  }
  return out;
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
    // ロケットえんぴつ方式：0件なら既存を維持（なければ空を作る）
    const exists = await fs.access("data/books.json").then(()=>true).catch(()=>false);
    if(!exists){
      const out0 = { generated_at: new Date().toISOString(), source: zipUrl, items: [] };
      await fs.writeFile("data/books.json", JSON.stringify(out0, null, 2), "utf-8");
      console.log("OK: 0 items (books.json created empty because it did not exist)");
    }else{
      console.log("OK: 0 items (no label-matched isbns) -> keep existing books.json");
    }
    return;
  }

  const openbd = await openbdBatch(isbns);

  const items = [];
  for(let i=0;i<openbd.length;i++){
    const b = pick(openbd[i], isbns[i]);
    if(b) items.push(b);
  }

  // 8件以上取れない日があっても、ここでは “取れたぶんだけ” 返す
  // 表示側で “埋め” はしない（表示の正しさ優先）
  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: items.slice(0, 8),
  };

  if(out.items.length === 0){
    // 0なら更新しない（ロケットえんぴつ）
    console.log("OK: 0 items after openBD -> keep existing books.json");
    return;
  }

  await fs.writeFile("data/books.json", JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", out.items.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
