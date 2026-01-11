// scripts/update-books.mjs
// 目的：TRC新刊(zip) → (ISBN→レーベル)抽出 → whitelist(レーベルのみ)でフィルタ → openBDで書影/タイトル取得 → data/books.json更新（ロケットえんぴつ）
//
// 重要仕様：
// - whitelist は data/whitelist.json だけを見る（レーベルのみ照合）
// - レーベルが取れない行は捨てる（label_only）
// - cover が null でも捨てない（フロントでNO IMAGE）
// - 0件の日は books.json を更新しない（棚を真っ白にしない）
// - 取れた分だけ先頭に追加して8件に丸める（ロケットえんぴつ）

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";
const AMAZON_TAG = "mozublo-22";

const WHITELIST_PATH = "data/whitelist.json";
const BOOKS_PATH = "data/books.json";

const MAX_KEEP = 8;
const TAKE = 1200; // 多めに読む（8埋め安定化）

function uniq(arr){ return [...new Set(arr)]; }
function normIsbn(s){ return String(s||"").replace(/[^0-9Xx]/g,"").toUpperCase(); }

function amazonUrlFromIsbn(isbn){
  return `https://www.amazon.co.jp/s?k=${encodeURIComponent(isbn)}&tag=${encodeURIComponent(AMAZON_TAG)}`;
}

// 表記揺れ吸収（照合用）
function normKey(s){
  return String(s ?? "")
    .toLowerCase()
    .replace(/[　\s]/g, "")
    .replace(/[！!]/g, "!")
    .replace(/[：:]/g, ":")
    .replace(/[・･]/g, "")
    .replace(/[’'"]/g, "")
    .replace(/[（）()\[\]【】「」『』]/g, "")
    .replace(/[‐-–—―ー\-]/g, "-")
    .replace(/[,，.。]/g, "");
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

async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");
  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipPickTextLike(zipPath){
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  const pick =
    names.find(n => n.toLowerCase().endsWith(".tsv")) ||
    names.find(n => n.toLowerCase().endsWith(".txt")) ||
    names[0];

  if(!pick) throw new Error("No file in zip");

  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, pick], { maxBuffer: 80 * 1024 * 1024 });
  console.log("ZIP CONTENTS picked:", pick);
  return { name: pick, text: stdout };
}

async function readWhitelist(){
  const raw = await fs.readFile(WHITELIST_PATH, "utf-8");
  const json = JSON.parse(raw);
  const list = Array.isArray(json.label_includes_any) ? json.label_includes_any : [];
  const normed = list.map(normKey).filter(Boolean);
  return { raw: list, normed };
}

function passesLabelOnly(wh, labelText){
  const hay = normKey(labelText);
  if(!hay) return false;
  return wh.normed.some(k => k && hay.includes(k));
}

async function readExistingBooks(){
  try{
    const raw = await fs.readFile(BOOKS_PATH, "utf-8");
    const json = JSON.parse(raw);
    return Array.isArray(json.items) ? json.items : [];
  }catch{
    return [];
  }
}

function rocketPencilMerge(newItems, oldItems){
  const seen = new Set();
  const merged = [];
  for(const it of [...newItems, ...oldItems]){
    const isbn = normIsbn(it?.isbn);
    if(!isbn || seen.has(isbn)) continue;
    seen.add(isbn);
    merged.push({ ...it, isbn });
    if(merged.length >= MAX_KEEP) break;
  }
  return merged;
}

// openBD
async function openbdBatch(isbns){
  const url = OPENBD + encodeURIComponent(isbns.join(","));
  return await fetchJson(url);
}

function pickFromOpenbd(entry, fallbackIsbn){
  const isbn = normIsbn(entry?.summary?.isbn || fallbackIsbn);
  const title = entry?.summary?.title || "";
  const cover = entry?.summary?.cover || ""; // null→""（フロントでNO IMAGE）
  if(!isbn || !title) return null;
  return { isbn, title, cover, amazon: amazonUrlFromIsbn(isbn) };
}

// ---------- TRC解析：ISBN→label を作る（レーベルonly） ----------

// まず “タブ区切りっぽいか” を判定
function detectDelimiter(lines){
  const sample = lines.slice(0, 30).join("\n");
  if(sample.includes("\t")) return "\t";
  // まれにカンマ区切りっぽいのが来たら一応対応
  if(sample.includes(",")) return ",";
  return null; // 区切れない＝列推定が難しい
}

// ヘッダがあるっぽいかを雑に判定
function looksLikeHeader(line){
  const s = String(line || "");
  return /isbn/i.test(s) || /書名|タイトル|レーベル|叢書|シリーズ|出版者|出版社/.test(s);
}

function splitCols(line, delim){
  if(!delim) return [String(line || "")];
  return String(line || "").split(delim);
}

// ヘッダから “label列” を探す（日本語/英字ゆれも少し吸収）
function findLabelIndexFromHeader(headerCols){
  const keys = [
    /レーベル/i,
    /label/i,
    /叢書/i,
    /シリーズ/i,
    /シリーズ名/i,
    /叢書名/i,
    /シリーズ.*レーベル/i,
  ];
  for(let i=0;i<headerCols.length;i++){
    const h = String(headerCols[i] || "");
    if(keys.some(r => r.test(h))) return i;
  }
  return -1;
}

// データ行から ISBN列 を推定（97[89]13桁が入ってる列）
function inferIsbnIndex(lines, delim){
  const scores = new Map(); // idx -> count
  const N = Math.min(lines.length, 200);
  for(let i=0;i<N;i++){
    const cols = splitCols(lines[i], delim);
    for(let c=0;c<cols.length;c++){
      const m = String(cols[c] || "").match(/\b(97[89]\d{10})\b/);
      if(m){
        scores.set(c, (scores.get(c) || 0) + 1);
      }
    }
  }
  let best = -1, bestScore = 0;
  for(const [idx, sc] of scores.entries()){
    if(sc > bestScore){
      bestScore = sc;
      best = idx;
    }
  }
  return best;
}

// データ行から label列 を推定：whitelistワードが一番多くヒットする列
function inferLabelIndex(lines, delim, whitelistNormed, isbnIdx){
  const hit = new Map(); // idx -> hits
  const N = Math.min(lines.length, 400);
  for(let i=0;i<N;i++){
    const cols = splitCols(lines[i], delim);
    for(let c=0;c<cols.length;c++){
      if(c === isbnIdx) continue;
      const cell = normKey(cols[c] || "");
      if(!cell) continue;
      // whitelistのどれかを含むか（重いので “どれか1つでも” で1点）
      if(whitelistNormed.some(k => k && cell.includes(k))){
        hit.set(c, (hit.get(c) || 0) + 1);
      }
    }
  }
  let best = -1, bestScore = 0;
  for(const [idx, sc] of hit.entries()){
    if(sc > bestScore){
      bestScore = sc;
      best = idx;
    }
  }
  return best;
}

function buildIsbnToLabel(text, whitelist){
  const linesAll = String(text || "").split(/\r?\n/).map(s => s.trimEnd());
  const lines = linesAll.filter(l => l && l.length > 0);

  if(lines.length === 0) return new Map();

  const delim = detectDelimiter(lines);
  if(!delim){
    // 区切れない場合：label_onlyにできないので空にする（既存を壊さない設計で回避）
    console.log("WARN: delimiter not detected -> cannot label_only");
    return new Map();
  }

  let start = 0;
  let headerCols = null;

  if(looksLikeHeader(lines[0])){
    headerCols = splitCols(lines[0], delim);
    start = 1;
  }

  const dataLines = lines.slice(start);

  const isbnIdx = inferIsbnIndex(dataLines, delim);
  if(isbnIdx === -1){
    console.log("WARN: ISBN column not inferred");
    return new Map();
  }

  let labelIdx = -1;
  if(headerCols){
    labelIdx = findLabelIndexFromHeader(headerCols);
  }
  if(labelIdx === -1){
    labelIdx = inferLabelIndex(dataLines, delim, whitelist.normed, isbnIdx);
  }

  if(labelIdx === -1){
    console.log("WARN: Label column not inferred");
    return new Map();
  }

  console.log("TRC inferred columns:", { isbnIdx, labelIdx, delim: delim === "\t" ? "TAB" : delim });

  const map = new Map(); // isbn -> labelText
  for(const line of dataLines){
    const cols = splitCols(line, delim);
    const isbn = normIsbn(cols[isbnIdx] || "");
    const label = String(cols[labelIdx] || "").trim();

    if(!isbn || isbn.length < 10) continue;
    if(!label) continue;                 // label_only：ラベル無しは捨てる
    if(!passesLabelOnly(whitelist, label)) continue;

    if(!map.has(isbn)) map.set(isbn, label);
    if(map.size >= TAKE) break;
  }

  return map;
}

async function main(){
  await fs.mkdir("data", { recursive: true });

  const whitelist = await readWhitelist();

  const zipUrl = await findLatestTrcZipUrl();
  const tmpZip = "data/_trc_latest.zip";
  await fetchBinToFile(zipUrl, tmpZip);

  const { text } = await unzipPickTextLike(tmpZip);

  // ISBN→label（レーベルonlyでここで絞る）
  const isbnToLabel = buildIsbnToLabel(text, whitelist);
  const isbns = Array.from(isbnToLabel.keys()).slice(0, TAKE);

  if(isbns.length === 0){
    console.log("OK: 0 items (no label-matched isbns) -> keep existing books.json");
    return; // 0日は更新しない
  }

  const openbd = await openbdBatch(isbns);

  const newItems = [];
  for(let i=0;i<openbd.length;i++){
    const b = pickFromOpenbd(openbd[i], isbns[i]);
    if(!b) continue;

    // label_onlyなので、TRC側で通ったISBNだけがここに来てる（念のため存在チェック）
    if(!isbnToLabel.has(b.isbn)) continue;

    newItems.push(b);
    if(newItems.length >= MAX_KEEP) break;
  }

  if(newItems.length === 0){
    console.log("OK: 0 items (openBD had no usable entries) -> keep existing books.json");
    return; // 0日は更新しない
  }

  const oldItems = await readExistingBooks();
  const merged = rocketPencilMerge(newItems, oldItems);

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: merged
  };

  await fs.writeFile(BOOKS_PATH, JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", merged.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
