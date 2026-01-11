// scripts/update-books.mjs
// 目的：TRC新刊(zip) → ISBN抽出 → openBDでタイトル/書影取得 → data/books.json更新（ロケットえんぴつ）
// 方針：
// - whitelist は data/whitelist.json のみ参照（コード内に直書きしない）
// - cover が null でも捨てない（フロント側で NO IMAGE を出す）
// - items=0 の日は books.json を更新しない（棚を真っ白にしない）
// - 新規が少ない日は「新規分だけ先頭に追加→8件に丸める」（ロケットえんぴつ）

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";
const AMAZON_TAG = "mozublo-22";

const WHITELIST_PATH = "data/whitelist.json";
const BOOKS_PATH = "data/books.json";

const MAX_KEEP = 8;     // 棚は常に 8
const TAKE = 1200;      // TRCから多めに拾う（ラノベが薄い日でも8埋めるため）

function uniq(arr){ return [...new Set(arr)]; }
function normIsbn(s){ return String(s||"").replace(/[^0-9Xx]/g,"").toUpperCase(); }

function amazonUrlFromIsbn(isbn){
  return `https://www.amazon.co.jp/s?k=${encodeURIComponent(isbn)}&tag=${encodeURIComponent(AMAZON_TAG)}`;
}

// 文字ゆれ吸収：全角→半角っぽく寄せ、空白/記号を落として比較用にする
function normKey(s){
  return String(s ?? "")
    .toLowerCase()
    .replace(/[　\s]/g, "")                 // 全角/半角スペース除去
    .replace(/[！!]/g, "!")
    .replace(/[：:]/g, ":")
    .replace(/[・･]/g, "")
    .replace(/[’'"]/g, "")
    .replace(/[（）()\[\]【】「」『』]/g, "")
    .replace(/[‐-–—―ー\-]/g, "-")           // ハイフン/長音を寄せる
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

  // TRCページは新しいのが上に来る前提で「最初のzip」を採用
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");

  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipPickTextLike(zipPath){
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  // 優先：*.tsv → *.txt
  const pick =
    names.find(n => n.toLowerCase().endsWith(".tsv")) ||
    names.find(n => n.toLowerCase().endsWith(".txt")) ||
    names[0];

  if(!pick) throw new Error("No file in zip");

  // 中身をstdoutで取る
  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, pick], { maxBuffer: 50 * 1024 * 1024 });
  console.log("ZIP CONTENTS picked:", pick);
  return { name: pick, text: stdout };
}

// 形式が変わっても最低ISBNは拾えるよう、まず「ISBNっぽい13桁」を総当たりで拾う
function extractIsbnsLoose(text){
  const m = [...String(text).matchAll(/\b(97[89]\d{10})\b/g)].map(x => normIsbn(x[1]));
  return uniq(m);
}

async function openbdBatch(isbns){
  const url = OPENBD + encodeURIComponent(isbns.join(","));
  return await fetchJson(url);
}

// openBDの1冊を棚用に整形
// ★重要：cover が null でも捨てない（フロントで NO IMAGE）
function pickFromOpenbd(entry, fallbackIsbn){
  const isbn = normIsbn(entry?.summary?.isbn || fallbackIsbn);
  const title = entry?.summary?.title || "";
  const cover = entry?.summary?.cover || ""; // null → "" にする
  if(!isbn || !title) return null;
  return { isbn, title, cover, amazon: amazonUrlFromIsbn(isbn) };
}

async function readWhitelist(){
  const raw = await fs.readFile(WHITELIST_PATH, "utf-8");
  const json = JSON.parse(raw);
  const list = Array.isArray(json.label_includes_any) ? json.label_includes_any : [];
  const normed = list.map(normKey).filter(Boolean);
  return { raw: list, normed };
}

// whitelist判定（label_only想定）
// ※TRC側の「レーベル文字列」をここに渡す前提。
// 今回は安全側として openBD title にも当てて「拾える可能性」を残す（=0日を減らす）
function passesWhitelist(wh, labelText, titleText){
  const hay = normKey(`${labelText || ""} ${titleText || ""}`);
  if(!hay) return false;
  return wh.normed.some(k => k && hay.includes(k));
}

async function readExistingBooks(){
  try{
    const raw = await fs.readFile(BOOKS_PATH, "utf-8");
    const json = JSON.parse(raw);
    const items = Array.isArray(json.items) ? json.items : [];
    return items;
  }catch{
    return [];
  }
}

// ロケットえんぴつ：new → old をつなげて ISBN で重複除去し、先頭から8件
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

async function main(){
  await fs.mkdir("data", { recursive: true });

  // whitelist 読み込み（存在前提）
  const whitelist = await readWhitelist();

  // TRC zip → 中身テキスト
  const zipUrl = await findLatestTrcZipUrl();
  const tmpZip = "data/_trc_latest.zip";
  await fetchBinToFile(zipUrl, tmpZip);

  const { text } = await unzipPickTextLike(tmpZip);

  // ISBNを多めに拾う（ゆるく）
  const isbnsAll = extractIsbnsLoose(text);
  const isbns = isbnsAll.slice(0, TAKE);

  if(isbns.length === 0){
    console.log("OK: 0 items (no isbns)");
    return;
  }

  // openBD
  const openbd = await openbdBatch(isbns);

  const picked = [];
  for(let i=0;i<openbd.length;i++){
    const b = pickFromOpenbd(openbd[i], isbns[i]);
    if(!b) continue;

    // whitelist 判定
    // ※本来は TRCのレーベル/シリーズ欄を使うのが理想だが、
    //   形式揺れで落ちると items=0 が増えるので、まずは title を含めて判定して安定化させる。
    if(!passesWhitelist(whitelist, "", b.title)) continue;

    picked.push(b);
    if(picked.length >= MAX_KEEP) break;
  }

  // 0件の日は books.json を更新しない（棚を真っ白にしない）
  if(picked.length === 0){
    console.log("OK: 0 items (filtered) -> keep existing books.json");
    return;
  }

  const oldItems = await readExistingBooks();
  const mergedItems = rocketPencilMerge(picked, oldItems);

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: mergedItems
  };

  await fs.writeFile(BOOKS_PATH, JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", mergedItems.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
