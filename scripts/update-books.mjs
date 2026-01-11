// scripts/update-books.mjs
// TRC新刊TSV(zip) → ISBN抽出 → openBD → whitelist(レーベル一致のみ) → data/books.json

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";
const AMAZON_TAG = "mozublo-22";
const WHITELIST_PATH = "data/whitelist.json";

function uniq(arr){ return [...new Set(arr)]; }
function normIsbn(s){ return String(s||"").replace(/[^0-9Xx]/g,"").toUpperCase(); }
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

async function readWhitelist(){
  const raw = await fs.readFile(WHITELIST_PATH, "utf-8");
  const json = JSON.parse(raw);

  const labels = Array.isArray(json.label_includes_any) ? json.label_includes_any : [];
  if(labels.length === 0) throw new Error("whitelist label_includes_any is empty");

  return { labels };
}

function includesAny(text, list){
  if(!text) return false;
  return list.some(k => text.includes(k));
}

async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");
  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipFirstDataFile(zipPath){
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  const pick = names.find(n => {
    const x = n.toLowerCase();
    return x.endsWith(".tsv") || x.endsWith(".txt") || x.endsWith(".csv");
  });
  if(!pick) throw new Error("data file not found in zip");

  const { stdout } = await execFileAsync(
    "unzip",
    ["-p", zipPath, pick],
    { maxBuffer: 40 * 1024 * 1024 }
  );
  return stdout;
}

function extractIsbnsFromTsv(tsvText){
  const lines = tsvText.split(/\r?\n/).filter(Boolean);
  if(lines.length === 0) return [];

  const headerLine = lines[0];
  const sep = headerLine.includes("\t") ? "\t" : ",";
  const header = headerLine.split(sep).map(s => s.trim());

  let isbnIdx = header.findIndex(h =>
    /isbn/i.test(h) ||
    /isbn13/i.test(h) ||
    /国際標準図書番号/.test(h)
  );

  if(isbnIdx === -1){
    const maxCols = Math.max(...lines.slice(0, 200).map(l => l.split(sep).length));
    const scores = new Array(maxCols).fill(0);
    for(const line of lines.slice(1, 200)){
      const cols = line.split(sep);
      for(let i=0;i<maxCols;i++){
        const v = normIsbn(cols[i]);
        if(v.length === 13 && (v.startsWith("978") || v.startsWith("979"))) scores[i] += 3;
        else if(v.length === 10) scores[i] += 1;
      }
    }
    const best = scores.map((s,i)=>({s,i})).sort((a,b)=>b.s-a.s)[0];
    if(!best || best.s < 5) throw new Error("ISBN column not found");
    isbnIdx = best.i;
  }

  const isbns = [];
  for(let i=1;i<lines.length;i++){
    const cols = lines[i].split(sep);
    const isbn = normIsbn(cols[isbnIdx]);
    if(isbn && isbn.length >= 10) isbns.push(isbn);
  }
  return uniq(isbns);
}

async function openbdBatch(isbns){
  const url = OPENBD + encodeURIComponent(isbns.join(","));
  return await fetchJson(url);
}

function readLabelFromOpenbd(entry){
  if(!entry) return "";
  const s = entry.summary || {};
  // openBDのレーベル/シリーズはデータ源で揺れるので、拾えるところを広めに見る
  return String(
    s.series || s.label || s.imprint || s.publisher || ""
  );
}

function pickItem(entry, fallbackIsbn){
  if(!entry) return null;
  const s = entry.summary || {};
  const isbn = normIsbn(s.isbn || fallbackIsbn);
  const title = s.title || "";
  if(!isbn || !title) return null;

  const cover = (s.cover && s.cover.trim() !== "") ? s.cover : null;
  return { isbn, title, cover, amazon: amazonUrlFromIsbn(isbn) };
}

async function main(){
  const { labels } = await readWhitelist();

  const zipUrl = await findLatestTrcZipUrl();
  const tmpZip = "data/_trc_latest.zip";

  await fs.mkdir("data", { recursive: true });
  await fetchBinToFile(zipUrl, tmpZip);

  const raw = await unzipFirstDataFile(tmpZip);
  const isbnsAll = extractIsbnsFromTsv(raw);

  // レーベル一致のみなので取りこぼす → 多めに取る
  const take = 400;
  const isbns = isbnsAll.slice(0, take);

  const openbd = await openbdBatch(isbns);

  const items = [];
  for(let i=0;i<openbd.length;i++){
    const entry = openbd[i];
    const labelText = readLabelFromOpenbd(entry);
    if(!includesAny(labelText, labels)) continue;

    const it = pickItem(entry, isbns[i]);
    if(!it) continue;

    // labelはデバッグ用に残す（表示側で使ってもOK）
    it.label = labelText;
    items.push(it);

    if(items.length >= 8) break; // 8件集まったら終了
  }

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    mode: "label_only",
    items
  };

  await fs.writeFile("data/books.json", JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", out.items.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
