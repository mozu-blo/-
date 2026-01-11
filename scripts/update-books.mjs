// scripts/update-books.mjs
// 目的：TRC新刊TSV(zip) → ISBN抽出 → openBDで書影/タイトル取得 → data/books.json更新
// AmazonはPA-APIなしで検索URL + tag 方式（追跡ID）

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";
const AMAZON_TAG = "mozublo-22";

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

async function findLatestTrcZipUrl(){
  const html = await fetchText(TRC_PAGE);

  // ページ内に zip へのリンクが複数あるので、とりあえず「最初の .zip」を最新として採用
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");

  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipFirstTsv(zipPath){
  // zip内のファイル名一覧
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  // ★デバッグ：実際の中身をログに出す（次の失敗で原因が確定できる）
  console.log("ZIP CONTENTS:", names);

  // ★ .tsv 以外（.txt/.csv）や、サブディレクトリ配下にも対応
  const pick = names.find(n => {
    const x = n.toLowerCase();
    return x.endsWith(".tsv") || x.endsWith(".txt") || x.endsWith(".csv");
  });

  if(!pick) throw new Error("TSV not found in zip");

  // 取得したファイル本文をstdoutで取得
  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, pick], { maxBuffer: 30 * 1024 * 1024 });
  return stdout;
}

function extractIsbnsFromTsv(tsvText){
  const lines = tsvText.split(/\r?\n/).filter(Boolean);
  if(lines.length === 0) return [];

  const header = lines[0].split("\t");
  const isbnIdx = header.findIndex(h => /isbn/i.test(h));
  if(isbnIdx === -1) throw new Error("ISBN column not found in TSV header");

  const isbns = [];
  for(let i=1;i<lines.length;i++){
    const cols = lines[i].split("\t");
    const isbn = normIsbn(cols[isbnIdx]);
    if(isbn && isbn.length >= 10) isbns.push(isbn);
  }
  return uniq(isbns);
}

async function openbdBatch(isbns){
  const url = OPENBD + encodeURIComponent(isbns.join(","));
  return await fetchJson(url);
}

function pick(entry, fallbackIsbn){
  if(!entry) return null;
  const s = entry.summary || {};
  const isbn = normIsbn(s.isbn || fallbackIsbn);
  const title = s.title || "";
  const cover = s.cover || "";
  if(!isbn || !title || !cover) return null;
  return { isbn, title, cover, amazon: amazonUrlFromIsbn(isbn) };
}

async function main(){
  const zipUrl = await findLatestTrcZipUrl();
  const tmpZip = "data/_trc_latest.zip";

  await fs.mkdir("data", { recursive: true });
  await fetchBinToFile(zipUrl, tmpZip);

  const tsv = await unzipFirstTsv(tmpZip);
  const isbnsAll = extractIsbnsFromTsv(tsv);

  // null混入を見越して多めに取得（棚は8冊）
  const take = 60;
  const isbns = isbnsAll.slice(0, take);

  const openbd = await openbdBatch(isbns);

  const items = [];
  for(let i=0;i<openbd.length;i++){
    const b = pick(openbd[i], isbns[i]);
    if(b) items.push(b);
  }

  // “ラノベっぽい語”が入るものを少し優先（軽め）
  const keys = ["ライトノベル","ラノベ","文庫","ノベル","小説"];
  const score = (t)=> keys.reduce((a,k)=>a+(t.includes(k)?1:0),0);
  items.sort((a,b)=> score(b.title) - score(a.title));

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: items.slice(0, 8)
  };

  await fs.writeFile("data/books.json", JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", out.items.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
