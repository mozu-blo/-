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

  // TRCは「TSVっぽいtxt」で、区切りがタブのことが多いが念のため判定
  const headerLine = lines[0];
  const sep = headerLine.includes("\t") ? "\t" : ",";

  const header = headerLine.split(sep).map(s => s.trim());

  // 1) まずはヘッダ名で探す（TRCの表記ゆれ対応）
  const isbnIdxByHeader = header.findIndex(h =>
    /isbn/i.test(h) ||
    /isbn13/i.test(h) ||
    /isbn_?13/i.test(h) ||
    /国際標準図書番号/.test(h) ||
    /標準図書番号/.test(h)
  );

  // 2) ヘッダ名で見つからなければ「実データを見てISBNっぽい列」を推定する
  let isbnIdx = isbnIdxByHeader;
  if(isbnIdx === -1){
    // 先頭200行くらいでスコアリング
    const maxCols = Math.max(...lines.slice(0, 200).map(l => l.split(sep).length));
    const scores = new Array(maxCols).fill(0);

    for(const line of lines.slice(1, 200)){
      const cols = line.split(sep);
      for(let i=0;i<maxCols;i++){
        const v = normIsbn(cols[i]);
        // ISBN-13(978/979) を強く加点、ISBN-10も少し加点
        if(v.length === 13 && (v.startsWith("978") || v.startsWith("979"))) scores[i] += 3;
        else if(v.length === 10) scores[i] += 1;
      }
    }

    // 一番スコアが高い列を採用（ただし最低限の得点を要求）
    const best = scores
      .map((s,i)=>({s,i}))
      .sort((a,b)=>b.s-a.s)[0];

    if(!best || best.s < 5){
      console.log("HEADER:", header);
      console.log("SCORES:", scores);
      throw new Error("ISBN column not found (by header nor by data)");
    }
    isbnIdx = best.i;
  }

  // 3) ISBN列から抽出
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

function pick(entry, fallbackIsbn){
  if(!entry) return null;
  const s = entry.summary || {};
  const isbn = normIsbn(s.isbn || fallbackIsbn);
  const title = s.title || "";

  if(!isbn || !title) return null;

  // coverが無い場合はnullにして通す
  const cover = s.cover && s.cover.trim() !== "" ? s.cover : null;

  return {
    isbn,
    title,
    cover,
    amazon: amazonUrlFromIsbn(isbn)
  };
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
