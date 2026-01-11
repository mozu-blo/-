// scripts/update-books.mjs
// 目的：TRC新刊データ(zip) → ISBN抽出 → openBDでタイトル/書影 → data/books.json更新
// NOTE: openBDのsummary.coverがnullでも、cover.openbd.jp/{isbn}.jpg を試す（無ければフロントでNO IMAGE）

import fs from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

// TRCオープンデータページ（ここから最新zipを拾う）
const TRC_PAGE = "https://www.trc.co.jp/trc_opendata/";

// openBD
const OPENBD = "https://api.openbd.jp/v1/get?isbn=";

// Amazon 検索URL（tag付き）
const AMAZON_TAG = "mozublo-22";

function uniq(arr){ return [...new Set(arr)]; }
function normIsbn(s){ return String(s||"").replace(/[^0-9Xx]/g,"").toUpperCase(); }

function amazonUrlFromIsbn(isbn){
  return `https://www.amazon.co.jp/s?k=${encodeURIComponent(isbn)}&tag=${encodeURIComponent(AMAZON_TAG)}`;
}

// openBDの代替書影（多くがここで引ける）
function openbdCoverFromIsbn(isbn){
  const n = normIsbn(isbn);
  return n ? `https://cover.openbd.jp/${encodeURIComponent(n)}.jpg` : "";
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

  // TRCページは新しいものが上に来る想定：最初に見つかったzipを採用
  const m = html.match(/href="([^"]+\.zip)"/i);
  if(!m) throw new Error("TRC zip link not found");

  return new URL(m[1], TRC_PAGE).toString();
}

async function unzipFirstTextOrTsv(zipPath){
  // zip内ファイル一覧
  const { stdout: list } = await execFileAsync("unzip", ["-Z1", zipPath]);
  const names = list.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  // TSVがあればTSV、無ければTXT（あなたのスクショだと .txt だった）
  const target =
    names.find(n => n.toLowerCase().endsWith(".tsv")) ||
    names.find(n => n.toLowerCase().endsWith(".txt"));

  if(!target) throw new Error("TSV/TXT not found in zip");

  const { stdout } = await execFileAsync("unzip", ["-p", zipPath, target], { maxBuffer: 30 * 1024 * 1024 });
  return { filename: target, text: stdout };
}

function extractIsbnsFromText(text){
  // まず「ISBNっぽい数字列」を全部拾う（区切り記号ありも吸収）
  const raw = [...text.matchAll(/(?:ISBN[^0-9]*)?([0-9][0-9\-\s]{10,20}[0-9Xx])/g)]
    .map(m => normIsbn(m[1]))
    .filter(x => x.length >= 10);

  // ISBN-13を優先（13桁）に寄せる
  const isbns = raw
    .map(x => x.replace(/\s+/g,""))
    .filter(x => x.length === 13 || x.length === 10);

  return uniq(isbns);
}

async function openbdBatch(isbns){
  // openBDはカンマ区切りで複数問い合わせ可能
  const url = OPENBD + encodeURIComponent(isbns.join(","));
  return await fetchJson(url);
}

function pick(entry, fallbackIsbn){
  // entry自体がnullのこともある
  const isbn = normIsbn((entry && entry.summary && entry.summary.isbn) || fallbackIsbn);
  if(!isbn) return null;

  const title = (entry && entry.summary && entry.summary.title) ? entry.summary.title : "";
  if(!title) return null;

  // coverはnullが多いので、無ければ cover.openbd.jp を採用
  const coverFromSummary = (entry && entry.summary) ? entry.summary.cover : "";
  const cover = (coverFromSummary && String(coverFromSummary).trim()) ? String(coverFromSummary).trim() : openbdCoverFromIsbn(isbn);

  return {
    isbn,
    title,
    cover, // 404の可能性はあるが、フロントでNO IMAGEに落とす
    amazon: amazonUrlFromIsbn(isbn)
  };
}

// ざっくり“ラノベ寄り”優先（強すぎると減るので軽め）
const keys = [
  "文庫","ライトノベル","ラノベ","ノベル","ファンタジア","電撃","スニーカー","MF","GA","HJ","オーバーラップ",
  "GCN","ガガガ","TOブックス","アース・スター","ツギクル","モンスター","ヒーロー","PASH","一迅社","サーガ"
];
const score = (t)=> keys.reduce((a,k)=>a+(String(t).includes(k)?1:0),0);

async function main(){
  const zipUrl = await findLatestTrcZipUrl();

  await fs.mkdir("data", { recursive: true });
  const tmpZip = "data/_trc_latest.zip";
  await fetchBinToFile(zipUrl, tmpZip);

  const { filename, text } = await unzipFirstTextOrTsv(tmpZip);
  console.log("ZIP CONTENTS picked:", filename);

  const isbnsAll = extractIsbnsFromText(text);

  // 棚は8冊。openBDのnull混入を考えて多めに取る
  const take = 120; // ←ここは後で増やす/減らすのが簡単
  const isbns = isbnsAll.slice(0, take);

  const openbd = await openbdBatch(isbns);

  const items = [];
  for(let i=0;i<openbd.length;i++){
    const b = pick(openbd[i], isbns[i]);
    if(b) items.push(b);
  }

  // ラノベ寄り優先
  items.sort((a,b)=> score(b.title) - score(a.title));

  // 8冊確保（重複ISBNを除去）
  const seen = new Set();
  const picked = [];
  for(const it of items){
    if(seen.has(it.isbn)) continue;
    seen.add(it.isbn);
    picked.push(it);
    if(picked.length >= 8) break;
  }

  const out = {
    generated_at: new Date().toISOString(),
    source: zipUrl,
    items: picked
  };

  await fs.writeFile("data/books.json", JSON.stringify(out, null, 2), "utf-8");
  console.log("OK:", out.items.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
