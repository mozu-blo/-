// scripts/update-books.mjs
// 目的：新刊RSS → ISBN抽出 → openBDで書影/タイトル取得 → data/books.json更新
// AmazonはPA-APIなしで検索URL + tag 方式（追跡ID）

import fs from "node:fs/promises";

const RSS_URL = "http://www.hanmoto.com/bd/shinkan/feed/";
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

function extractIsbnsFromRss(xml){
  // 版元ドットコムのRSSは /bd/ISBN978-...html が多いのでそこを拾う
  const a = [...xml.matchAll(/\/bd\/ISBN([0-9][0-9\-]{10,20}[0-9Xx])\.html/g)].map(m => normIsbn(m[1]));
  // 念のため本文の ISBN: 978-... も拾う
  const b = [...xml.matchAll(/ISBN[^0-9]*([0-9][0-9\-]{10,20}[0-9Xx])/g)].map(m => normIsbn(m[1]));
  return uniq([...a, ...b]).filter(x => x.length >= 10);
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
  const rss = await fetchText(RSS_URL);
  const isbnsAll = extractIsbnsFromRss(rss);

  // null混入を見越して多めに取得（棚は8冊）
  const take = 40;
  const isbns = isbnsAll.slice(0, take);

  const openbd = await openbdBatch(isbns);

  const items = [];
  for(let i=0;i<openbd.length;i++){
    const b = pick(openbd[i], isbns[i]);
    if(b) items.push(b);
  }

  // “ラノベっぽい語”が入るものを少し優先（強すぎると0件になるので軽め）
  const keys = ["ライトノベル","ラノベ","文庫","ノベル","小説"];
  const score = (t)=> keys.reduce((a,k)=>a+(t.includes(k)?1:0),0);
  items.sort((a,b)=> score(b.title) - score(a.title));

  const out = {
    generated_at: new Date().toISOString(),
    source: RSS_URL,
    items: items.slice(0, 8)
  };

  await fs.mkdir("data", { recursive: true });
  await fs.writeFile("data/books.json", JSON.stringify(out, null, 2), "utf-8");

  console.log("OK:", out.items.length, "items");
}

main().catch(e => {
  console.error("ERROR:", e);
  process.exit(1);
});
