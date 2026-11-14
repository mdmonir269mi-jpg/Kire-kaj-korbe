import asyncio, html, json, logging, math, os, random, re, threading, time
from http.server import BaseHTTPRequestHandler, HTTPServer
import httpx
from dotenv import load_dotenv
from telegram import InlineKeyboardButton as B, InlineKeyboardMarkup as M, ReplyKeyboardMarkup, Update
from telegram.error import BadRequest
try:  # নতুন PTB-তে সরাসরি আছে
    from telegram import CopyTextButton
except ImportError:
    CopyTextButton = None
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv("zebra_env.txt")
BOT_TOKEN = os.environ["BOT_TOKEN"]
KEY = os.environ["ZEBRA_KEY"]
BASE = os.environ.get("ZEBRA_BASE", "https://api.zebrasms.com/api/v1/publicapi").rstrip("/")
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
SUPPORT = os.environ.get("SUPPORT", "@Monir12255")
BLOCKED_PREFIXES = [x for x in os.environ.get("BLOCKED_PREFIXES", "44").replace(" ", "").split(",") if x]

# বট সার্ভিস -> (বাটনের নাম, liveaccess-এর sender)
SERVICES = {
    "fb": ("📘 FACEBOOK", "facebook"),
    "ig": ("📸 INSTAGRAM", "instagram"),
    "wa": ("💬 WHATSAPP", "whatsapp"),
    "tg": ("✈️ TELEGRAM", "telegram"),
}
OTP_GROUP_URL = os.environ.get("OTP_GROUP_URL", "").strip()   # OTP গ্রুপ/চ্যানেলের লিংক (ঐচ্ছিক)
OWNER_URL = os.environ.get("OWNER_URL", f"https://t.me/{SUPPORT.lstrip('@')}")
KEEPALIVE_URL = os.environ.get("KEEPALIVE_URL") or os.environ.get("RENDER_EXTERNAL_URL")
KEEP_EVERY = 240          # সেকেন্ড (Render ১৫ মিনিট চুপ থাকলে ঘুমায়)
TOP_N = 3
LIVE_EVERY = 60            # liveaccess cache ৬০ সেকেন্ড
POLL_EVERY, POLL_MAX = 4, 20 * 60
TRY_RANGES = 4
CODES = {  # কান্ট্রি কোড -> (নাম, ISO)
 "1":("USA/Canada","US"),"7":("Russia","RU"),"20":("Egypt","EG"),"27":("South Africa","ZA"),"30":("Greece","GR"),"31":("Netherlands","NL"),
 "32":("Belgium","BE"),"33":("France","FR"),"34":("Spain","ES"),"36":("Hungary","HU"),"39":("Italy","IT"),"40":("Romania","RO"),"41":("Switzerland","CH"),
 "43":("Austria","AT"),"44":("United Kingdom","GB"),"45":("Denmark","DK"),"46":("Sweden","SE"),"47":("Norway","NO"),"48":("Poland","PL"),"49":("Germany","DE"),
 "51":("Peru","PE"),"52":("Mexico","MX"),"54":("Argentina","AR"),"55":("Brazil","BR"),"56":("Chile","CL"),"57":("Colombia","CO"),"58":("Venezuela","VE"),
 "60":("Malaysia","MY"),"61":("Australia","AU"),"62":("Indonesia","ID"),"63":("Philippines","PH"),"64":("New Zealand","NZ"),"65":("Singapore","SG"),"66":("Thailand","TH"),
 "81":("Japan","JP"),"82":("South Korea","KR"),"84":("Vietnam","VN"),"86":("China","CN"),"90":("Turkey","TR"),"91":("India","IN"),"92":("Pakistan","PK"),
 "93":("Afghanistan","AF"),"94":("Sri Lanka","LK"),"95":("Myanmar","MM"),"98":("Iran","IR"),"212":("Morocco","MA"),"213":("Algeria","DZ"),"216":("Tunisia","TN"),
 "218":("Libya","LY"),"220":("Gambia","GM"),"221":("Senegal","SN"),"222":("Mauritania","MR"),"223":("Mali","ML"),"224":("Guinea","GN"),"225":("Ivory Coast","CI"),
 "226":("Burkina Faso","BF"),"227":("Niger","NE"),"228":("Togo","TG"),"229":("Benin","BJ"),"230":("Mauritius","MU"),"231":("Liberia","LR"),"232":("Sierra Leone","SL"),
 "233":("Ghana","GH"),"234":("Nigeria","NG"),"235":("Chad","TD"),"236":("Central African Rep.","CF"),"237":("Cameroon","CM"),"238":("Cape Verde","CV"),
 "240":("Equatorial Guinea","GQ"),"241":("Gabon","GA"),"242":("Congo","CG"),"243":("DR Congo","CD"),"244":("Angola","AO"),"245":("Guinea-Bissau","GW"),
 "249":("Sudan","SD"),"250":("Rwanda","RW"),"251":("Ethiopia","ET"),"252":("Somalia","SO"),"253":("Djibouti","DJ"),"254":("Kenya","KE"),"255":("Tanzania","TZ"),
 "256":("Uganda","UG"),"257":("Burundi","BI"),"258":("Mozambique","MZ"),"260":("Zambia","ZM"),"261":("Madagascar","MG"),"263":("Zimbabwe","ZW"),"264":("Namibia","NA"),
 "265":("Malawi","MW"),"266":("Lesotho","LS"),"267":("Botswana","BW"),"268":("Eswatini","SZ"),"351":("Portugal","PT"),"353":("Ireland","IE"),"358":("Finland","FI"),
 "359":("Bulgaria","BG"),"374":("Armenia","AM"),"375":("Belarus","BY"),"380":("Ukraine","UA"),"381":("Serbia","RS"),"420":("Czechia","CZ"),"421":("Slovakia","SK"),
 "508":("Saint Pierre & Miquelon","PM"),"855":("Cambodia","KH"),"856":("Laos","LA"),"880":("Bangladesh","BD"),"960":("Maldives","MV"),"962":("Jordan","JO"),
 "963":("Syria","SY"),"964":("Iraq","IQ"),"965":("Kuwait","KW"),"966":("Saudi Arabia","SA"),"967":("Yemen","YE"),"968":("Oman","OM"),"971":("UAE","AE"),
 "972":("Israel","IL"),"973":("Bahrain","BH"),"974":("Qatar","QA"),"977":("Nepal","NP"),"992":("Tajikistan","TJ"),"993":("Turkmenistan","TM"),
 "994":("Azerbaijan","AZ"),"995":("Georgia","GE"),"996":("Kyrgyzstan","KG"),"998":("Uzbekistan","UZ"),
}

logging.basicConfig(level=logging.INFO)
http = httpx.AsyncClient(timeout=20)
live: dict[str, dict] = {}          # service -> {range: hits}
watchers: dict[str, dict] = {}
chat_number: dict[int, str] = {}
last_raw_log = 0.0

# ---------- Zebra API (সফল হলে meta.code == 0) ----------
async def zapi(method, path, params=None, body=None):
    r = await http.request(method, f"{BASE}/{path}", headers={"MAuth": KEY}, params=params, json=body)
    try:
        j = r.json()
    except Exception:
        r.raise_for_status(); raise RuntimeError("JSON পাওয়া যায়নি")
    meta = j.get("meta") or {}
    if meta.get("code", 0) != 0:
        raise RuntimeError(f"{meta.get('code')}: {j.get('message') or meta.get('error') or 'error'}")
    r.raise_for_status()
    return j.get("data", j)

def digits(x): return re.sub(r"\D", "", str(x))
def flag(code):
    return "".join(chr(0x1F1E6 + ord(c) - 65) for c in code) if len(code) == 2 and code.isalpha() else "🌍"
def country_of(rng):
    d = digits(rng)
    for n in (3, 2, 1):
        if d[:n] in CODES: return CODES[d[:n]]
    return ("Unknown", "??")
def blocked(rng): return any(digits(rng).startswith(p) for p in BLOCKED_PREFIXES)

# ---------- লাইভ range (ট্রাফিক অনুযায়ী) ----------
async def live_loop():
    while True:
        for key, (_, sender) in SERVICES.items():
            try:
                d = await zapi("GET", "liveaccess", params={"sender": sender})
                ranges: dict[str, int] = {}
                for row in d.get("rows", []):
                    rs = row.get("ranges") or []
                    per = max(1, math.ceil(int(row.get("count", 1) or 1) / max(1, len(rs))))
                    for r in rs:
                        if not blocked(r): ranges[r] = ranges.get(r, 0) + per
                if ranges: live[key] = ranges          # খালি এলে আগেরটা রেখে দেয়
                logging.info("live %s: %d ranges", key, len(ranges))
            except Exception as e:
                logging.warning("liveaccess %s: %s", key, e)
            await asyncio.sleep(2)
        await asyncio.sleep(LIVE_EVERY)

def country_groups(key):
    groups = {}
    for rng, hits in live.get(key, {}).items():
        name, iso = country_of(rng)
        g = groups.setdefault(iso, {"name": name, "ranges": [], "hits": 0})
        g["ranges"].append(rng); g["hits"] += hits
    return sorted(groups.items(), key=lambda kv: -kv[1]["hits"])

async def get_number(key, iso):
    g = dict(country_groups(key)).get(iso)
    if not g: raise RuntimeError("এই দেশে এখন range নেই")
    cands = sorted(g["ranges"], key=lambda r: -live[key].get(r, 0) * random.random())
    last = None
    for rng in cands[:TRY_RANGES]:
        try:
            d = await zapi("POST", "getnum", body={"range": rng})
            rows = d.get("rows") or []
            if rows and rows[0].get("number"):
                return rows[0]
            last = RuntimeError("নাম্বার পাওয়া যায়নি")
        except Exception as e:
            last = e; logging.warning("getnum %s: %s", rng, e)
    raise last or RuntimeError("নাম্বার পাওয়া যায়নি")

# ---------- OTP (getupdate, সব ইউজারের জন্য একটাই পোলার) ----------
def first(d, keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""): return d[k]
    return None

def find_list(j):
    if isinstance(j, list): return j
    if isinstance(j, dict):
        for k in ("rows", "data", "messages", "updates", "results", "items", "sms"):
            if k in j:
                r = find_list(j[k])
                if r is not None: return r
    return None

def to_ms(v):
    try:
        v = float(v)
    except Exception:
        return None
    return v * 1000 if v < 1e11 else v

def parse_updates(d):
    out = []
    for it in find_list(d) or []:
        num = first(it, ("number", "phone", "to", "msisdn", "recipient"))
        txt = first(it, ("text", "message", "body", "sms", "content", "otp", "code"))
        if num is None or txt is None: continue
        ts = to_ms(first(it, ("at_ms", "time_ms", "timestamp", "ts", "time", "received_at_ms")))
        uid = first(it, ("id", "uuid", "message_id")) or f"{num}:{txt}:{ts}"
        out.append((digits(num), str(txt), str(uid), ts))
    return out

def add_watcher(chat_id, number, key="fb", iso="??", country=""):
    drop_watcher(chat_id)
    n = digits(number)
    watchers[n] = {"key": key, "iso": iso, "country": country, "chat": chat_id, "start": time.time() * 1000 - 5000, "expires": time.time() + POLL_MAX, "seen": set(), "got": False}
    chat_number[chat_id] = n

def drop_watcher(chat_id):
    n = chat_number.pop(chat_id, None)
    if n: watchers.pop(n, None)

async def otp_loop(app):
    global last_raw_log
    while True:
        try:
            if watchers:
                d = await zapi("GET", "getupdate")
                items = parse_updates(d)
                if not items and time.time() - last_raw_log > 60:
                    last_raw_log = time.time(); logging.info("getupdate raw: %s", json.dumps(d, ensure_ascii=False)[:600])
                for num, txt, uid, ts in items:
                    w = watchers.get(num)
                    if not w or uid in w["seen"]: continue
                    if ts is not None and ts < w["start"]: continue     # নাম্বার নেওয়ার আগের মেসেজ বাদ
                    w["seen"].add(uid); w["got"] = True
                    m = re.search(r"\b\d{4,8}\b", txt)
                    code = m.group(0) if m else None
                    lines = ["🔥 <b>100% OTP RECEIVE</b>",
                             f"{flag(w['iso'])} {w['iso']} | {svc_emoji(w['key'])} <code>+{num}</code> | 🔴 <b>LIVE</b>"]
                    if not code:
                        lines += ["", f"💬 {html.escape(txt)}"]
                    rows = []
                    if code: rows.append([copy_btn(f"✅ 📋 {code}", code)])
                    rows.append(url_row(("👤 OWNER", OWNER_URL), ("✅ OTP GROUP", OTP_GROUP_URL)))
                    try:
                        await app.bot.send_message(w["chat"], "\n".join(lines), parse_mode="HTML", reply_markup=M([r for r in rows if r]))
                    except Exception as e:
                        logging.warning("send: %s", e)
        except Exception as e:
            logging.warning("getupdate error: %s", e)
        now = time.time()
        for n, w in list(watchers.items()):
            if now > w["expires"]:
                watchers.pop(n, None); chat_number.pop(w["chat"], None)
                if not w["got"]:
                    try: await app.bot.send_message(w["chat"], "⌛ সময় শেষ, OTP আসেনি। নতুন নাম্বার নিন।")
                    except Exception: pass
        await asyncio.sleep(POLL_EVERY if watchers else 10)

# ---------- UI ----------
def copy_btn(label, text):
    """ট্যাপ করলেই কপি হয় (Telegram copy_text বাটন)"""
    if CopyTextButton is not None:
        return B(label, copy_text=CopyTextButton(text=text))
    return B(label, api_kwargs={"copy_text": {"text": text}})

def url_row(*pairs):
    return [B(t, url=u) for t, u in pairs if u]

def svc_emoji(key): return SERVICES[key][0].split()[0]
def svc_name(key): return SERVICES[key][0].split(" ", 1)[1].title()

MAIN_KB = ReplyKeyboardMarkup([["📱 GET NUMBER"], ["🆘 SUPPORT"]], resize_keyboard=True)

def service_kb():
    rows = [[B(v[0], callback_data=f"svc:{k}")] for k, v in SERVICES.items()]
    rows.append([B("❌ CLOSE", callback_data="close")])
    return M(rows)

def country_view(key):
    groups = country_groups(key)[:TOP_N]
    name = SERVICES[key][0]
    if not groups:
        return f"❌ {name}-এর জন্য এখন কোনো চালু range নেই।\nসাপোর্ট: {SUPPORT}", M([[B("⬅️ BACK", callback_data="back")]])
    lines = [f"🌍 Select a country for {name}:", "", f"Top {len(groups)} countries by hits"]; btns = []
    for i, (iso, g) in enumerate(groups, 1):
        fire = " 🔥" if i == 1 else ""
        lines.append(f"{i}. {flag(iso)} {g['name']} ({iso}) • {g['hits']} hits{fire}")
        btns.append(B(f"{flag(iso)} {iso} · {g['hits']}", callback_data=f"cty:{key}:{iso}"))
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)] + [[B("⬅️ BACK", callback_data="back")]]
    return "\n".join(lines), M(rows)

async def safe_edit(q, text, **kw):
    try:
        await q.edit_message_text(text, **kw)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            await q.message.reply_text(text, **kw)

async def start(u: Update, c): await u.message.reply_text("স্বাগতম! নিচের মেনু থেকে বেছে নিন 👇", reply_markup=MAIN_KB)
async def menu(u: Update, c): await u.message.reply_text("📍 Select a service:", reply_markup=service_kb())
async def support(u: Update, c): await u.message.reply_text(f"🆘 সাপোর্ট: {SUPPORT}")
async def my_id(u: Update, c): await u.message.reply_text(f"আপনার id: {u.effective_user.id}")

async def test(u: Update, c):
    if u.effective_user.id not in ADMIN_IDS:
        await u.message.reply_text("শুধু অ্যাডমিন। ADMIN_IDS-এ আপনার id বসান (/id)।"); return
    for name, path, params in (("getupdate", "getupdate", None), ("liveaccess(facebook)", "liveaccess", {"sender": "facebook"})):
        try:
            d = await zapi("GET", path, params=params)
            await u.message.reply_text(f"[{name}]\n{json.dumps(d, ensure_ascii=False)[:1300]}")
        except Exception as e:
            await u.message.reply_text(f"[{name}] ❌ {e}")
    await u.message.reply_text("live: " + ", ".join(f"{k}={len(v)}" for k, v in live.items()))

async def on_cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query; await q.answer()
    chat = q.message.chat_id
    if q.data == "close":
        await q.message.delete(); return
    if q.data == "back":
        drop_watcher(chat); await safe_edit(q, "📍 Select a service:", reply_markup=service_kb()); return
    if q.data.startswith("svc:"):
        drop_watcher(chat); text, kb = country_view(q.data[4:]); await safe_edit(q, text, reply_markup=kb); return
    if q.data.startswith("cty:"):
        _, key, iso = q.data.split(":"); drop_watcher(chat)
        try:
            row = await get_number(key, iso)
        except Exception as e:
            text, kb = country_view(key); await safe_edit(q, f"❌ {e}\n\n" + text, reply_markup=kb); return
        num = digits(row["number"])
        iso2 = str(row.get("iso2") or iso).upper(); cname = row.get("country", "")
        text = (f"{svc_emoji(key)} <b>{svc_name(key)} {html.escape(cname)}</b>\n"
                f"{flag(iso2)} {html.escape(cname)} ({iso2})\n"
                f"📶 {html.escape(str(row.get('operator', '')))}\n\n"
                f"⏳ OTP-র জন্য অপেক্ষা করছি...")
        rows = [[copy_btn(f"{flag(iso2)} 📋 +{num}", f"+{num}")],
                [B("🔄 CHANGE NUMBER", callback_data=f"cty:{key}:{iso}")] + url_row(("✅ OTP GROUP", OTP_GROUP_URL)),
                [B("⬅️ Back", callback_data=f"svc:{key}"), B("❌ Close", callback_data="close")]]
        await safe_edit(q, text, parse_mode="HTML", reply_markup=M(rows))
        add_watcher(chat, num, key, iso2, cname)

# ---------- Render ----------
def start_health_server():
    port = os.environ.get("PORT")
    if not port: return
    class H(BaseHTTPRequestHandler):
        def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        do_HEAD = do_GET
        def log_message(self, *a): pass
    threading.Thread(target=HTTPServer(("0.0.0.0", int(port)), H).serve_forever, daemon=True).start()

async def keepalive():
    """নিজের URL-এ নিয়মিত ঢুকে Render-কে জাগিয়ে রাখে"""
    while KEEPALIVE_URL:
        await asyncio.sleep(KEEP_EVERY)
        try:
            await http.get(KEEPALIVE_URL)
        except Exception as e:
            logging.warning("keepalive: %s", e)

async def post_init(app):
    asyncio.create_task(live_loop()); asyncio.create_task(otp_loop(app)); asyncio.create_task(keepalive())

def build_and_run():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("id", my_id))
    app.add_handler(CommandHandler("test", test))
    app.add_handler(MessageHandler(filters.Regex("GET NUMBER$"), menu))
    app.add_handler(MessageHandler(filters.Regex("SUPPORT$"), support))
    app.add_handler(CallbackQueryHandler(on_cb))
    asyncio.set_event_loop(asyncio.new_event_loop())
    app.run_polling()

def main():
    start_health_server()
    while True:               # কোনো কারণে থেমে গেলে ৫ সেকেন্ডে নিজে আবার চালু হয়
        try:
            build_and_run()
            break
        except Exception as e:
            logging.error("বট থেমে গেছে, আবার চালু হচ্ছে: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
