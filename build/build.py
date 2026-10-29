#!/usr/bin/env python3
"""Wikiあ preprocessor.

Reads a Kiwix/openZIM file (new namespace scheme, zstd clusters) directly
(own parser, no libzim needed), and writes:
  out/packs/pNNN.bin   concatenated per-entry gzip blobs (each <= PACK_MAX bytes)
  out/index.bin        16384 TSV shards (raw), concatenated
  out/titles.bin       all titles sorted (UTF-16 order), '\n' separated, in blocks
                       (for prefix search; block start keys in meta.json)
  out/meta.json        shard offset table + pack list + stats
  out/assets/          _mw_/ and _res_/ static files for the Worker

Entries are processed in cluster order (each cluster decompressed once),
split over N worker processes by contiguous cluster ranges.

Index shard line formats (UTF-8, '\n' separated, sorted by path):
  path \t pack \t offset \t length          content entry (gzip blob)
  path \t >target_path                     redirect
Shard number = fnv1a32(utf8(path)) & (SHARDS-1)
"""
import mmap, os, re, struct, sys, zlib, json, time, html as htmlmod
from multiprocessing import Process, Queue
from urllib.parse import quote
import zstandard

SHARDS = 16384
PACK_MAX = int(os.environ.get("PACK_MAX", str(1_900_000_000)))
NWORK = int(os.environ.get("NWORK", str(os.cpu_count() or 2)))
SNAPSHOT = os.environ.get("SNAPSHOT", "2026-06")
SHARD_GZ = int(os.environ.get("SHARD_GZ", "0"))
TBLOCK = int(os.environ.get("TBLOCK", "384"))

def fnv1a(b: bytes) -> int:
    h = 0x811C9DC5
    for c in b:
        h ^= c
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h

# ---------------------------------------------------------------- ZIM parsing
class Zim:
    def __init__(self, fn):
        self.f = open(fn, "rb")
        self.m = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)
        m = self.m
        (magic, major, minor) = struct.unpack_from("<IHH", m, 0)
        assert magic == 0x044D495A, "not a ZIM"
        (self.entry_count, self.cluster_count, self.path_ptr, self.title_ptr,
         self.cluster_ptr, self.mime_pos, self.main_page, self.layout,
         self.checksum_pos) = struct.unpack_from("<IIQQQQIIQ", m, 24)
        # mime list
        mimes = []
        p = self.mime_pos
        while True:
            e = m.find(b"\0", p)
            s = m[p:e]
            if not s:
                break
            mimes.append(s.decode())
            p = e + 1
        self.mimes = mimes
        self.cptr = struct.unpack_from("<%dQ" % self.cluster_count, m, self.cluster_ptr)
        self.pptr = struct.unpack_from("<%dQ" % self.entry_count, m, self.path_ptr)

    def dirent(self, i):
        m = self.m
        o = self.pptr[i]
        mime, plen, ns, rev = struct.unpack_from("<HBcI", m, o)
        if mime == 0xFFFF:
            (target,) = struct.unpack_from("<I", m, o + 8)
            q = o + 12
            cl = bl = None
        else:
            cl, bl = struct.unpack_from("<II", m, o + 8)
            q = o + 16
            target = None
        e = m.find(b"\0", q)
        path = m[q:e]
        e2 = m.find(b"\0", e + 1)
        title = m[e + 1:e2]
        return mime, ns, path, title, cl, bl, target

    def cluster_end(self, c):
        return self.cptr[c + 1] if c + 1 < self.cluster_count else self.checksum_pos

    def cluster_blobs(self, c, dctx):
        m = self.m
        start = self.cptr[c]
        end = self.cluster_end(c)
        info = m[start]
        comp = info & 0x0F
        ext = bool(info & 0x10)
        raw = m[start + 1:end]
        if comp == 5:
            d = dctx.decompressobj()
            data = d.decompress(raw)
        elif comp in (0, 1):
            data = raw
        elif comp == 4:
            import lzma
            data = lzma.LZMADecompressor().decompress(raw)
        else:
            raise ValueError("cluster compression %d" % comp)
        osz = 8 if ext else 4
        fmt = "<Q" if ext else "<I"
        (first,) = struct.unpack_from(fmt, data, 0)
        n = first // osz
        offs = struct.unpack_from("<%d%s" % (n, "Q" if ext else "I"), data, 0)
        return data, offs

# ---------------------------------------------------------------- HTML rewrite
COMMON_CSS = [
    "_mw_/ext.cite.parsoid.styles.css", "_mw_/ext.cite.styles.css",
    "_mw_/ext.tmh.player.styles.css", "_mw_/ext.wikimediamessages.styles.css",
    "_mw_/mediawiki.skinning.content.parsoid.css", "_mw_/skins.vector.icons.css",
    "_mw_/skins.vector.search.codex.styles.css", "_mw_/skins.vector.styles.css",
    "_mw_/ext.gadget.PDFLinkIcon.css", "_mw_/ext.gadget.RedirectColor.css",
    "_mw_/site.styles.css", "_mw_/noscript.css", "_res_/footer.css",
    "_res_/vector-2022.css",
]
COMMON_SET = set(COMMON_CSS)
HTML_CLASS = ("client-nojs vector-feature-limited-width-clientpref-1 "
              "vector-feature-limited-width-content-enabled vector-toc-not-available "
              "vector-feature-custom-font-size-clientpref-1 skin-theme-clientpref-os "
              "skin-thumbsize-clientpref-standard")
re_title = re.compile(r"<title>(.*?)</title>", re.S)
re_link = re.compile(r'<link\b[^>]*?href="([^"]+)"[^>]*>')
re_script = re.compile(r'\s*<script\b[^>]*src="[^"]*_webp_/[^"]*"[^>]*></script>')
re_footer = re.compile(r'<div class="zim-footer">(.*?)</div>', re.S)
re_oldid = re.compile(r'href="(https://ja\.wikipedia\.org/wiki/\?title=[^"]+)"')
re_pagecls = re.compile(r' (?:page|rootpage)-[^\s"]+')
re_relpre = re.compile(r'^(?:\./|\.\./)+')

TOPBAR = ('<div id="wa-bar"><a href="/" class="wa-logo">Wikiあ</a>'
          '<form action="/s"><input name="q" placeholder="記事名で移動" aria-label="記事名">'
          '<button>移動</button></form><a href="/random">おまかせ</a></div>')

def rewrite_html(path: str, s: str) -> str:
    hi = s.find("</head>")
    if hi < 0:
        return s
    head = s[:hi]
    body = s[hi + 7:]
    mt = re_title.search(head)
    title = mt.group(1) if mt else htmlmod.escape(path.replace("_", " "))
    extra = []
    for href in re_link.findall(head):
        h = re_relpre.sub("", href)
        if h.startswith("http") or h.endswith(".png"):
            continue
        if h in COMMON_SET:
            continue
        if h.endswith(".css"):
            extra.append('<link rel="stylesheet" href="/wiki/%s">' % h)
    out = ['<!DOCTYPE html><html class="', HTML_CLASS, '" lang="ja" dir="ltr"><head>'
           '<meta charset="UTF-8"><title>', title, ' - Wikiあ</title>'
           '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
           '<link rel="stylesheet" href="/wiki/_mw_/_s.css">']
    out.extend(extra)
    out.append("</head>")
    body = re_script.sub("", body)
    # page-specific body classes are not needed
    bi = body.find(">")
    if bi > 0:
        btag = re_pagecls.sub("", body[:bi + 1])
        # top bar right after <body ...>
        body = btag + TOPBAR + body[bi + 1:]
    # footer
    orig = "https://ja.wikipedia.org/wiki/" + quote(path, safe="")
    mf = re_footer.search(body)
    oldid = None
    if mf:
        mo = re_oldid.search(mf.group(1))
        if mo:
            oldid = mo.group(1)
    foot = ('<div class="zim-footer wa-foot">この記事はウィキペディア日本語版の'
            '「<a class="external" href="%s">%s</a>」' % (orig, title))
    if oldid:
        foot += '（<a class="external" href="%s">%s時点の版</a>）' % (oldid, SNAPSHOT)
    foot += ('をもとにしています。本文は<a class="external" rel="license" '
             'href="https://creativecommons.org/licenses/by-sa/4.0/deed.ja">'
             'クリエイティブ・コモンズ 表示-継承 4.0 (CC BY-SA 4.0)</a>のもとで利用できます'
             '（著作者: ウィキペディアの執筆者）。画像等のメディアは含みません。'
             'Wikiあ は非公式の読み取り専用ミラーです。</div>')
    if mf:
        body = body[:mf.start()] + foot + body[mf.end():]
    else:
        bi = body.rfind("</body>")
        body = body[:bi] + foot + body[bi:]
    out.append(body)
    return "".join(out)

# ---------------------------------------------------------------- worker
def gz(b: bytes) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, 31, 9)
    return c.compress(b) + c.flush()

def worker(k, zimfn, outdir, crange, by_cluster_file, q):
    import pickle
    with open(by_cluster_file, "rb") as f:
        by_cluster = pickle.load(f)
    z = Zim(zimfn)
    dctx = zstandard.ZstdDecompressor()
    packno = k * 100
    pj = 0
    fn = None
    fh = None
    off = 0
    idx = open(os.path.join(outdir, "idx-%02d.tsv" % k), "w", encoding="utf-8")
    raw_total = gz_total = n = 0
    t0 = time.time()

    def newpack():
        nonlocal fh, fn, off, pj
        if fh:
            fh.close()
            os.rename(fn + ".part", fn)
            q.put(("pack", fn))
        fn = os.path.join(outdir, "packs", "p%03d.bin" % (packno + pj))
        pj += 1
        fh = open(fn + ".part", "wb")
        off = 0

    newpack()
    a, b = crange
    for c in range(a, b):
        ents = by_cluster.get(c)
        if not ents:
            continue
        data, offs = z.cluster_blobs(c, dctx)
        for (bl, path, is_html) in ents:
            blob = data[offs[bl]:offs[bl + 1]]
            if is_html:
                try:
                    s = blob.decode("utf-8")
                    blob = rewrite_html(path, s).encode("utf-8")
                except Exception as ex:  # keep original on failure
                    sys.stderr.write("rewrite fail %s %s\n" % (path, ex))
            g = gz(blob)
            if off + len(g) > PACK_MAX:
                newpack()
            fh.write(g)
            idx.write("%s\t%d\t%d\t%d\n" % (path, packno + pj - 1, off, len(g)))
            off += len(g)
            raw_total += len(blob)
            gz_total += len(g)
            n += 1
        if (c - a) % 500 == 0:
            q.put(("prog", k, c - a, b - a, n, raw_total, gz_total, time.time() - t0))
    fh.close()
    os.rename(fn + ".part", fn)
    q.put(("pack", fn))
    idx.close()
    q.put(("done", k, n, raw_total, gz_total))

# ---------------------------------------------------------------- main
def main():
    zimfn, outdir = sys.argv[1], sys.argv[2]
    limit_clusters = int(os.environ.get("LIMIT_CLUSTERS", "0"))
    os.makedirs(os.path.join(outdir, "packs"), exist_ok=True)
    os.makedirs(os.path.join(outdir, "assets"), exist_ok=True)
    z = Zim(zimfn)
    print("entries", z.entry_count, "clusters", z.cluster_count, flush=True)
    t0 = time.time()
    by_cluster = {}
    redirects = []  # (path, target_index)
    paths = [None] * z.entry_count
    assets = []
    kinds = {}
    for i in range(z.entry_count):
        mime, ns, path, title, cl, bl, target = z.dirent(i)
        if ns != b"C":
            continue
        p = path.decode("utf-8")
        paths[i] = p
        if mime == 0xFFFF:
            redirects.append((p, target))
            continue
        mt = z.mimes[mime] if mime < len(z.mimes) else ""
        if p.startswith(("_mw_/", "_res_/", "_webp_/")):
            assets.append((p, cl, bl))
            continue
        is_html = mt.startswith("text/html")
        kinds[mt] = kinds.get(mt, 0) + 1
        by_cluster.setdefault(cl, []).append((bl, p, is_html))
    print("dirents parsed %.1fs; content clusters %d, redirects %d" %
          (time.time() - t0, len(by_cluster), len(redirects)), flush=True)
    print("kinds", kinds, flush=True)

    # static assets for the worker
    dctx = zstandard.ZstdDecompressor()
    css = {}
    for p, cl, bl in assets:
        data, offs = z.cluster_blobs(cl, dctx)
        b = data[offs[bl]:offs[bl + 1]]
        dst = os.path.join(outdir, "assets", "wiki", p)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "wb") as f:
            f.write(b)
        if p in COMMON_SET:
            css[p] = b
    # combined stylesheet lives in _mw_/ so relative url()s keep working
    with open(os.path.join(outdir, "assets", "wiki", "_mw_", "_s.css"), "wb") as f:
        for p in COMMON_CSS:
            if p in css:
                b = css[p]
                if p.startswith("_res_/"):
                    b = b.replace(b"url(", b"url(../_res_/").replace(b"url(../_res_/'", b"url('../_res_/").replace(b'url(../_res_/"', b'url("../_res_/')
                f.write(b"/* %s */\n" % p.encode() + b + b"\n")
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "wikia.css"), "rb") as g:
            f.write(g.read())

    # redirects: resolve chains to final content path
    def resolve(t, depth=0):
        mime, ns, path, title, cl, bl, target = z.dirent(t)
        if mime == 0xFFFF and depth < 10:
            return resolve(target, depth + 1)
        return path.decode("utf-8") if ns == b"C" else None
    with open(os.path.join(outdir, "idx-redirects.tsv"), "w", encoding="utf-8") as f:
        for p, t in redirects:
            tp = resolve(t)
            if tp is not None and tp != p:
                f.write("%s\t>%s\n" % (p, tp))
    print("redirects written %.1fs" % (time.time() - t0), flush=True)

    clusters = sorted(by_cluster)
    if limit_clusters:
        clusters = clusters[:limit_clusters]
    # balance by number of entries
    import pickle
    bcf = os.path.join(outdir, "by_cluster.pkl")
    with open(bcf, "wb") as f:
        pickle.dump(by_cluster, f, protocol=4)
    del by_cluster
    total = len(clusters)
    bounds = [clusters[0]] + [clusters[min(total - 1, (total * j) // NWORK)] for j in range(1, NWORK)] + [clusters[-1] + 1]
    q = Queue()
    procs = []
    for k in range(NWORK):
        pr = Process(target=worker, args=(k, zimfn, outdir, (bounds[k], bounds[k + 1]), bcf, q))
        pr.start()
        procs.append(pr)
    done = 0
    stats = {}
    packfile_log = open(os.path.join(outdir, "packs-ready.txt"), "a")
    while done < NWORK:
        msg = q.get()
        if msg[0] == "pack":
            packfile_log.write(msg[1] + "\n")
            packfile_log.flush()
            print("PACK", msg[1], os.path.getsize(msg[1]), flush=True)
        elif msg[0] == "prog":
            print("prog w%d %d/%d n=%d raw=%.2fGB gz=%.2fGB %.0fs" %
                  (msg[1], msg[2], msg[3], msg[4], msg[5] / 1e9, msg[6] / 1e9, msg[7]), flush=True)
        elif msg[0] == "done":
            done += 1
            stats[msg[1]] = msg[2:]
            print("DONE", msg, flush=True)
    for pr in procs:
        pr.join()
    os.remove(bcf)

    # build sharded index (raw TSV shards: the Worker never has to decompress)
    shards = [[] for _ in range(SHARDS)]
    allp = []
    names = sorted(x for x in os.listdir(outdir) if x.startswith("idx-") and x.endswith(".tsv"))
    for nm in names:
        with open(os.path.join(outdir, nm), encoding="utf-8") as f:
            for line in f:
                p, rest = line.split("\t", 1)
                shards[fnv1a(p.encode("utf-8")) & (SHARDS - 1)].append(line)
                if not p.startswith("_"):
                    allp.append(p + ("\t" + rest.rstrip("\n") if rest.startswith(">") else ""))
    offs = [0]
    with open(os.path.join(outdir, "index.bin"), "wb") as f:
        for s in shards:
            s.sort()
            g = "".join(s).encode("utf-8")
            if SHARD_GZ:
                g = gz(g)
            f.write(g)
            offs.append(offs[-1] + len(g))
    del shards
    # sorted title list for prefix search, in blocks; first key of each block
    # goes into the Worker bundle. Sorted by UTF-16 code units (= JS order).
    allp.sort(key=lambda x: x.split("\t", 1)[0].encode("utf-16-be"))
    tkeys, toffs = [], [0]
    with open(os.path.join(outdir, "titles.bin"), "wb") as f:
        for i in range(0, len(allp), TBLOCK):
            blk = allp[i:i + TBLOCK]
            tkeys.append(blk[0].split("\t", 1)[0])
            b = ("\n".join(blk) + "\n").encode("utf-8")
            f.write(b)
            toffs.append(toffs[-1] + len(b))
    articles = sum(1 for x in allp if "\t>" not in x)
    packs = sorted(os.path.basename(x) for x in os.listdir(os.path.join(outdir, "packs")) if x.endswith(".bin"))
    meta = {
        "snapshot": SNAPSHOT,
        "shards": SHARDS,
        "shard_offsets": offs,
        "packs": packs,
        "pack_sizes": {p: os.path.getsize(os.path.join(outdir, "packs", p)) for p in packs},
        "index_size": offs[-1],
        "shard_gz": bool(SHARD_GZ),
        "title_keys": tkeys,
        "title_offsets": toffs,
        "titles_size": toffs[-1],
        "articles": articles,
        "entries": sum(v[0] for v in stats.values()),
        "raw_bytes": sum(v[1] for v in stats.values()),
        "gz_bytes": sum(v[2] for v in stats.values()),
        "redirects": len(redirects),
        "seconds": time.time() - t0,
    }
    with open(os.path.join(outdir, "meta.json"), "w") as f:
        json.dump(meta, f)
    print("META", json.dumps({k: v for k, v in meta.items() if k not in ("shard_offsets", "title_keys", "title_offsets")}), flush=True)

if __name__ == "__main__":
    main()
