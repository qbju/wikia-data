# wikia-data

Data packs for **Wikiあ** (https://wikia.i2p.workers.dev), a read-only mirror of the Japanese Wikipedia served by a Cloudflare Worker.

- Source: Kiwix ZIM `wikipedia_ja_all_nopic_2026-06.zim` (Japanese Wikipedia, June 2026 snapshot, no pictures).
- Content license: text of Wikipedia articles is available under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (Wikipedia contributors, https://ja.wikipedia.org/).
- The release assets contain per-article gzip blobs (`packs/NNN.bin`) and a sharded title index (`index.bin`). See release notes for the format.
