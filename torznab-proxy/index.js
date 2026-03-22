const express = require("express");
const axios = require("axios");
const { create } = require("xmlbuilder2");

const app = express();
const PORT = 7070;

// URL do seu indexer
const INDEXER_URL = "http://torrent-indexer:7006";

app.get("/torznab/api", async (req, res) => {
  const { t, q } = req.query;

  // =========================
  // CAPS (OBRIGATÓRIO)
  // =========================
  if (t === "caps") {
    res.set("Content-Type", "application/xml; charset=utf-8");

    return res.send(`<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.0" title="torrent-indexer" />
  <limits max="100" default="50" />
  <searching>
    <search available="yes" supportedParams="q" />
  </searching>
  <categories>
    <category id="2000" name="Movies" />
    <category id="5000" name="TV" />
  </categories>
</caps>`);
  }

  // =========================
  // SEARCH
  // =========================
  if (t === "search") {
    try {
      const response = await axios.get(`${INDEXER_URL}/indexers/bludv`, {
        params: { q }
      });

      const results = response.data.results || [];

      // cria XML com namespace torznab
      const root = create({ version: "1.0", encoding: "UTF-8" })
        .ele("rss", {
          version: "2.0",
          "xmlns:torznab": "http://torznab.com/schemas/2015/feed"
        })
        .ele("channel");

      results.forEach((item) => {
        const entry = root.ele("item");

        entry.ele("title").txt(item.title || item.original_title || "Sem título");
        entry.ele("link").txt(item.details || "");
        entry.ele("guid").txt(item.info_hash || Math.random().toString());

        entry.ele("enclosure", {
          url: item.magnet_link || "",
          length: item.size || "0",
          type: "application/x-bittorrent"
        });

        entry.ele("pubDate").txt(
          item.date ? new Date(item.date).toUTCString() : new Date().toUTCString()
        );

        entry.ele("torznab:attr", {
          name: "seeders",
          value: item.seed_count || 0
        });

        entry.ele("torznab:attr", {
          name: "peers",
          value: item.leech_count || 0
        });
      });

      const xml = root.end({ prettyPrint: true });

      res.set("Content-Type", "application/xml; charset=utf-8");
      return res.send(xml);

    } catch (err) {
      console.error(err.message);
      return res.status(500).send("error");
    }
  }

  // =========================
  // DEFAULT
  // =========================
  return res.status(400).send("Unsupported parameter");
});

app.listen(PORT, () => {
  console.log(`Torznab proxy rodando na porta ${PORT}`);
});
