exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return json(204, {});
    }

    const query = event.queryStringParameters || {};
    const body = parseJson(event.body);
    const action = String(query.action || body.action || "listing").toLowerCase();

    if (action === "listing") {
      return await handleListing(query, body);
    }

    if (action === "market") {
      return await handleMarket(query, body);
    }

    return json(400, {
      error: "unsupported_action",
      message: "Use action=listing or action=market."
    });
  } catch (error) {
    console.error("[airroi] handler error:", error);
    return json(500, {
      error: "internal_error",
      message: error && error.message ? error.message : "Unknown error"
    });
  }
};

async function handleListing(query, body) {
  const rawId = query.id || body.id || extractAirbnbId(body.url);
  const listingId = rawId ? String(rawId).trim() : "";
  const listingUrl = safeHttpUrl(body.url) || (listingId ? buildListingUrl(listingId) : "");

  if (!listingId && !listingUrl) {
    return json(400, {
      error: "missing_listing_id",
      message: "Provide action=listing&id=XXXX or a body.url."
    });
  }

  let primaryItems = [];
  try {
    primaryItems = await fetchApify({ startUrls: [{ url: listingUrl }], maxItems: 4 });
  } catch (e) {
    console.warn("[airroi] Apify listing fetch failed:", e.message);
    return json(200, { listing: null, listings: [], market: null, error: "apify_unavailable" });
  }

  const primaryListings = normalizeListings(primaryItems, { fallbackUrl: listingUrl });
  let listing = pickListing(primaryListings, listingId, listingUrl);

  if (!listing) {
    return json(200, {
      error: "listing_not_found",
      listing: null,
      listings: [],
      market: null
    });
  }

  let comparables = primaryListings.filter(function(candidate) {
    return !isSameListing(candidate, listing);
  });

  if (!comparables.length && listing.ville) {
    const searchUrl = buildSearchUrl(listing.ville, listing.capacity || 2);
    try {
      const marketItems = await fetchApify({ startUrls: [{ url: searchUrl }], maxItems: 10 });
      comparables = normalizeListings(marketItems, { fallbackUrl: searchUrl })
        .filter(function(candidate) {
          return hasPrice(candidate) && !isSameListing(candidate, listing);
        });
    } catch (e) {
      console.warn("[airroi] Apify comparables fetch failed:", e.message);
    }
  }

  comparables = dedupeListings(comparables).slice(0, 10);

  const market = buildMarketSummary(comparables.length ? comparables : [listing], listing.ville);
  listing = enrichListing(listing, market);
  comparables = comparables.map(function(candidate) {
    return enrichListing(candidate, market);
  });

  const scoreTotal = computeListingScore(listing, market);
  const verdict = getVerdict(scoreTotal, listing.variation);
  const insight = buildInsight(listing, market, comparables);

  return json(200, {
    listing: listing,
    listings: comparables,
    market: market,
    score_total: scoreTotal,
    verdict: verdict,
    insight: insight,
    source: "apify"
  });
}

async function handleMarket(query, body) {
  const city = String(query.city || body.city || "").trim();
  const guests = toNumber(query.guests || body.guests) || 2;

  if (!city) {
    return json(400, {
      error: "missing_city",
      message: "Provide action=market&city=..."
    });
  }

  const searchUrl = buildSearchUrl(city, guests);
  const items = await fetchApify({
    startUrls: [{ url: searchUrl }],
    maxItems: 18
  });

  const listings = dedupeListings(
    normalizeListings(items, { fallbackUrl: searchUrl }).filter(hasPrice)
  ).slice(0, 12);

  const market = buildMarketSummary(listings, city);

  return json(200, {
    market: market,
    listings: listings.map(function(candidate) {
      return enrichListing(candidate, market);
    }),
    source: "apify"
  });
}

async function fetchApify(payload) {
  const token = process.env.APIFY_TOKEN;

  if (!token) {
    throw new Error("Missing APIFY_TOKEN");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);

  try {
    const response = await fetch(
      "https://api.apify.com/v2/acts/maxcopell~airbnb-scraper/run-sync-get-dataset-items?token=" + encodeURIComponent(token),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal
      }
    );

    if (!response.ok) {
      const text = await response.text();
      throw new Error("Apify HTTP " + response.status + ": " + text.slice(0, 220));
    }

    const data = await response.json();
    return Array.isArray(data) ? data : [];
  } catch (e) {
    if (e.name === "AbortError") throw new Error("Apify timeout (9s)");
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeListings(items, options) {
  return (Array.isArray(items) ? items : [])
    .map(function(item) { return normalizeListing(item, options); })
    .filter(Boolean);
}

function normalizeListing(item, options) {
  if (!item || typeof item !== "object") return null;

  const fallbackUrl = options && options.fallbackUrl ? String(options.fallbackUrl) : "";
  const url = safeHttpUrl(firstString(item.url, item.link, item.listingUrl, item.roomUrl, fallbackUrl));
  const id = firstString(item.id, item.roomId, item.listingId, extractAirbnbId(url), extractAirbnbId(fallbackUrl));
  const prix = toNumber(
    item.priceAmount,
    item.price && item.price.amount,
    item.rate && item.rate.amount,
    item.nightlyRate && item.nightlyRate.amount,
    typeof item.price === "number" ? item.price : null
  );
  const note = toNumber(item.rating, item.starRating, item.avgRating, item.reviewsScore);
  const avis = toNumber(item.reviewsCount, item.numberOfReviews, item.reviews);
  const occupation = toNumber(item.occupancyRate, item.occupancy, item.occupancyPercentage);
  const ville = firstString(
    item.city, item.localizedCity,
    item.location && item.location.city,
    item.address && item.address.city,
    inferCityFromUrl(url || fallbackUrl)
  );
  const quartier = firstString(
    item.neighborhood, item.district, item.area,
    item.location && item.location.localizedCity,
    item.address && item.address.neighborhood
  );
  const type = firstString(item.roomType, item.type, item.propertyType, item.roomAndPropertyType, "Logement");
  const photo = pickPhoto(item);
  const capacity = toNumber(item.personCapacity, item.numberOfGuests, item.guests, item.maxGuests);

  return {
    id: id || "",
    nom: firstString(item.name, item.title, "Logement Airbnb"),
    prix: prix == null ? null : Math.round(prix),
    photo: photo,
    note: note == null ? null : roundTo(note, 1),
    variation: null,
    avis: avis == null ? null : Math.round(avis),
    occupation: occupation == null ? null : Math.round(occupation),
    quartier: quartier || "",
    ville: ville || "",
    city: ville || "",
    type: type || "Logement",
    url: url,
    lat: toNumber(item.lat, item.location && item.location.lat, item.coordinates && item.coordinates.latitude),
    lng: toNumber(item.lng, item.location && item.location.lng, item.coordinates && item.coordinates.longitude),
    capacity: capacity == null ? null : Math.round(capacity)
  };
}

function enrichListing(listing, market) {
  const clone = Object.assign({}, listing);
  clone.variation = computeVariation(clone.prix, market && market.prix_moyen);
  return clone;
}

function buildMarketSummary(listings, city) {
  const priced = (Array.isArray(listings) ? listings : []).filter(hasPrice);
  const prices = priced.map(function(item) { return item.prix; }).filter(function(v) { return v != null; }).sort(function(a, b) { return a - b; });
  const occupations = priced.map(function(item) { return item.occupation; }).filter(function(v) { return v != null; });
  const medianPrice = median(prices);
  const lowBand = percentile(prices, 0.25);
  const highBand = percentile(prices, 0.75);
  return {
    ville: city || firstString(priced[0] && priced[0].ville, ""),
    prix_moyen: medianPrice,
    bon_deal_max: lowBand != null ? Math.round(lowBand) : null,
    trop_cher_min: highBand != null ? Math.round(highBand) : null,
    occupation: occupations.length ? Math.round(average(occupations)) : null
  };
}

function computeVariation(prix, marche) {
  if (prix == null || marche == null || marche <= 0) return null;
  return Math.round(((prix - marche) / marche) * 100);
}

function computeListingScore(listing, market) {
  let score = 60;
  if (listing.variation != null) score += clamp(-listing.variation * 1.8, -30, 30);
  if (listing.note != null) score += clamp((listing.note - 4.5) * 18, -12, 12);
  if (listing.avis != null) score += clamp(listing.avis / 20, 0, 8);
  if (listing.occupation != null) score += clamp((listing.occupation - 60) / 4, -6, 10);
  if (market && market.prix_moyen != null && market.bon_deal_max != null && listing.prix != null && listing.prix <= market.bon_deal_max) score += 5;
  return Math.max(20, Math.min(99, Math.round(score)));
}

function getVerdict(score, variation) {
  if (variation != null) {
    if (variation <= -10) return "Bonne affaire";
    if (variation >= 10) return "Surévalué";
  }
  if (score >= 80) return "Bonne affaire";
  if (score >= 55) return "Prix correct";
  return "À surveiller";
}

function buildInsight(listing, market, comparables) {
  const parts = [];
  if (listing.variation != null) {
    if (listing.variation < 0) parts.push("Prix " + Math.abs(listing.variation) + "% sous le marché local.");
    else if (listing.variation > 0) parts.push("Prix " + listing.variation + "% au-dessus du marché local.");
    else parts.push("Prix dans la moyenne du marché local.");
  }
  if (listing.note != null) parts.push("Note voyageurs : " + listing.note + "/5.");
  if (comparables.length) parts.push(comparables.length + " bien(s) comparables trouvés.");
  if (market && market.prix_moyen != null) parts.push("Prix médian du marché : " + market.prix_moyen + "€/nuit.");
  return parts.join(" ");
}

function pickListing(listings, listingId, listingUrl) {
  const nId = listingId ? String(listingId) : "";
  const nUrl = listingUrl ? String(listingUrl) : "";
  return listings.find(function(i) { return nId && i.id && String(i.id) === nId; })
    || listings.find(function(i) { return nUrl && i.url && i.url === nUrl; })
    || listings[0] || null;
}

function dedupeListings(listings) {
  const seen = new Set();
  return (Array.isArray(listings) ? listings : []).filter(function(item) {
    const key = item.id || item.url || (item.nom + "|" + item.prix + "|" + item.ville);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function isSameListing(a, b) {
  if (!a || !b) return false;
  if (a.id && b.id && String(a.id) === String(b.id)) return true;
  if (a.url && b.url && a.url === b.url) return true;
  return false;
}

function hasPrice(item) { return !!item && item.prix != null; }

function pickPhoto(item) {
  const imageList = Array.isArray(item.images) ? item.images : [];
  const candidates = [
    item.photo, item.image, item.thumbnail,
    imageList[0] && imageList[0].url,
    imageList[0] && imageList[0].originalUrl,
    imageList[0]
  ];
  return safeHttpUrl(firstString.apply(null, candidates));
}

function buildListingUrl(id) { return "https://www.airbnb.fr/rooms/" + encodeURIComponent(String(id)); }
function buildSearchUrl(city, guests) { return "https://www.airbnb.fr/s/" + encodeURIComponent(city) + "/homes?adults=" + encodeURIComponent(String(guests || 2)); }

function extractAirbnbId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^\d+$/.test(raw)) return raw;
  let match = raw.match(/\/rooms\/(\d+)/i);
  if (match) return match[1];
  match = raw.match(/[?&](?:room_id|id)=(\d+)/i);
  if (match) return match[1];
  return "";
}

function inferCityFromUrl(value) {
  const match = String(value || "").match(/\/s\/([^/]+)\/homes/i);
  if (!match) return "";
  return decodeURIComponent(match[1]).replace(/-/g, " ").trim();
}

function firstString() {
  for (let i = 0; i < arguments.length; i++) {
    const v = arguments[i];
    if (typeof v === "string" && v.trim()) return v.trim();
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
  }
  return "";
}

function toNumber() {
  for (let i = 0; i < arguments.length; i++) {
    const v = arguments[i];
    if (v == null || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function average(values) {
  if (!values.length) return null;
  return values.reduce(function(s, v) { return s + v; }, 0) / values.length;
}

function median(values) {
  if (!values.length) return null;
  const s = values.slice().sort(function(a, b) { return a - b; });
  const m = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? Math.round((s[m - 1] + s[m]) / 2) : Math.round(s[m]);
}

function percentile(values, ratio) {
  if (!values.length) return null;
  const s = values.slice().sort(function(a, b) { return a - b; });
  const idx = (s.length - 1) * ratio;
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo);
}

function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }
function roundTo(v, d) { const f = Math.pow(10, d); return Math.round(v * f) / f; }

function safeHttpUrl(value) {
  if (!value) return "";
  try {
    const p = new URL(String(value));
    if (p.protocol === "http:" || p.protocol === "https:") return p.href;
  } catch (e) {}
  return "";
}

function parseJson(raw) {
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try { return JSON.parse(raw); } catch (e) { return {}; }
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
    },
    body: JSON.stringify(body)
  };
}
