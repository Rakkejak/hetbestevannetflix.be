const ALLOWED_ORIGINS = new Set([
  "https://hetbestevannetflix.be",
  "https://www.hetbestevannetflix.be"
]);

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": ALLOWED_ORIGINS.has(origin) ? origin : "https://hetbestevannetflix.be",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin"
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(origin)
      });
    }

    if (url.pathname === "/health") {
      return new Response("ok");
    }

    if (url.pathname !== "/event" || request.method !== "POST") {
      return new Response("Not Found", { status: 404 });
    }

    if (!ALLOWED_ORIGINS.has(origin)) {
      return new Response("Forbidden", { status: 403 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Invalid JSON", { status: 400 });
    }

    const experiment = String(body.experiment || "");
    const variant = String(body.variant || "");
    const event = String(body.event || "");

    if (
      experiment !== "support_cta_v1" ||
      !["pintje", "half-pintje", "waterke"].includes(variant) ||
      !["impression", "click"].includes(event)
    ) {
      return new Response("Invalid event", { status: 400 });
    }

    env.AB_EVENTS.writeDataPoint({
      blobs: [experiment, variant, event],
      doubles: [1],
      indexes: [experiment]
    });

    return new Response(null, {
      status: 204,
      headers: corsHeaders(origin)
    });
  }
};
