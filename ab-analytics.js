(() => {
  const ENDPOINT = "https://hbvn-ab-analytics.rakkejak.workers.dev/event";
  const EXPERIMENT = "support_cta_v1";
  const VALID_VARIANTS = new Set(["pintje", "half-pintje", "waterke"]);

  let impressionSent = false;
  let clickSent = false;

  function sendEvent(event, variant) {
    if (!VALID_VARIANTS.has(variant)) return;

    fetch(ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        experiment: EXPERIMENT,
        variant,
        event
      }),
      credentials: "omit",
      referrerPolicy: "no-referrer",
      keepalive: true
    }).catch(() => {});
  }

  function activateTracking() {
    const buttons = Array.from(document.querySelectorAll("[data-support-cta]"));
    const variant = buttons.find(el => VALID_VARIANTS.has(el.dataset.variant))?.dataset.variant;

    if (!variant) return;

    if (!impressionSent) {
      impressionSent = true;
      sendEvent("impression", variant);
    }

    buttons.forEach(button => {
      if (button.dataset.abTrackingBound) return;

      button.dataset.abTrackingBound = "true";
      button.addEventListener("click", () => {
        if (clickSent) return;
        clickSent = true;
        sendEvent("click", button.dataset.variant || variant);
      });
    });
  }

  const observer = new MutationObserver(activateTracking);

  document.querySelectorAll("[data-support-cta]").forEach(button => {
    observer.observe(button, {
      attributes: true,
      attributeFilter: ["data-variant"]
    });
  });

  activateTracking();
})();
