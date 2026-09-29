(function () {
  const config = window.HBVN_AUTH_CONFIG || {};
  const message = document.querySelector("#auth-message");
  const authDialog = document.querySelector("#auth-dialog");
  const recoveryDialog = document.querySelector("#recovery-dialog");
  const recoveryMessage = document.querySelector("#recovery-message");

  function setMessage(text, isError = false) {
    message.textContent = text;
    message.style.color = isError ? "#ff9b9b" : "#ffd86a";
  }

  if (!config.supabaseUrl || !config.supabaseAnonKey || !window.supabase) {
    setMessage("De aanmeldfunctie wordt momenteel klaargemaakt.");
    document.querySelectorAll("[data-auth-provider], #email-auth-form button")
      .forEach(button => button.disabled = true);
    return;
  }

  const client = window.supabase.createClient(
    config.supabaseUrl,
    config.supabaseAnonKey,
    {
      auth: {
        persistSession: true,
        autoRefreshToken: true,
        detectSessionInUrl: true
      }
    }
  );

  async function loadPreferences(user) {
    const { data, error } = await client
      .from("boutique_preferences")
      .select("title_key,status")
      .eq("user_id", user.id);

    if (error) throw error;

    return {
      seen: data.filter(row => row.status === "seen").map(row => row.title_key),
      dismissed: data.filter(row => row.status === "dismissed").map(row => row.title_key)
    };
  }

  async function applySession(session) {
    const user = session?.user || null;

    if (!user) {
      window.HBVNBoutique.setSession(null);
      return;
    }

    try {
      const preferences = await loadPreferences(user);
      window.HBVNBoutique.setSession(user, preferences);
      if (authDialog.open) authDialog.close();
    } catch (error) {
      setMessage("Je profiel kon niet worden geladen. Probeer het opnieuw.", true);
    }
  }

  async function socialSignIn(provider) {
    setMessage("Je wordt doorgestuurd…");
    const { error } = await client.auth.signInWithOAuth({
      provider,
      options: { redirectTo: window.location.origin + window.location.pathname }
    });
    if (error) setMessage(error.message, true);
  }

  document.querySelectorAll("[data-auth-provider]").forEach(button => {
    button.addEventListener("click", () => socialSignIn(button.dataset.authProvider));
  });

  document.querySelector("#email-auth-form").addEventListener("submit", async event => {
    event.preventDefault();
    setMessage("Aanmelden…");
    const email = document.querySelector("#auth-email").value.trim();
    const password = document.querySelector("#auth-password").value;
    const { error } = await client.auth.signInWithPassword({ email, password });
    if (error) setMessage("Aanmelden is niet gelukt. Controleer je gegevens.", true);
  });

  document.querySelector("#create-account").addEventListener("click", async () => {
    const email = document.querySelector("#auth-email").value.trim();
    const password = document.querySelector("#auth-password").value;

    if (!email || password.length < 8) {
      setMessage("Vul een geldig e-mailadres en minstens 8 tekens in.", true);
      return;
    }

    setMessage("Account wordt aangemaakt…");
    const { error } = await client.auth.signUp({ email, password });
    setMessage(
      error ? "Account maken is niet gelukt." : "Controleer je e-mail om je account te bevestigen.",
      Boolean(error)
    );
  });

  document.querySelector("#forgot-password").addEventListener("click", async () => {
    const email = document.querySelector("#auth-email").value.trim();

    if (!email) {
      setMessage("Vul eerst je e-mailadres in.", true);
      return;
    }

    setMessage("Herstelmail wordt verstuurd…");
    const { error } = await client.auth.resetPasswordForEmail(email, {
      redirectTo: window.location.origin + window.location.pathname
    });
    setMessage(
      error ? "De herstelmail kon niet worden verstuurd." : "Controleer je e-mail voor de herstel-link.",
      Boolean(error)
    );
  });

  document.querySelector("#close-recovery").addEventListener("click", () => recoveryDialog.close());
  recoveryDialog.addEventListener("click", event => {
    if (event.target === recoveryDialog) recoveryDialog.close();
  });

  document.querySelector("#recovery-form").addEventListener("submit", async event => {
    event.preventDefault();
    const password = document.querySelector("#recovery-password").value;
    const confirmation = document.querySelector("#recovery-password-confirm").value;

    if (password.length < 8) {
      recoveryMessage.textContent = "Gebruik minstens 8 tekens.";
      recoveryMessage.style.color = "#ff9b9b";
      return;
    }

    if (password !== confirmation) {
      recoveryMessage.textContent = "De wachtwoorden zijn niet gelijk.";
      recoveryMessage.style.color = "#ff9b9b";
      return;
    }

    recoveryMessage.textContent = "Wachtwoord wordt bewaard…";
    recoveryMessage.style.color = "#ffd86a";
    const { error } = await client.auth.updateUser({ password });

    if (error) {
      recoveryMessage.textContent = "Het wachtwoord kon niet worden gewijzigd. Open de herstel-link opnieuw.";
      recoveryMessage.style.color = "#ff9b9b";
      return;
    }

    recoveryMessage.textContent = "Je wachtwoord is gewijzigd.";
    recoveryMessage.style.color = "#73f6b4";
    setTimeout(() => recoveryDialog.close(), 900);
  });

  document.querySelector("#sign-out").addEventListener("click", async () => {
    await client.auth.signOut();
    document.querySelector("#boutique-dialog").close();
  });

  let saveTimer;
  window.addEventListener("hbvn:boutique-change", event => {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(async () => {
      const { data: { user } } = await client.auth.getUser();
      if (!user) return;

      const preferences = event.detail;
      const rows = [
        ...preferences.seen.map(title_key => ({ user_id:user.id, title_key, status:"seen" })),
        ...preferences.dismissed.map(title_key => ({ user_id:user.id, title_key, status:"dismissed" }))
      ];

      await client.from("boutique_preferences").delete().eq("user_id", user.id);
      if (rows.length) await client.from("boutique_preferences").insert(rows);
    }, 250);
  });

  client.auth.onAuthStateChange((event, session) => {
    applySession(session);
    if (event === "PASSWORD_RECOVERY") {
      if (authDialog.open) authDialog.close();
      recoveryDialog.showModal();
    }
  });
  client.auth.getSession().then(({ data }) => applySession(data.session));
})();
