(function () {
  var auth = window.KopiaAuth;
  var form = document.getElementById("login-form");
  var passwordStep = document.getElementById("password-step");
  var totpStep = document.getElementById("totp-step");
  var msg = document.getElementById("msg");
  var loginBtn = document.getElementById("login-btn");
  var totpBtn = document.getElementById("totp-btn");
  var backBtn = document.getElementById("back-btn");
  var passkeyBtn = document.getElementById("passkey-btn");
  var onTotpStep = false;

  function showTotp() {
    onTotpStep = true;
    passwordStep.classList.add("hidden");
    totpStep.classList.remove("hidden");
    document.getElementById("totp").focus();
  }

  function showPassword() {
    onTotpStep = false;
    totpStep.classList.add("hidden");
    passwordStep.classList.remove("hidden");
    document.getElementById("password").focus();
  }

  async function verifyTotp() {
    auth.setMsg(msg, "");
    totpBtn.disabled = true;
    try {
      var out = await auth.post("/api/v1/auth/login/totp", {
        code: document.getElementById("totp").value
      });
      if (auth.reloadOnCSRF(out, msg)) return;
      if (out.res.ok && out.data && out.data.status === "ok") {
        auth.goHome();
        return;
      }
      auth.setMsg(msg, (out.data && out.data.error) || "Invalid code", "error");
    } catch (err) {
      auth.setMsg(msg, "Network error", "error");
    } finally {
      totpBtn.disabled = false;
    }
  }

  form.addEventListener("submit", async function (e) {
    e.preventDefault();
    if (onTotpStep) {
      await verifyTotp();
      return;
    }
    auth.setMsg(msg, "");
    loginBtn.disabled = true;
    try {
      var out = await auth.post("/api/v1/auth/login", {
        username: document.getElementById("username").value.trim(),
        password: document.getElementById("password").value
      });
      if (auth.reloadOnCSRF(out, msg)) return;
      if (out.res.ok && out.data && out.data.status === "totp_required") {
        showTotp();
        return;
      }
      if (out.res.ok && out.data && out.data.status === "ok") {
        auth.goHome();
        return;
      }
      auth.setMsg(msg, (out.data && out.data.error) || "Sign in failed", "error");
    } catch (err) {
      auth.setMsg(msg, "Network error", "error");
    } finally {
      loginBtn.disabled = false;
    }
  });

  totpBtn.addEventListener("click", function () {
    verifyTotp();
  });

  backBtn.addEventListener("click", function () {
    auth.setMsg(msg, "");
    showPassword();
  });

  if (!passkeyBtn) return;

  if (!auth.supportsPasskeys()) {
    var section = document.getElementById("passkey-section");
    if (section) section.classList.add("hidden");
    return;
  }

  passkeyBtn.addEventListener("click", async function () {
    auth.setMsg(msg, "");
    var envErr = auth.passkeyEnvironmentOK();
    if (envErr) {
      auth.setMsg(msg, envErr, "error");
      return;
    }
    passkeyBtn.disabled = true;
    try {
      var begin = await auth.post("/api/v1/auth/webauthn/login/begin", {});
      if (auth.reloadOnCSRF(begin, msg)) return;
      if (!begin.res.ok) {
        auth.setMsg(msg, (begin.data && begin.data.error) || "Passkey login unavailable", "error");
        return;
      }
      var pk = begin.data && begin.data.publicKey;
      if (!pk || !pk.challenge) {
        auth.setMsg(msg, "Invalid passkey options from server", "error");
        return;
      }
      var options = auth.preparePublicKey(pk);
      var cred = await navigator.credentials.get({ publicKey: options });
      var finish = await auth.post("/api/v1/auth/webauthn/login/finish", auth.assertionToJSON(cred));
      if (auth.reloadOnCSRF(finish, msg)) return;
      if (finish.res.ok && finish.data && finish.data.status === "ok") {
        auth.goHome();
        return;
      }
      auth.setMsg(msg, (finish.data && finish.data.error) || "Passkey verification failed", "error");
    } catch (err) {
      if (err && err.name === "NotAllowedError") {
        auth.setMsg(msg, "Passkey sign-in was cancelled", "error");
      } else {
        auth.setMsg(msg, "Passkey sign-in failed: " + ((err && err.message) || "unknown error"), "error");
      }
    } finally {
      passkeyBtn.disabled = false;
    }
  });
})();
