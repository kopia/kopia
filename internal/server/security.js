(function () {
  var auth = window.KopiaAuth;
  var totpMsg = document.getElementById("totp-msg");
  var passkeyMsg = document.getElementById("passkey-msg");

  function withBusy(btn, fn) {
    return async function () {
      if (btn.disabled) return;
      btn.disabled = true;
      try {
        await fn();
      } finally {
        btn.disabled = false;
      }
    };
  }

  var totpBegin = document.getElementById("totp-begin");
  if (totpBegin) {
    totpBegin.addEventListener("click", withBusy(totpBegin, async function () {
      auth.setMsg(totpMsg, "");
      try {
        var out = await auth.post("/api/v1/auth/totp/setup/begin", {});
        if (auth.reloadOnCSRF(out, totpMsg)) return;
        if (!out.res.ok) {
          auth.setMsg(totpMsg, (out.data && out.data.error) || "Setup failed", "error");
          return;
        }
        document.getElementById("totp-setup").classList.remove("hidden");
        document.getElementById("totp-qr").src = out.data.qrCodeDataUrl;
        document.getElementById("totp-secret").textContent = "Secret: " + out.data.secret;
        document.getElementById("totp-code").focus();
      } catch (err) {
        auth.setMsg(totpMsg, "Network error", "error");
      }
    }));
  }

  var totpConfirm = document.getElementById("totp-confirm");
  if (totpConfirm) {
    totpConfirm.addEventListener("click", withBusy(totpConfirm, async function () {
      try {
        var out = await auth.post("/api/v1/auth/totp/setup/confirm", {
          code: document.getElementById("totp-code").value
        });
        if (auth.reloadOnCSRF(out, totpMsg)) return;
        if (!out.res.ok) {
          auth.setMsg(totpMsg, (out.data && out.data.error) || "Invalid code", "error");
          return;
        }
        window.location.reload();
      } catch (err) {
        auth.setMsg(totpMsg, "Network error", "error");
      }
    }));
  }

  var totpDisable = document.getElementById("totp-disable");
  if (totpDisable) {
    totpDisable.addEventListener("click", withBusy(totpDisable, async function () {
      var password = document.getElementById("totp-disable-password").value;
      if (!password) {
        auth.setMsg(totpMsg, "Enter your password to disable TOTP", "error");
        return;
      }
      if (!confirm("Disable TOTP for this account?")) return;
      try {
        var out = await auth.post("/api/v1/auth/totp/disable", { password: password });
        if (auth.reloadOnCSRF(out, totpMsg)) return;
        if (!out.res.ok) {
          auth.setMsg(totpMsg, (out.data && out.data.error) || "Failed", "error");
          return;
        }
        window.location.reload();
      } catch (err) {
        auth.setMsg(totpMsg, "Network error", "error");
      }
    }));
  }

  var passkeyRegister = document.getElementById("passkey-register");
  if (passkeyRegister) {
    passkeyRegister.addEventListener("click", withBusy(passkeyRegister, async function () {
      auth.setMsg(passkeyMsg, "");
      var envErr = auth.passkeyEnvironmentOK();
      if (envErr) {
        auth.setMsg(passkeyMsg, envErr, "error");
        return;
      }
      var password = document.getElementById("passkey-password").value;
      if (!password) {
        auth.setMsg(passkeyMsg, "Enter your password to register a passkey", "error");
        return;
      }
      try {
        var begin = await auth.post("/api/v1/auth/webauthn/register/begin", { password: password });
        if (auth.reloadOnCSRF(begin, passkeyMsg)) return;
        if (!begin.res.ok) {
          auth.setMsg(passkeyMsg, (begin.data && begin.data.error) || "Registration unavailable", "error");
          return;
        }
        var pk = begin.data && begin.data.publicKey;
        if (!pk || !pk.challenge) {
          auth.setMsg(passkeyMsg, "Invalid registration options from server", "error");
          return;
        }
        var options = auth.preparePublicKey(pk);
        var cred = await navigator.credentials.create({ publicKey: options });
        var finish = await auth.post("/api/v1/auth/webauthn/register/finish", auth.attestationToJSON(cred));
        if (auth.reloadOnCSRF(finish, passkeyMsg)) return;
        if (!finish.res.ok) {
          auth.setMsg(passkeyMsg, (finish.data && finish.data.error) || "Registration failed", "error");
          return;
        }
        window.location.reload();
      } catch (err) {
        var detail = (err && (err.message || err.name)) || "cancelled or failed";
        auth.setMsg(passkeyMsg, "Passkey registration failed: " + detail, "error");
      }
    }));
  }

  document.querySelectorAll(".passkey-delete").forEach(function (btn) {
    btn.addEventListener("click", withBusy(btn, async function () {
      var password = document.getElementById("passkey-password").value;
      if (!password) {
        auth.setMsg(passkeyMsg, "Enter your password to remove a passkey", "error");
        return;
      }
      if (!confirm("Remove this passkey?")) return;
      try {
        var out = await auth.post("/api/v1/auth/webauthn/delete", {
          credentialId: btn.getAttribute("data-id"),
          password: password
        });
        if (auth.reloadOnCSRF(out, passkeyMsg)) return;
        if (!out.res.ok) {
          auth.setMsg(passkeyMsg, (out.data && out.data.error) || "Remove failed", "error");
          return;
        }
        window.location.reload();
      } catch (err) {
        auth.setMsg(passkeyMsg, "Network error", "error");
      }
    }));
  });

  document.getElementById("logout-btn").addEventListener("click", function () {
    auth.logout();
  });
})();
