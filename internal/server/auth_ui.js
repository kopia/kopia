window.KopiaAuth = (function () {
  function csrfMeta() {
    return document.querySelector('meta[name="kopia-csrf-token"]');
  }

  function csrfFromMeta() {
    var meta = csrfMeta();
    return meta ? meta.getAttribute("content") : "";
  }

  var csrf = csrfFromMeta();

  function setCSRF(token) {
    if (!token) return;
    csrf = token;
    var meta = csrfMeta();
    if (meta) meta.setAttribute("content", token);
  }

  function setMsg(el, text, kind) {
    if (!el) return;
    el.textContent = text || "";
    el.className = "msg" + (kind ? " " + kind : "");
  }

  function headers() {
    return {
      "Content-Type": "application/json",
      "X-Kopia-Csrf-Token": csrf || csrfFromMeta(),
      Accept: "application/json"
    };
  }

  async function post(path, body) {
    var res = await fetch(path, {
      method: "POST",
      credentials: "same-origin",
      headers: headers(),
      body: body ? JSON.stringify(body) : "{}"
    });
    var text = await res.text();
    var data = null;
    try {
      data = JSON.parse(text);
    } catch (e) {
      data = { error: text || ("HTTP " + res.status) };
    }
    if (data && data.csrfToken) {
      setCSRF(data.csrfToken);
    }
    return { res: res, data: data };
  }

  async function logout() {
    try {
      await post("/api/v1/auth/logout", {});
    } catch (err) {
    }
    window.location.href = "/login";
  }

  function b64urlToBuf(b64url) {
    var pad = "=".repeat((4 - (b64url.length % 4)) % 4);
    var b64 = (b64url + pad).replace(/-/g, "+").replace(/_/g, "/");
    var str = atob(b64);
    var buf = new ArrayBuffer(str.length);
    var view = new Uint8Array(buf);
    for (var i = 0; i < str.length; i++) view[i] = str.charCodeAt(i);
    return buf;
  }

  function bufToB64url(buf) {
    var view = new Uint8Array(buf);
    var str = "";
    for (var i = 0; i < view.length; i++) str += String.fromCharCode(view[i]);
    return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }

  function assertionToJSON(cred) {
    return {
      id: cred.id,
      rawId: bufToB64url(cred.rawId),
      type: cred.type,
      response: {
        clientDataJSON: bufToB64url(cred.response.clientDataJSON),
        authenticatorData: bufToB64url(cred.response.authenticatorData),
        signature: bufToB64url(cred.response.signature),
        userHandle: cred.response.userHandle ? bufToB64url(cred.response.userHandle) : null
      },
      clientExtensionResults: cred.getClientExtensionResults()
    };
  }

  function attestationToJSON(cred) {
    return {
      id: cred.id,
      rawId: bufToB64url(cred.rawId),
      type: cred.type,
      response: {
        clientDataJSON: bufToB64url(cred.response.clientDataJSON),
        attestationObject: bufToB64url(cred.response.attestationObject)
      },
      clientExtensionResults: cred.getClientExtensionResults()
    };
  }

  function preparePublicKey(options) {
    options.challenge = b64urlToBuf(options.challenge);
    if (options.user && options.user.id) {
      options.user.id = b64urlToBuf(options.user.id);
    }
    if (options.allowCredentials) {
      options.allowCredentials = options.allowCredentials.map(function (c) {
        return Object.assign({}, c, { id: b64urlToBuf(c.id) });
      });
    }
    if (options.excludeCredentials) {
      options.excludeCredentials = options.excludeCredentials.map(function (c) {
        return Object.assign({}, c, { id: b64urlToBuf(c.id) });
      });
    }
    return options;
  }

  function isCSRFError(out) {
    return out.res.status === 401 && /csrf|session cookie/i.test((out.data && out.data.error) || "");
  }

  function reloadOnCSRF(out, msgEl) {
    if (!isCSRFError(out)) return false;
    setMsg(msgEl, "Session expired — reloading…", "error");
    setTimeout(function () { window.location.reload(); }, 600);
    return true;
  }

  function passkeyEnvironmentOK() {
    if (!window.isSecureContext) {
      return "Passkeys require HTTPS or http://localhost";
    }
    if (/^\d+\.\d+\.\d+\.\d+$/.test(location.hostname) || location.hostname.indexOf(":") >= 0) {
      return "Open this page via http://localhost:" + (location.port || "80") + " — passkeys do not work with raw IP addresses";
    }
    if (!window.PublicKeyCredential) {
      return "Passkeys are not supported in this browser";
    }
    return "";
  }

  function goHome() {
    window.location.assign("/");
  }

  return {
    post: post,
    logout: logout,
    setMsg: setMsg,
    setCSRF: setCSRF,
    preparePublicKey: preparePublicKey,
    assertionToJSON: assertionToJSON,
    attestationToJSON: attestationToJSON,
    reloadOnCSRF: reloadOnCSRF,
    passkeyEnvironmentOK: passkeyEnvironmentOK,
    goHome: goHome,
    supportsPasskeys: function () {
      return !!window.PublicKeyCredential;
    }
  };
})();
