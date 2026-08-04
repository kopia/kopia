(function () {
  var NAV_ID = "kopia-auth-nav";

  function csrfToken() {
    var meta = document.querySelector('meta[name="kopia-csrf-token"]');
    return meta ? meta.getAttribute("content") : "";
  }

  async function logout(e) {
    e.preventDefault();
    if (window.KopiaAuth && window.KopiaAuth.logout) {
      window.KopiaAuth.logout();
      return;
    }
    try {
      await fetch("/api/v1/auth/logout", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-Kopia-Csrf-Token": csrfToken(),
          Accept: "application/json"
        },
        body: "{}"
      });
    } catch (err) {
    }
    window.location.href = "/login";
  }

  function ensureNav() {
    if (document.getElementById(NAV_ID)) return true;

    var collapse = document.getElementById("basic-navbar-nav");
    if (!collapse) return false;

    var nav = document.createElement("div");
    nav.id = NAV_ID;
    nav.className = "navbar-nav ms-auto";
    nav.innerHTML =
      '<a class="nav-link" data-testid="tab-security" href="/account/security">Security</a>' +
      '<a class="nav-link" data-testid="tab-logout" href="/login" id="kopia-logout-link">Logout</a>';

    collapse.appendChild(nav);

    var logoutLink = document.getElementById("kopia-logout-link");
    if (logoutLink) logoutLink.addEventListener("click", logout);

    return true;
  }

  function start() {
    ensureNav();

    var root = document.getElementById("root") || document.body;
    var observer = new MutationObserver(function () {
      ensureNav();
    });
    observer.observe(root, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
