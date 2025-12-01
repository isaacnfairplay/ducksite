import { readPageConfig } from "./page_config.js";

function loadIdentity() {
  return {
    email: localStorage.getItem("ducksite_user_email") || "",
    password: localStorage.getItem("ducksite_user_password") || "",
  };
}

function saveIdentity(email, password) {
  localStorage.setItem("ducksite_user_email", email || "");
  localStorage.setItem("ducksite_user_password", password || "");
}

function markPasswordSet(email) {
  if (!email) return;
  const key = `ducksite_user_password_set_for:${email.toLowerCase()}`;
  localStorage.setItem(key, "1");
}

function isPasswordMarkedSet(email) {
  if (!email) return false;
  const key = `ducksite_user_password_set_for:${email.toLowerCase()}`;
  return localStorage.getItem(key) === "1";
}

function allowedDomainOk(email, allowedDomains) {
  if (!allowedDomains) return true;
  if (!email) return false;
  const parts = allowedDomains
    .split(";")
    .map((p) => p.trim())
    .filter(Boolean);
  return parts.some((dom) => email.toLowerCase().endsWith(dom.toLowerCase()));
}

export function initFormsUI(inputsRef) {
  const pageConfig = readPageConfig();
  const forms = (pageConfig && pageConfig.forms) || [];
  if (!forms.length) return;

  let bar = document.querySelector(".ducksite-input-bar");
  if (!bar) {
    bar = document.createElement("div");
    bar.className = "ducksite-input-bar";
    document.body.insertBefore(bar, document.body.firstChild);
  }

  const identity = loadIdentity();
  const allowed = forms.map((f) => f.allowed_email_domains).filter(Boolean).join(";");

  const idGroup = document.createElement("div");
  idGroup.className = "ducksite-input-group";
  const emailInput = document.createElement("input");
  emailInput.type = "email";
  emailInput.placeholder = "email";
  emailInput.value = identity.email;
  emailInput.onchange = () => {
    saveIdentity(emailInput.value, passwordInput.value);
  };
  const passwordInput = document.createElement("input");
  passwordInput.type = "password";
  passwordInput.placeholder = "password (optional)";
  passwordInput.value = identity.password;
  passwordInput.onchange = () => {
    saveIdentity(emailInput.value, passwordInput.value);
  };
  const updateBtn = document.createElement("button");
  updateBtn.type = "button";
  updateBtn.textContent = "Change password";
  updateBtn.onclick = async () => {
    const currentIdentity = loadIdentity();
    const email = window.prompt("Email for password update:", currentIdentity.email || "");
    if (!email) {
      alert("Email is required.");
      return;
    }
    const oldPwd = window.prompt("Current password:", "");
    if (!oldPwd) {
      alert("Current password is required.");
      return;
    }
    const newPwd1 = window.prompt("New password:", "");
    const newPwd2 = window.prompt("Repeat new password:", "");
    if (!newPwd1 || !newPwd2) {
      alert("New password is required.");
      return;
    }
    if (newPwd1 !== newPwd2) {
      alert("New passwords do not match.");
      return;
    }

    const resp = await fetch("/api/auth/update_password", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, old_password: oldPwd, new_password: newPwd1 }),
    });
    const json = await resp.json();
    if (json.status === "ok") {
      saveIdentity(email, newPwd1);
      markPasswordSet(email);
      alert("Password updated.");
    } else if (String(json.error || "").toLowerCase().includes("unauthorized")) {
      alert("Unauthorized: current password is incorrect.");
    } else {
      alert(`Password update error: ${json.error}`);
    }
  };
  idGroup.appendChild(emailInput);
  idGroup.appendChild(passwordInput);
  idGroup.appendChild(updateBtn);
  bar.appendChild(idGroup);

  forms.forEach((form) => {
    const wrapper = document.createElement("div");
    wrapper.className = "ducksite-input-group";
    const label = document.createElement("span");
    label.textContent = form.label || form.id;
    wrapper.appendChild(label);

    if (form.image_field) {
      const file = document.createElement("input");
      file.type = "file";
      file.accept = "image/*";
      file.dataset.formId = form.id;
      wrapper.appendChild(file);
    }

    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Submit";
    btn.onclick = async () => {
      const identityNow = loadIdentity();
      if (form.auth_required && !identityNow.email) {
        alert("Email required before submitting.");
        return;
      }
      if (allowed && !allowedDomainOk(identityNow.email, allowed)) {
        alert("Email domain not allowed");
        return;
      }

      const firstTime = form.auth_required && !isPasswordMarkedSet(identityNow.email);
      if (firstTime && identityNow.password) {
        const ok = window.confirm(
          `Set this password for ${identityNow.email}? You'll need it for future submissions.`,
        );
        if (!ok) {
          saveIdentity(identityNow.email, "");
          passwordInput.value = "";
          return;
        }
      }

      const inputs = typeof window.ducksiteGetInputs === "function"
        ? window.ducksiteGetInputs()
        : { ...inputsRef };
      inputs._user_email = identityNow.email;
      if (identityNow.password) inputs._user_password = identityNow.password;

      const payload = {
        form_id: form.id,
        inputs,
      };

      const fileInput = wrapper.querySelector("input[type='file']");
      let resp;
      if (fileInput && fileInput.files && fileInput.files[0]) {
        const fd = new FormData();
        fd.append("form_id", form.id);
        fd.append("inputs", JSON.stringify(inputs));
        fd.append(form.image_field, fileInput.files[0]);
        resp = await fetch("/api/forms/submit", {
          method: "POST",
          body: fd,
        });
      } else {
        resp = await fetch("/api/forms/submit", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      const json = await resp.json();
      if (json.error) {
        if (String(json.error).toLowerCase().includes("unauthorized")) {
          alert("Unauthorized: wrong or missing password. Please check or update your password.");
          passwordInput.value = "";
          saveIdentity(identityNow.email, "");
        } else {
          alert(`Form error: ${json.error}`);
        }
      } else {
        if (json.auth_status === "set" && identityNow.email) {
          markPasswordSet(identityNow.email);
          alert(`Password set for ${identityNow.email}. You’ll need it for future submissions.`);
        } else {
          alert("Submitted");
        }
      }
    };

    wrapper.appendChild(btn);
    bar.appendChild(wrapper);
  });
}
