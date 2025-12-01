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
  idGroup.appendChild(emailInput);
  idGroup.appendChild(passwordInput);
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
        alert(`Form error: ${json.error}`);
      } else {
        alert("Submitted");
      }
    };

    wrapper.appendChild(btn);
    bar.appendChild(wrapper);
  });
}
