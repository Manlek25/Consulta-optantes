const form = document.getElementById("formUpload");
const fileEl = document.getElementById("file");
const outputEl = document.getElementById("output");
const delayEl = document.getElementById("delay");

const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const btn = document.getElementById("processBtn");
const cancelBtn = document.getElementById("cancelBtn");
const spinner = document.getElementById("spinner");

const statusTitle = document.getElementById("statusTitle");
const statusSub = document.getElementById("statusSub");
const barFill = document.getElementById("barFill");

function showError(msg) {
  errorEl.textContent = msg;
  errorEl.hidden = false;
}

function clearError() {
  errorEl.hidden = true;
  errorEl.textContent = "";
}

function setLoading(isLoading) {
  btn.disabled = isLoading;
  statusEl.hidden = !isLoading;
  if (cancelBtn) cancelBtn.hidden = !isLoading;
  if (cancelBtn) cancelBtn.disabled = false;
  if (spinner) spinner.hidden = !isLoading;
}

function resetUI() {
  clearError();
  statusEl.hidden = true;
  btn.disabled = false;
  if (cancelBtn) cancelBtn.hidden = true;
  if (cancelBtn) cancelBtn.disabled = false;
  if (spinner) spinner.hidden = true;
  if (barFill) barFill.style.width = "0%";
  if (statusTitle) statusTitle.textContent = "";
  if (statusSub) statusSub.textContent = "";
}

window.addEventListener("DOMContentLoaded", resetUI);

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  clearError();

  const file = fileEl.files[0];
  if (!file) {
    showError("Selecione um arquivo CSV ou XLSX/XLS.");
    return;
  }

  const output = outputEl.value;
  const delay = delayEl.value;

  const fd = new FormData();
  fd.append("file", file);

  setLoading(true);
  statusTitle.textContent = "Enviando arquivo...";
  statusSub.textContent = "";
  barFill.style.width = "0%";

  let jobId = null;
  let es = null;
  let canceled = false;

  const doCancel = async () => {
    if (!jobId) return;
    canceled = true;
    if (cancelBtn) cancelBtn.disabled = true;
    if (statusTitle) statusTitle.textContent = "Cancelando...";
    try {
      await fetch(`/cancel/${jobId}`, { method: "POST" });
    } catch {}
    if (es) es.close();
    // a gente vai checar status até virar canceled/done, e então baixar parcial (se houver)
    try {
      const st = await fetch(`/status/${jobId}`);
      if (st.ok) {
        const s = await st.json();
        if (s.has_file) {
          if (statusTitle) statusTitle.textContent = "Cancelado — gerando download parcial...";
          const down = await fetch(`/download/${jobId}`);
          if (down.ok) {
            const blob = await down.blob();
            const filename = output === "csv" ? "resultado_parcial.csv" : "resultado_parcial.xlsx";
            const a = document.createElement("a");
            const url = window.URL.createObjectURL(blob);
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
          }
        }
      }
    } catch {}
    setLoading(false);
  };

  if (cancelBtn) {
    cancelBtn.onclick = doCancel;
  }

  try {
    const res = await fetch(
      `/lotes?output=${encodeURIComponent(output)}&sleep_seconds=${encodeURIComponent(delay)}`,
      { method: "POST", body: fd }
    );

    if (!res.ok) {
      const t = await res.text();
      throw new Error(t);
    }

    const data = await res.json();
    jobId = data.job_id;

    statusTitle.textContent = "Processando...";

    es = new EventSource(`/progresso/${jobId}`);

    es.addEventListener("progress", (ev) => {
      const payload = JSON.parse(ev.data);
      const progress = payload.progress || 0;
      const total = payload.total || 0;
      const pct = total ? Math.round((progress / total) * 100) : 0;

      // UI extra: se está cancelando, reflete isso
      if (payload.status === "canceling" || payload.status === "canceled") {
        statusTitle.textContent = payload.status === "canceled" ? "Cancelado" : "Cancelando...";
      }

      statusSub.textContent = `${progress}/${total} (${pct}%)`;
      barFill.style.width = `${pct}%`;
    });

    es.addEventListener("error", async (ev) => {
      // Erro "real" vindo do backend via evento SSE
      es.close();
      const msg = ev.data || "Falha no processamento";
      showError(`Erro: ${msg}`);
      setLoading(false);
    });

    es.addEventListener("done", async () => {
      // Job finalizou
      es.close();
      // Se cancelou, ainda liberamos download do parcial
      statusTitle.textContent = canceled ? "Cancelado — gerando download parcial..." : "Gerando download...";
      barFill.style.width = "100%";

      const down = await fetch(`/download/${jobId}`);
      if (!down.ok) {
        const t = await down.text();
        throw new Error(t);
      }

      const blob = await down.blob();
      const filename = canceled
        ? (output === "csv" ? "resultado_parcial.csv" : "resultado_parcial.xlsx")
        : (output === "csv" ? "resultado.csv" : "resultado.xlsx");

      const a = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      setLoading(false);
    });

    // ⚠️ Esse é o "error" do próprio EventSource (queda/reconexão/fechamento).
    // A gente verifica no /status antes de acusar erro.
    es.onerror = async () => {
      try {
        const st = await fetch(`/status/${jobId}`);
        if (st.ok) {
          const s = await st.json();

          if (s.status === "done") {
            // job concluiu e SSE só oscilou
            return;
          }

          if (s.status === "canceled" && s.has_file) {
            // job cancelado e já tem parcial pronto
            es.close();
            statusTitle.textContent = "Cancelado — gerando download parcial...";
            const down = await fetch(`/download/${jobId}`);
            if (down.ok) {
              const blob = await down.blob();
              const filename = output === "csv" ? "resultado_parcial.csv" : "resultado_parcial.xlsx";
              const a = document.createElement("a");
              const url = window.URL.createObjectURL(blob);
              a.href = url;
              a.download = filename;
              document.body.appendChild(a);
              a.click();
              a.remove();
              window.URL.revokeObjectURL(url);
            }
            setLoading(false);
            return;
          }

          if (s.status === "error") {
            es.close();
            showError(`Erro: ${s.error || "Falha no processamento"}`);
            setLoading(false);
            return;
          }

          // se ainda está rodando/cancelando, ignora oscilação
          if (s.status === "running" || s.status === "queued" || s.status === "canceling") {
            return;
          }
        }
      } catch {}

      // fallback: se não conseguiu checar status, mostra aviso leve
      // (não fecha o app, mas libera o botão)
      showError("Falha ao acompanhar progresso (SSE). Tente novamente.");
      setLoading(false);
      if (es) es.close();
    };
  } catch (err) {
    showError(`Erro: ${err.message}`);
    setLoading(false);
    if (es) es.close();
  }
});