// PocketQueue Dashboard - Pure Vanilla JavaScript Controller
document.addEventListener('DOMContentLoaded', () => {
  // --- State ---
  const state = {
    events: [],
    isPaused: false,
    selectedTopic: '',
    searchQuery: '',
    eventsThisSecond: 0,
    sparklineHistory: new Array(30).fill(0),
    topics: [],
    eventSource: null,
  };

  // --- Theme Toggle ---
  const themeToggle = document.getElementById('theme-toggle');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  let currentTheme = localStorage.getItem('pq-theme') || (prefersDark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme', currentTheme);

  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      currentTheme = currentTheme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', currentTheme);
      localStorage.setItem('pq-theme', currentTheme);
    });
  }

  // --- Tab Navigation ---
  const tabBtns = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');
  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetId = btn.getAttribute('data-tab');
      tabBtns.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      const targetPanel = document.getElementById(targetId);
      if (targetPanel) {
        targetPanel.classList.add('active');
        if (targetId === 'tab-topics' || targetId === 'tab-dlq') {
          fetchTopics();
        }
      }
    });
  });

  // --- Modals Helper ---
  function closeModal() {
    document.querySelectorAll('.modal').forEach(m => m.classList.remove('open'));
  }
  document.querySelectorAll('.modal-close').forEach(btn => btn.addEventListener('click', closeModal));
  window.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) closeModal();
  });
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
  });

  // --- Toast Notification ---
  function showToast(message, isError = false) {
    const toast = document.getElementById('toast');
    if (!toast) return;
    toast.textContent = message;
    toast.style.background = isError ? 'var(--pico-danger)' : 'var(--pico-color)';
    toast.style.color = 'var(--pico-bg)';
    toast.classList.add('show');
    setTimeout(() => {
      toast.classList.remove('show');
    }, 3000);
  }

  // --- SSE Stream Consumer ---
  function initSSE() {
    if (state.eventSource) {
      state.eventSource.close();
    }

    const connStatus = document.getElementById('connection-status');
    const sseUrl = '/api/v1/topics/_all/sse';
    const es = new EventSource(sseUrl);
    state.eventSource = es;

    es.onopen = () => {
      if (connStatus) {
        connStatus.className = 'badge badge-success';
        connStatus.textContent = '● Live SSE';
      }
    };

    es.onerror = () => {
      if (connStatus) {
        connStatus.className = 'badge badge-warning';
        connStatus.textContent = '⚠️ Reconnecting...';
      }
    };

    es.onmessage = (e) => {
      try {
        const record = JSON.parse(e.data);
        state.eventsThisSecond++;
        state.events.unshift(record);
        if (state.events.length > 500) {
          state.events.pop();
        }

        if (!state.isPaused) {
          renderStreamTable();
        }
      } catch (err) {
        console.error('Failed to parse SSE event:', err);
      }
    };
  }

  // --- Stream Table Rendering ---
  function matchesFilter(rec) {
    if (state.selectedTopic && rec.topic !== state.selectedTopic) {
      return false;
    }
    if (state.searchQuery) {
      const q = state.searchQuery.toLowerCase();
      const valStr = typeof rec.value === 'string' ? rec.value.toLowerCase() : JSON.stringify(rec.value).toLowerCase();
      const keyStr = (rec.key || '').toLowerCase();
      const topicStr = (rec.topic || '').toLowerCase();
      if (!valStr.includes(q) && !keyStr.includes(q) && !topicStr.includes(q)) {
        return false;
      }
    }
    return true;
  }

  function renderStreamTable() {
    const tbody = document.getElementById('stream-tbody');
    if (!tbody) return;

    const filtered = state.events.filter(matchesFilter);
    if (filtered.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--pico-muted); padding: 2rem;">No matching events.</td></tr>`;
      return;
    }

    const rows = filtered.slice(0, 100).map((rec, idx) => {
      const isDLQ = (rec.topic || '').endsWith('.dlq');
      const timeStr = rec.timestamp ? new Date(rec.timestamp).toLocaleTimeString() : '-';
      const topicBadge = isDLQ ? `<span class="badge badge-danger">🚨 ${escapeHTML(rec.topic)}</span>` : `<span class="badge badge-muted">${escapeHTML(rec.topic)}</span>`;
      const keyDisplay = rec.key ? escapeHTML(rec.key) : '<span style="color:var(--pico-muted)">(none)</span>';
      const valStr = typeof rec.value === 'string' ? rec.value : JSON.stringify(rec.value);
      const valPreview = escapeHTML(valStr.length > 80 ? valStr.substring(0, 80) + '…' : valStr);

      return `
        <tr>
          <td><code style="font-size:0.8rem">${timeStr}</code></td>
          <td>${topicBadge}</td>
          <td>${rec.partition}</td>
          <td><code>#${rec.offset}</code></td>
          <td><code>${keyDisplay}</code></td>
          <td class="payload-cell" title="${escapeHTML(valStr)}">${valPreview}</td>
          <td><button class="secondary sm btn-inspect" data-idx="${idx}">Inspect</button></td>
        </tr>
      `;
    }).join('');

    tbody.innerHTML = rows;

    // Attach inspect handlers
    tbody.querySelectorAll('.btn-inspect').forEach(btn => {
      btn.addEventListener('click', () => {
        const idx = parseInt(btn.getAttribute('data-idx'), 10);
        const item = filtered[idx];
        if (item) {
          openInspectModal(item);
        }
      });
    });
  }

  function openInspectModal(rec) {
    const modal = document.getElementById('modal-inspect');
    const codeEl = document.getElementById('inspect-code');
    if (!modal || !codeEl) return;

    let formatted = rec.value;
    try {
      if (typeof rec.value === 'string') {
        const parsed = JSON.parse(rec.value);
        formatted = JSON.stringify(parsed, null, 2);
      } else {
        formatted = JSON.stringify(rec.value, null, 2);
      }
    } catch (_) {
      formatted = String(rec.value);
    }

    const payloadObj = {
      topic: rec.topic,
      partition: rec.partition,
      offset: rec.offset,
      key: rec.key,
      timestamp: rec.timestamp,
      payload: formatted,
    };

    codeEl.textContent = JSON.stringify(payloadObj, null, 2);
    modal.classList.add('open');
  }

  // --- Throughput Sparkline Canvas ---
  const canvas = document.getElementById('sparkline');
  const rateCounter = document.getElementById('rate-counter');

  function drawSparkline() {
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const w = canvas.width;
    const h = canvas.height;
    ctx.clearRect(0, 0, w, h);

    const data = state.sparklineHistory;
    const max = Math.max(...data, 5);

    ctx.beginPath();
    ctx.strokeStyle = '#10b981';
    ctx.lineWidth = 2;

    for (let i = 0; i < data.length; i++) {
      const x = (i / (data.length - 1)) * w;
      const y = h - (data[i] / max) * (h - 6) - 3;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();

    // Fill under curve
    ctx.lineTo(w, h);
    ctx.lineTo(0, h);
    ctx.closePath();
    ctx.fillStyle = 'rgba(16, 185, 129, 0.15)';
    ctx.fill();
  }

  // Ticking ticker every second
  setInterval(() => {
    state.sparklineHistory.push(state.eventsThisSecond);
    if (state.sparklineHistory.length > 30) {
      state.sparklineHistory.shift();
    }
    if (rateCounter) {
      rateCounter.textContent = `${state.eventsThisSecond} msg/s`;
    }
    state.eventsThisSecond = 0;
    drawSparkline();
  }, 1000);

  // --- Stream Controls ---
  const btnPause = document.getElementById('btn-pause');
  if (btnPause) {
    btnPause.addEventListener('click', () => {
      state.isPaused = !state.isPaused;
      btnPause.textContent = state.isPaused ? '▶ Resume' : '⏸ Pause';
      if (!state.isPaused) {
        renderStreamTable();
      }
    });
  }

  const btnClear = document.getElementById('btn-clear');
  if (btnClear) {
    btnClear.addEventListener('click', () => {
      state.events = [];
      renderStreamTable();
    });
  }

  const filterTopic = document.getElementById('filter-topic');
  if (filterTopic) {
    filterTopic.addEventListener('change', (e) => {
      state.selectedTopic = e.target.value;
      renderStreamTable();
    });
  }

  const filterSearch = document.getElementById('filter-search');
  if (filterSearch) {
    filterSearch.addEventListener('input', (e) => {
      state.searchQuery = e.target.value;
      renderStreamTable();
    });
  }

  // --- Topics & Watermarks Fetcher ---
  async function fetchTopics() {
    try {
      const res = await fetch('/api/v1/topics');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      state.topics = data.topics || [];
      renderTopicsTable();
      updateTopicDropdowns();
      renderDLQTable();
    } catch (err) {
      console.error('Failed to fetch topics:', err);
    }
  }

  function renderTopicsTable() {
    const tbody = document.getElementById('topics-tbody');
    if (!tbody) return;

    if (state.topics.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" style="text-align: center; color: var(--pico-muted); padding: 1.5rem;">No active topics registered.</td></tr>`;
      return;
    }

    tbody.innerHTML = state.topics.map(t => {
      const wmBadges = (t.partitions_metadata || []).map(p =>
        `<span class="badge badge-muted" style="margin-right: 0.35rem;">P${p.id}: ${p.earliest} → ${p.latest} (${p.total_count} msgs)</span>`
      ).join('');

      return `
        <tr>
          <td><strong>${escapeHTML(t.name)}</strong></td>
          <td>${t.partitions}</td>
          <td><code>${t.total_messages}</code></td>
          <td>${wmBadges || '-'}</td>
        </tr>
      `;
    }).join('');
  }

  function updateTopicDropdowns() {
    const sel = document.getElementById('filter-topic');
    if (!sel) return;

    const current = sel.value;
    const opts = ['<option value="">All Topics</option>'];
    state.topics.forEach(t => {
      opts.push(`<option value="${escapeHTML(t.name)}" ${t.name === current ? 'selected' : ''}>${escapeHTML(t.name)}</option>`);
    });
    sel.innerHTML = opts.join('');
  }

  function renderDLQTable() {
    const tbody = document.getElementById('dlq-tbody');
    if (!tbody) return;

    const dlqTopics = state.topics.filter(t => t.name.endsWith('.dlq'));
    if (dlqTopics.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--pico-muted); padding: 1.5rem;">No Dead-Letter Queues (*.dlq) found. Everything is healthy! 🟢</td></tr>`;
      return;
    }

    tbody.innerHTML = dlqTopics.map(t => {
      const targetTopic = t.name.replace(/\.dlq$/, '');
      return `
        <tr>
          <td><span class="badge badge-danger">${escapeHTML(t.name)}</span></td>
          <td><code>${t.total_messages}</code></td>
          <td><strong>${escapeHTML(targetTopic)}</strong></td>
          <td><span class="badge badge-warning">Poison Pill</span></td>
          <td><code>${t.total_messages} failed messages</code></td>
          <td>
            <button class="sm danger btn-open-replay" data-dlq="${escapeHTML(t.name)}" data-target="${escapeHTML(targetTopic)}">
              Inspect & Replay
            </button>
          </td>
        </tr>
      `;
    }).join('');

    tbody.querySelectorAll('.btn-open-replay').forEach(btn => {
      btn.addEventListener('click', () => {
        const dlqTopic = btn.getAttribute('data-dlq');
        const targetTopic = btn.getAttribute('data-target');
        openReplayModal(dlqTopic, targetTopic);
      });
    });
  }

  function openReplayModal(dlqTopic, targetTopic) {
    const modal = document.getElementById('modal-replay');
    if (!modal) return;

    document.getElementById('replay-dlq-topic').value = dlqTopic;
    document.getElementById('replay-target-topic').value = targetTopic;
    document.getElementById('replay-key').value = '';
    document.getElementById('replay-value').value = '{\n  "repaired": true\n}';

    modal.classList.add('open');
  }

  const btnRefreshTopics = document.getElementById('btn-refresh-topics');
  if (btnRefreshTopics) btnRefreshTopics.addEventListener('click', fetchTopics);
  const btnRefreshDLQ = document.getElementById('btn-refresh-dlq');
  if (btnRefreshDLQ) btnRefreshDLQ.addEventListener('click', fetchTopics);

  // --- Web Message Publisher Form ---
  const formPublish = document.getElementById('form-publish');
  if (formPublish) {
    formPublish.addEventListener('submit', async (e) => {
      e.preventDefault();
      const topic = document.getElementById('pub-topic').value.trim();
      const partition = parseInt(document.getElementById('pub-partition').value, 10) || 0;
      const key = document.getElementById('pub-key').value.trim();
      const rawVal = document.getElementById('pub-value').value;
      const rawHeaders = document.getElementById('pub-headers').value.trim();

      if (!topic) return;

      let parsedVal = rawVal;
      try {
        parsedVal = JSON.parse(rawVal);
      } catch (_) {
        // Leave as plain string
      }

      let parsedHeaders = undefined;
      if (rawHeaders) {
        try {
          parsedHeaders = JSON.parse(rawHeaders);
        } catch (_) {
          showToast('Invalid headers JSON format', true);
          return;
        }
      }

      const payload = {
        key: key || undefined,
        value: parsedVal,
        partition: partition,
        headers: parsedHeaders,
      };

      try {
        const res = await fetch(`/api/v1/topics/${encodeURIComponent(topic)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        if (!res.ok) {
          const errText = await res.text();
          throw new Error(errText || `HTTP ${res.status}`);
        }

        const data = await res.json();
        showToast(`✅ Published to ${data.topic} [P${data.partition}, offset ${data.offset}]`);
        document.getElementById('pub-value').value = '';
        fetchTopics();
      } catch (err) {
        showToast(`❌ Publish failed: ${err.message}`, true);
      }
    });
  }

  // --- DLQ Replay Form ---
  const formReplay = document.getElementById('form-replay');
  if (formReplay) {
    formReplay.addEventListener('submit', async (e) => {
      e.preventDefault();
      const dlqTopic = document.getElementById('replay-dlq-topic').value.trim();
      const targetTopic = document.getElementById('replay-target-topic').value.trim();
      const key = document.getElementById('replay-key').value.trim();
      const rawVal = document.getElementById('replay-value').value;

      let parsedVal = rawVal;
      try {
        parsedVal = JSON.parse(rawVal);
      } catch (_) {}

      const payload = {
        dlq_topic: dlqTopic,
        target_topic: targetTopic,
        key: key || undefined,
        value: parsedVal,
        partition: 0,
      };

      try {
        const res = await fetch('/api/v1/dlq/replay', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        if (!res.ok) {
          const errText = await res.text();
          throw new Error(errText || `HTTP ${res.status}`);
        }

        const data = await res.json();
        closeModal();
        showToast(`✅ Replayed to ${data.target_topic} at offset ${data.offset}`);
        fetchTopics();
      } catch (err) {
        showToast(`❌ Replay failed: ${err.message}`, true);
      }
    });
  }

  // --- Utility ---
  function escapeHTML(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // --- Boot Initialization ---
  initSSE();
  fetchTopics();
});
