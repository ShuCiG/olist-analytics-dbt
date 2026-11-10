// Static dashboard: DuckDB-WASM reads data/*.parquet, with the same queries.json that
// dashboard/tests/test_static_site.py runs on the built files.
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm";

const TABLES = ["kpis", "monthly", "categories", "payments", "states", "review_scores", "retention"];
const PERIODS = [["all", "Jan 2017 – Aug 2018"], ["2017", "2017"], ["2018", "Jan – Aug 2018"]];
const SVG = "http://www.w3.org/2000/svg";
const HTML = "http://www.w3.org/1999/xhtml";

const statusEl = document.getElementById("status");
const tooltip = document.getElementById("tooltip");

const number = (v, digits = 0) => v.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const fmt = {
  int: (v) => number(v),
  reais: (v) => `R$ ${number(v)}`,
  millions: (v) => `R$ ${number(v / 1e6, 2)}M`,
  pct0: (v) => `${number(v * 100)}%`,
  pct1: (v) => `${number(v * 100, 1)}%`,
  pct2: (v) => `${number(v * 100, 2)}%`,
  days: (v) => number(v, 1),
  score: (v) => `${number(v, 2)} / 5`,
};
const compact = (v) => {
  if (Math.abs(v) >= 1e6) return `${number(v / 1e6, 1)}M`;
  return Math.abs(v) >= 1e4 ? `${number(v / 1e3)}K` : number(v);
};
const monthLabel = (iso) => new Date(`${iso}T00:00:00Z`).toLocaleString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });

function el(parent, tag, attrs = {}, text) {
  const node = document.createElementNS(tag === "svg" || parent.namespaceURI === SVG ? SVG : HTML, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (text !== undefined) node.textContent = text;
  parent.append(node);
  return node;
}

function showTip(x, y, value, label) {
  tooltip.replaceChildren();
  el(tooltip, "b", {}, value);
  el(tooltip, "span", {}, label);
  tooltip.hidden = false;
  const box = tooltip.getBoundingClientRect();
  tooltip.style.left = `${Math.max(8, Math.min(x + 12, innerWidth - box.width - 8))}px`;
  tooltip.style.top = `${Math.max(8, y - box.height - 8)}px`;
}
const hideTip = () => { tooltip.hidden = true; };

// Four equal ticks on a clean step, so no axis label has to round
function niceTicks(v) {
  const rough = (v || 1) / 4;
  const power = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * power).find((m) => m >= rough);
  return { top: step * 4, step };
}

function header(fig, title, note) {
  fig.replaceChildren();
  el(fig, "figcaption", {}, title);
  if (note) el(fig, "p", { class: "note" }, note);
}

function dataTable(fig, columns, rows) {
  const details = el(fig, "details");
  el(details, "summary", {}, "Table");
  const table = el(el(details, "div", { class: "scroll" }), "table");
  const head = el(el(table, "thead"), "tr");
  for (const [name] of columns) el(head, "th", {}, name);
  const body = el(table, "tbody");
  for (const row of rows) {
    const tr = el(body, "tr");
    for (const [, read] of columns) el(tr, "td", {}, read(row));
  }
}

// One series over an ordered x: 2px line, end dot, snapping crosshair (arrow keys on focus)
function lineChart(fig, { title, note, rows, y, format, xLabel, axis = compact, xName, yName }) {
  header(fig, title, note);
  const width = Math.max(fig.clientWidth - 34, 240);
  const height = 220;
  const m = { top: 10, right: 16, bottom: 26, left: 52 };
  const { top, step } = niceTicks(Math.max(...rows.map(y)));
  const px = (i) => m.left + (i * (width - m.left - m.right)) / Math.max(rows.length - 1, 1);
  const py = (v) => height - m.bottom - (v / top) * (height - m.top - m.bottom);
  const svg = el(fig, "svg", { viewBox: `0 0 ${width} ${height}`, height, role: "img", "aria-label": `${title}: line chart`, tabindex: 0 });
  for (let t = 0; t <= 4; t++) {
    const value = step * t;
    el(svg, "line", { class: "grid", x1: m.left, x2: width - m.right, y1: py(value), y2: py(value) });
    el(svg, "text", { x: m.left - 8, y: py(value) + 4, "text-anchor": "end" }, axis(value));
  }
  const every = Math.ceil(rows.length / Math.max(Math.floor((width - m.left) / 70), 2));
  rows.forEach((row, i) => {
    if (i % every === 0) el(svg, "text", { x: px(i), y: height - 8, "text-anchor": i === 0 ? "start" : "middle" }, xLabel(row));
  });
  el(svg, "path", { class: "line", d: rows.map((r, i) => `${i ? "L" : "M"}${px(i)} ${py(y(r))}`).join("") });
  const last = rows.length - 1;
  el(svg, "circle", { class: "dot", cx: px(last), cy: py(y(rows[last])), r: 5 });
  const cross = el(svg, "line", { class: "crosshair", y1: m.top, y2: height - m.bottom, visibility: "hidden" });
  const marker = el(svg, "circle", { class: "dot", r: 5, visibility: "hidden" });
  let current = last;
  const show = (i, x, yPos) => {
    current = i;
    cross.setAttribute("x1", px(i));
    cross.setAttribute("x2", px(i));
    marker.setAttribute("cx", px(i));
    marker.setAttribute("cy", py(y(rows[i])));
    cross.setAttribute("visibility", "visible");
    marker.setAttribute("visibility", "visible");
    showTip(x, yPos, format(y(rows[i])), xLabel(rows[i]));
  };
  const hide = () => {
    cross.setAttribute("visibility", "hidden");
    marker.setAttribute("visibility", "hidden");
    hideTip();
  };
  const clamp = (i) => Math.max(0, Math.min(last, i));
  svg.addEventListener("pointermove", (e) => {
    const box = svg.getBoundingClientRect();
    const at = ((e.clientX - box.left) / box.width) * width;
    show(clamp(Math.round(((at - m.left) / (width - m.left - m.right)) * last)), e.clientX, e.clientY);
  });
  svg.addEventListener("pointerleave", hide);
  svg.addEventListener("blur", hide);
  svg.addEventListener("keydown", (e) => {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
    e.preventDefault();
    const i = clamp(current + (e.key === "ArrowLeft" ? -1 : 1));
    const box = svg.getBoundingClientRect();
    show(i, box.left + (px(i) / width) * box.width, box.top + py(y(rows[i])));
  });
  dataTable(fig, [[xName, xLabel], [yName, (r) => format(y(r))]], rows);
}

// Horizontal bars from one baseline, 4px rounded data end; one or two series per category
function barChart(fig, { title, note, rows, category, series, format, tipExtra, columns }) {
  header(fig, title, note);
  if (series.length > 1) {
    const legend = el(fig, "div", { class: "legend" });
    series.forEach((s, n) => {
      const key = el(legend, "span");
      el(key, "i", { class: `s${n + 1}` });
      key.append(s.name);
    });
  }
  const width = Math.max(fig.clientWidth - 34, 240);
  const names = [...new Set(rows.map(category))];
  const barHeight = series.length > 1 ? 12 : 18;
  const band = series.length * (barHeight + 2) + 8;
  const left = Math.min(Math.max(...names.map((n) => n.length)) * 7 + 10, width * 0.45);
  const right = 58;
  const height = names.length * band + 4;
  const top = Math.max(...rows.map(series[0].value));
  const scale = (v) => (v / top) * (width - left - right);
  const svg = el(fig, "svg", { viewBox: `0 0 ${width} ${height}`, height, role: "img", "aria-label": `${title}: bar chart` });
  names.forEach((name, i) => {
    const y0 = i * band + 4;
    const shown = name.length * 7 + 10 > left ? `${name.slice(0, Math.floor((left - 16) / 7))}…` : name;
    el(svg, "text", { x: left - 8, y: y0 + (series.length * (barHeight + 2)) / 2 + 2, "text-anchor": "end" }, shown);
    series.forEach((s, n) => {
      const row = rows.find((r) => category(r) === name && s.match(r));
      if (!row) return;
      const w = Math.max(scale(s.value(row)), 1);
      const y = y0 + n * (barHeight + 2);
      const r = Math.min(4, w);
      const bar = el(svg, "path", {
        class: `bar s${n + 1}`,
        d: `M${left} ${y}h${w - r}a${r} ${r} 0 0 1 ${r} ${r}v${barHeight - 2 * r}a${r} ${r} 0 0 1 ${-r} ${r}h${r - w}z`,
      });
      const tip = `${s.name ? `${s.name}, ` : ""}${name} · ${tipExtra(row)}`;
      bar.addEventListener("pointermove", (e) => showTip(e.clientX, e.clientY, format(s.value(row)), tip));
      bar.addEventListener("pointerleave", hideTip);
      el(svg, "text", { x: left + w + 6, y: y + barHeight / 2 + 4 }, format(s.value(row)));
    });
  });
  dataTable(fig, columns, rows);
}

function tiles(id, items) {
  const box = document.getElementById(id);
  box.replaceChildren();
  for (const [label, value] of items) {
    const tile = el(box, "div", { class: "tile" });
    el(tile, "div", { class: "label" }, label);
    el(tile, "div", { class: "value" }, value);
  }
}

function render(d) {
  const k = d.kpis[0];
  tiles("tiles-sales", [
    ["Orders", fmt.int(k.orders)],
    ["Customers", fmt.int(k.customers)],
    ["Revenue", fmt.millions(k.revenue)],
    ["Avg order value", fmt.reais(k.avg_order_value)],
  ]);
  tiles("tiles-delivery", [
    ["Delivered orders", fmt.int(k.delivered_orders)],
    ["Avg days to deliver", fmt.days(k.avg_days_to_deliver)],
    ["Delivered late", fmt.pct1(k.late_delivery_rate)],
    ["Avg review score", fmt.score(k.avg_review_score)],
  ]);
  tiles("tiles-cohorts", [
    ["New customers", fmt.int(k.new_customers)],
    ["Back in month 1", fmt.pct2(k.retention_m1)],
    ["Back in month 3", fmt.pct2(k.retention_m3)],
    ["3-month value per customer", fmt.reais(k.ltv_3m)],
  ]);

  const figure = (id) => document.getElementById(id);
  const byMonth = { rows: d.monthly, xLabel: (r) => monthLabel(r.month), xName: "Month" };
  lineChart(figure("chart-orders"), { ...byMonth, title: "Orders per month", y: (r) => r.orders, format: fmt.int, yName: "Orders" });
  lineChart(figure("chart-revenue"), { ...byMonth, title: "Revenue per month, R$", y: (r) => r.revenue, format: fmt.reais, yName: "Revenue" });
  lineChart(figure("chart-late"), { ...byMonth, title: "Late deliveries by purchase month", y: (r) => r.late_delivery_rate, format: fmt.pct1, axis: fmt.pct0, yName: "Delivered late" });
  lineChart(figure("chart-days"), { ...byMonth, title: "Days to deliver by purchase month", y: (r) => r.avg_days_to_deliver, format: fmt.days, axis: fmt.int, yName: "Avg days" });

  const one = (value) => [{ name: "", value, match: () => true }];
  barChart(figure("chart-categories"), {
    title: "Top 10 categories by item revenue",
    rows: d.categories,
    category: (r) => r.category,
    series: one((r) => r.revenue),
    format: (v) => `R$ ${compact(v)}`,
    tipExtra: (r) => `${fmt.int(r.orders)} orders`,
    columns: [["Category", (r) => r.category], ["Item revenue", (r) => fmt.reais(r.revenue)], ["Orders", (r) => fmt.int(r.orders)]],
  });
  barChart(figure("chart-payments"), {
    title: "Payment mix",
    note: "Share of paid amount. Boleto: Brazilian bank slip, paid in cash or online banking.",
    rows: d.payments,
    category: (r) => r.method,
    series: one((r) => r.share),
    format: fmt.pct1,
    tipExtra: (r) => fmt.reais(r.amount),
    columns: [["Method", (r) => r.method], ["Share", (r) => fmt.pct1(r.share)], ["Amount", (r) => fmt.reais(r.amount)]],
  });
  barChart(figure("chart-states"), {
    title: "Highest late rate by customer state",
    note: "States with at least 100 delivered orders in the period.",
    rows: d.states,
    category: (r) => r.state,
    series: one((r) => r.late_delivery_rate),
    format: fmt.pct1,
    tipExtra: (r) => `${fmt.int(r.orders)} delivered orders`,
    columns: [["State", (r) => r.state], ["Delivered late", (r) => fmt.pct1(r.late_delivery_rate)], ["Delivered orders", (r) => fmt.int(r.orders)]],
  });
  barChart(figure("chart-scores"), {
    title: "Review scores of late and on-time orders",
    note: "Share of each group's reviews, by stars.",
    rows: d.review_scores,
    category: (r) => `${r.review_score} ★`,
    series: ["On time", "Late"].map((name) => ({ name, value: (r) => r.share, match: (r) => r.delivery === name })),
    format: fmt.pct0,
    tipExtra: (r) => `${fmt.int(r.orders)} reviews`,
    columns: [["Stars", (r) => r.review_score], ["Delivery", (r) => r.delivery], ["Share", (r) => fmt.pct1(r.share)], ["Reviews", (r) => fmt.int(r.orders)]],
  });

  const byAge = { xLabel: (r) => String(r.months_since_first_order), xName: "Month" };
  lineChart(figure("chart-retention"), {
    ...byAge,
    title: "Share who ordered again, by months since first order",
    note: "Months with fewer than 10 returning people are left out.",
    rows: d.retention.filter((r) => r.months_since_first_order > 0),
    y: (r) => r.retention_rate,
    format: fmt.pct2,
    axis: fmt.pct1,
    yName: "Ordered again",
  });
  lineChart(figure("chart-value"), {
    ...byAge,
    title: "Revenue to date per acquired customer, R$",
    rows: d.retention,
    y: (r) => r.value_per_customer,
    format: fmt.reais,
    axis: fmt.int,
    yName: "Revenue to date",
  });
}

async function startEngine() {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const script = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(script));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(script);
  for (const name of TABLES) {
    await db.registerFileURL(`${name}.parquet`, new URL(`data/${name}.parquet`, location.href).href, duckdb.DuckDBDataProtocol.HTTP, false);
  }
  return db.connect();
}

const plain = (table) => table.toArray().map((row) => Object.fromEntries(
  Object.entries(row.toJSON()).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : v]),
));

async function main() {
  const started = performance.now();
  const queries = await (await fetch("queries.json")).json();
  const conn = await startEngine();
  const statements = {};
  for (const [name, sql] of Object.entries(queries)) statements[name] = await conn.prepare(sql);
  const content = document.getElementById("content");
  const buttons = document.getElementById("periods");
  let data;

  async function load(periodId) {
    content.classList.add("refreshing");
    const loaded = {};
    for (const [name, statement] of Object.entries(statements)) loaded[name] = plain(await statement.query(periodId));
    data = loaded;
    render(data);
    content.classList.remove("refreshing");
    for (const b of buttons.children) b.setAttribute("aria-pressed", String(b.dataset.period === periodId));
  }

  for (const [id, label] of PERIODS) {
    const button = el(buttons, "button", { type: "button", "data-period": id, "aria-pressed": "false" }, label);
    button.addEventListener("click", () => load(id));
  }
  await load("all");
  let resize;
  addEventListener("resize", () => {
    clearTimeout(resize);
    resize = setTimeout(() => render(data), 150);
  });
  statusEl.textContent = `Queried in your browser in ${((performance.now() - started) / 1000).toFixed(1)} s.`;
  document.body.dataset.ready = "true";
}

main().catch((error) => {
  console.error(error);
  statusEl.className = "error";
  statusEl.replaceChildren("The in-browser query engine could not start. Try the ");
  el(statusEl, "a", { href: "https://olist-analytics-dbt.streamlit.app/" }, "Streamlit app");
  statusEl.append(" instead.");
});
