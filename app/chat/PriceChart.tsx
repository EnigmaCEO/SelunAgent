"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import type { PriceChart as PriceChartData } from "./types";
import styles from "./chat.module.css";

// The bottom holds two rows: the low's label, then the months.
const PAD = { top: 26, right: 12, bottom: 50, left: 58 };

const dayDate = (day: string) => new Date(`${day}T00:00:00Z`);
const longDay = (day: string) => dayDate(day).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
const shortDay = (day: string) => dayDate(day).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

function usd(value: number): string {
  if (value >= 1000) return `$${Math.round(value).toLocaleString("en-US")}`;
  if (value >= 1) return `$${value.toFixed(2)}`;
  return `$${value.toPrecision(4)}`;
}

function usdAxis(value: number): string {
  if (value >= 1000) return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value);
  return usd(value);
}

/** Three to five round levels that span the data. */
function levels(min: number, max: number): number[] {
  const span = max - min || max || 1;
  const raw = span / 4;
  const step = [1, 2, 2.5, 5, 10].map((factor) => factor * 10 ** Math.floor(Math.log10(raw))).find((candidate) => candidate >= raw) ?? raw;
  const out: number[] = [];
  for (let value = Math.ceil(min / step) * step; value <= max + step * 1e-9; value += step) out.push(Number(value.toPrecision(12)));
  return out;
}

function useWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.round(entry.contentRect.width)));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}

function downloadCsv(chart: PriceChartData) {
  const lines = ["Day (UTC),Close (USD)", ...chart.closes.map(([day, close]) => `${day},${close}`)];
  const url = URL.createObjectURL(new Blob([`${lines.join("\n")}\n`], { type: "text/csv" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = `${chart.asset.toLowerCase()}-daily-closes-${chart.closes.at(-1)?.[0] ?? ""}.csv`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** The daily closes an analysis was scored on, with its largest drop marked. Every point is from the result. */
export default function PriceChart({ chart }: { chart: PriceChartData }) {
  const [frame, width] = useWidth();
  const [active, setActive] = useState<number | null>(null);
  const titleId = useId();
  const { closes, largestDrawdown: drop } = chart;
  const narrow = width > 0 && width < 480;
  const height = narrow ? 190 : 220;
  const plotWidth = Math.max(width - PAD.left - PAD.right, 10);
  const plotHeight = height - PAD.top - PAD.bottom;

  const geometry = useMemo(() => {
    const values = closes.map(([, close]) => close);
    const low = Math.min(...values);
    const high = Math.max(...values);
    const ticks = levels(low, high);
    const yMin = Math.min(low, ticks[0] ?? low);
    const yMax = Math.max(high, ticks.at(-1) ?? high);
    const start = dayDate(closes[0][0]).getTime();
    const end = dayDate(closes[closes.length - 1][0]).getTime();
    const x = (day: string) => PAD.left + ((dayDate(day).getTime() - start) / (end - start || 1)) * plotWidth;
    const y = (value: number) => PAD.top + (1 - (value - yMin) / (yMax - yMin || 1)) * plotHeight;
    const points = closes.map(([day, close]) => [x(day), y(close)] as const);
    const line = points.map(([px, py], index) => `${index === 0 ? "M" : "L"}${px.toFixed(1)},${py.toFixed(1)}`).join("");
    const area = `${line}L${points[points.length - 1][0].toFixed(1)},${PAD.top + plotHeight}L${points[0][0].toFixed(1)},${PAD.top + plotHeight}Z`;
    const fromIndex = drop ? closes.findIndex(([day]) => day === drop.fromDay) : -1;
    const toIndex = drop ? closes.findIndex(([day]) => day === drop.toDay) : -1;
    const fall = fromIndex >= 0 && toIndex > fromIndex ? points.slice(fromIndex, toIndex + 1).map(([px, py], index) => `${index === 0 ? "M" : "L"}${px.toFixed(1)},${py.toFixed(1)}`).join("") : null;
    // Month labels, thinned to fit.
    const months: { x: number; label: string }[] = [];
    const cursor = dayDate(closes[0][0]);
    cursor.setUTCDate(1);
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    while (cursor.getTime() <= end) {
      const day = cursor.toISOString().slice(0, 10);
      const month = cursor.toLocaleDateString("en-US", { month: "short", timeZone: "UTC" });
      months.push({ x: x(day), label: cursor.getUTCMonth() === 0 ? `${month} ${cursor.getUTCFullYear()}` : month });
      cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    }
    const every = Math.max(1, Math.ceil(months.length / Math.max(2, Math.floor(plotWidth / 64))));
    return { ticks, x, y, points, line, area, fall, fromIndex, toIndex, months: months.filter((_, index) => index % every === 0) };
  }, [closes, drop, plotWidth, plotHeight]);

  const pick = useCallback((clientX: number, element: SVGSVGElement) => {
    const left = element.getBoundingClientRect().left;
    const target = clientX - left;
    let best = 0;
    for (let index = 1; index < geometry.points.length; index += 1) {
      if (Math.abs(geometry.points[index][0] - target) < Math.abs(geometry.points[best][0] - target)) best = index;
    }
    setActive(best);
  }, [geometry]);

  const onKey = (event: KeyboardEvent<SVGSVGElement>) => {
    const last = closes.length - 1;
    const step = event.shiftKey ? 30 : 1;
    const next = event.key === "ArrowLeft" ? Math.max(0, (active ?? last) - step)
      : event.key === "ArrowRight" ? Math.min(last, (active ?? 0) + step)
      : event.key === "Home" ? 0
      : event.key === "End" ? last
      : event.key === "Escape" ? null
      : undefined;
    if (next === undefined) return;
    event.preventDefault();
    setActive(next);
  };

  const first = closes[0];
  const last = closes[closes.length - 1];
  const summary = `${chart.name} daily closes from ${longDay(first[0])} to ${longDay(last[0])}, ${usd(first[1])} to ${usd(last[1])}.${drop ? ` Largest drop ${drop.pct}%, from ${usd(drop.fromClose)} on ${longDay(drop.fromDay)} to ${usd(drop.toClose)} on ${longDay(drop.toDay)}.` : ""}`;
  const peak = geometry.fromIndex >= 0 ? geometry.points[geometry.fromIndex] : null;
  const trough = geometry.toIndex >= 0 ? geometry.points[geometry.toIndex] : null;
  const anchor = (px: number) => (px < PAD.left + 70 ? "start" : px > PAD.left + plotWidth - 70 ? "end" : "middle");
  const hover = active === null ? null : { day: closes[active][0], close: closes[active][1], at: geometry.points[active] };

  return (
    <figure className={styles.priceChart} aria-labelledby={titleId}>
      <figcaption className={styles.priceChartHeader}>
        <div>
          <h4 id={titleId}>{chart.name} price, daily closes</h4>
          <p>{longDay(first[0])} to {longDay(last[0])} · the {closes.length} closes this analysis measured</p>
        </div>
        {drop && (
          <p className={styles.priceChartDrop}>
            <span className={styles.priceChartKey} aria-hidden />
            Largest drop <strong>−{drop.pct}%</strong>
            <span className={styles.priceChartDropDays}>{shortDay(drop.fromDay)} → {shortDay(drop.toDay)}</span>
          </p>
        )}
      </figcaption>
      <div ref={frame} className={styles.priceChartFrame}>
        {width > 0 && (
          <svg
            width={width}
            height={height}
            role="img"
            aria-label={summary}
            tabIndex={0}
            onPointerMove={(event: PointerEvent<SVGSVGElement>) => pick(event.clientX, event.currentTarget)}
            onPointerDown={(event: PointerEvent<SVGSVGElement>) => pick(event.clientX, event.currentTarget)}
            onPointerLeave={() => setActive(null)}
            onBlur={() => setActive(null)}
            onKeyDown={onKey}
          >
            {geometry.ticks.map((tick) => (
              <g key={tick}>
                <line className={styles.priceChartGrid} x1={PAD.left} x2={PAD.left + plotWidth} y1={geometry.y(tick)} y2={geometry.y(tick)} />
                <text className={styles.priceChartAxis} x={PAD.left - 8} y={geometry.y(tick)} dy="0.32em" textAnchor="end">{usdAxis(tick)}</text>
              </g>
            ))}
            {geometry.months.map((month) => (
              <text key={month.x} className={styles.priceChartAxis} x={month.x} y={PAD.top + plotHeight + 38} textAnchor="middle">{month.label}</text>
            ))}
            {peak && trough && (
              <rect className={styles.priceChartBand} x={peak[0]} y={PAD.top} width={Math.max(trough[0] - peak[0], 1)} height={plotHeight} />
            )}
            <path className={styles.priceChartArea} d={geometry.area} />
            <path className={styles.priceChartLine} d={geometry.line} />
            {geometry.fall && <path className={styles.priceChartFall} d={geometry.fall} />}
            {peak && drop && (
              <g>
                <circle className={styles.priceChartMarker} cx={peak[0]} cy={peak[1]} r={4.5} />
                <text className={styles.priceChartLabel} x={peak[0]} y={Math.max(peak[1] - 10, 12)} textAnchor={anchor(peak[0])}>High {usd(drop.fromClose)}</text>
              </g>
            )}
            {trough && drop && (
              <g>
                <circle className={styles.priceChartMarker} cx={trough[0]} cy={trough[1]} r={4.5} />
                <text className={styles.priceChartLabel} x={trough[0]} y={Math.min(trough[1] + 20, PAD.top + plotHeight + 18)} textAnchor={anchor(trough[0])}>Low {usd(drop.toClose)}</text>
              </g>
            )}
            {hover && (
              <g pointerEvents="none">
                <line className={styles.priceChartCrosshair} x1={hover.at[0]} x2={hover.at[0]} y1={PAD.top} y2={PAD.top + plotHeight} />
                <circle className={styles.priceChartDot} cx={hover.at[0]} cy={hover.at[1]} r={4.5} />
              </g>
            )}
          </svg>
        )}
        {hover && (
          <div
            className={styles.priceChartTip}
            role="status"
            style={{ left: Math.min(Math.max(hover.at[0], 70), width - 70), top: Math.max(hover.at[1] - 58, 0) }}
          >
            <span>{longDay(hover.day)}</span>
            <strong>{usd(hover.close)}</strong>
          </div>
        )}
      </div>
      <div className={styles.priceChartFooter}>
        <span>Past prices only. They don&apos;t predict future prices.</span>
        <button type="button" className={styles.priceChartCsv} onClick={() => downloadCsv(chart)}>Download closes (CSV)</button>
      </div>
    </figure>
  );
}
